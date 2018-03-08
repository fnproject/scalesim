package main

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"

	"github.com/dchest/siphash"
)

const (
	recordInterval = 1 * time.Second
	nodeBootTime   = 30 * time.Second // TODO this should be a function w/ variance
)

// XXX(reed): yank out the scaling & routing logic so that it's more pluggable instead
// of baked into the event model

func main() {
	// TODO flags / json config

	var conf Config
	conf.Runtime = 60 * time.Minute
	conf.CallGen = func(d time.Duration) time.Duration {
		// bursty traffic for 10 seconds every minute
		if d%time.Minute < 10*time.Second {
			return 5 * time.Millisecond
		}
		return 500 * time.Millisecond
	}

	now := time.Now()
	m := Run(&conf)
	fmt.Println("sim runtime:", time.Since(now))

	http.HandleFunc("/log/", log(m))
	http.HandleFunc("/", dash(m))

	http.ListenAndServe(":5000", nil)
}

// ----------------- http handlers -----------------------------

func dash(m *Mustapha) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		file, err := ioutil.ReadFile("sim.tmpl")
		if err != nil {
			panic(err)
		}
		w.Write(file)
	}
}

func log(m *Mustapha) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		v := plot(m)
		//json.NewEncoder(os.Stdout).Encode(v)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(v)
	}
}

// turn data into a series of [name]{x,y} coordinates with names highcharts can plot
func plot(m *Mustapha) []map[string]interface{} {
	xy := func(s, i interface{}) [2]interface{} { return [2]interface{}{s, i} }

	// build histograms for each metric
	inners := make(map[string][][2]interface{})
	for _, p := range m.hist {
		t := int(p.Time.Nanoseconds() / int64(time.Millisecond))
		inners["requests_queued"] = append(inners["requests_queued"], xy(t, p.RequestsQueued))
		inners["requests_running"] = append(inners["requests_running"], xy(t, p.RequestsRunning))
		inners["requests_complete"] = append(inners["requests_complete"], xy(t, p.RequestsComplete))
		inners["requests_timeout"] = append(inners["requests_timeout"], xy(t, p.RequestsTimeout))

		inners["nodes_booting"] = append(inners["nodes_booting"], xy(t, p.NodesBooting))
		inners["nodes_live"] = append(inners["nodes_live"], xy(t, p.NodesLive))
	}

	// XXX(reed): automate this when you get your brain back
	var datas []map[string]interface{}
	datas = append(datas, []map[string]interface{}{
		{
			"name": "requests_queued",
			"data": inners["requests_queued"],
		},
		{
			"name": "requests_running",
			"data": inners["requests_running"],
		},
		{
			"name": "requests_complete",
			"data": inners["requests_complete"],
		},
		{
			"name": "requests_timeout",
			"data": inners["requests_timeout"],
		},
		{
			"name": "nodes_booting",
			"data": inners["nodes_booting"],
		},
		{
			"name": "nodes_live",
			"data": inners["nodes_live"],
		},
	}...)

	return datas
}

// ------------------------- sim state -------------------------------------

// The World Controller
type Mustapha struct {
	now    time.Duration
	events heap.Interface

	callGen          func(time.Duration) time.Duration
	requestsQueued   []request
	requestsRunning  []request
	requestsComplete int
	requestsTimeout  int // TODO we need 2 timeout (ran / not run)

	machinesBooting int

	lb lb // TODO this should prob be a []lb somewhere within

	hist []point
}

type Config struct {
	Runtime time.Duration
	CallGen func(time.Duration) time.Duration
}

type point struct {
	Time time.Duration

	RequestsQueued   int
	RequestsRunning  int
	RequestsComplete int
	RequestsTimeout  int
	NodesBooting     int
	NodesLive        int

	// TODO what else do we want to see?
	// * mean/median/p99 wait time
	// * distribution?
	// * utilization?
}

type request struct {
	queuedAt time.Duration
	runtime  time.Duration
	timeout  time.Duration
	mem      int // MB
	path     string

	// running info
	startedAt time.Duration
	node      *machine
}

type machine struct {
	memFree  int // MB
	memTotal int // MB

	// TODO CPU

	// TODO multiple route types
	waitTime time.Duration
}

type lb struct {
	machines []machine
}

func Run(conf *Config) *Mustapha {
	var m Mustapha
	m.events = new(pq)
	m.callGen = conf.CallGen

	heap.Init(m.events)

	// start recording metrics & start the call train
	m.Queue(eventRecord{}, 0)
	m.Queue(eventCall{}, 0)

	for m.now < conf.Runtime {
		e := heap.Pop(m.events).(event)
		m.now = e.time

		e.handleEvent(&m)
	}

	return &m
}

// ------------------ event queue ---------------------------

var _ heap.Interface = new(pq)

type pq []event

func (p pq) Len() int            { return len(p) }
func (p pq) Less(i, j int) bool  { return p[i].time < p[j].time }
func (p pq) Swap(i, j int)       { p[i], p[j] = p[j], p[i] }
func (p *pq) Push(x interface{}) { *p = append(*p, x.(event)) }
func (p *pq) Pop() interface{}   { x := (*p)[len(*p)-1]; *p = (*p)[:len(*p)-1]; return x }

type event struct {
	eventHandler
	time time.Duration
}

// ------------------ event handlers -----------------------

func (m *Mustapha) Queue(h eventHandler, next time.Duration) {
	heap.Push(m.events, event{eventHandler: h, time: m.now + next})
}

type eventHandler interface {
	handleEvent(m *Mustapha)
}

// TODO devise a more cohesive naming strategy
type eventRecord struct{}
type eventCall struct{}
type eventProcess struct{}
type eventNodeAdd struct{}
type eventNodeReady struct{}
type eventCallFinish struct{}

// eventCall queues a call against the lb, schedules the next queued call, kicks off processing
func (e eventCall) handleEvent(m *Mustapha) {
	call := request{
		queuedAt: m.now,
		runtime:  200 * time.Millisecond,
		timeout:  30 * time.Second,
		mem:      256,
		path:     "TODO/multiple/me",
	}
	m.requestsQueued = append(m.requestsQueued, call)

	// queue next
	next := m.callGen(m.now)
	m.Queue(eventCall{}, next)

	// see what we can process // TODO or just do it here?
	m.Queue(eventProcess{}, 0)
}

func (e eventProcess) handleEvent(m *Mustapha) {
	if len(m.lb.machines) < 1 {
		if m.machinesBooting == 0 {
			// TODO just adds 1 node, for now, to get going
			m.Queue(eventNodeAdd{}, 0)
		}
		return
	}

	m.timeoutQueued() // don't process any timed out

	for i := 0; i < len(m.requestsQueued); i++ {
		call := m.requestsQueued[i]
		node := Route(m.lb.machines, call.path)
		if node.memFree > call.mem {
			// use the ram
			node.memFree -= call.mem
			// TODO this needs to be per route
			node.waitTime = ewma(node.waitTime, m.now-call.queuedAt)

			// remove from queued
			m.requestsQueued = append(m.requestsQueued[:i], m.requestsQueued[i+1:]...)
			i--

			// add to running
			call.node = node // TODO we should track this per node.
			call.startedAt = m.now
			m.requestsRunning = append(m.requestsRunning, call)

			// TODO timeout should precisely create event here
			m.Queue(eventCallFinish{}, call.runtime)
			continue
		} // else, leave it queued

		// TODO naive scaler here, if all nodes are full, add another
		//var room bool
		//for _, node := range m.lb.machines {
		//if node.memFree > call.mem {
		//room = true
		//}
		//}
		//if !room {
		//m.Queue(eventNodeAdd{}, 0)
		//}
	}
}

// tracks last 10 samples (very fast)
const DECAY = 0.1

func ewma(old, new time.Duration) time.Duration {
	// TODO could 'warm' it up and drop first few samples since we'll have docker pulls / hot starts
	return time.Duration((float64(new) * DECAY) + (float64(old) * (1 - DECAY)))
}

func (e eventCallFinish) handleEvent(m *Mustapha) {
	// TODO O(n) is avoidable...
	for i := 0; i < len(m.requestsRunning); i++ {
		call := m.requestsRunning[i]
		if call.startedAt+call.runtime <= m.now {
			// restore the ram
			call.node.memFree += call.mem

			// remove from running
			m.requestsRunning = append(m.requestsRunning[:i], m.requestsRunning[i+1:]...)
			i--

			// TODO handle timeout
			// TODO per node?
			m.requestsComplete++
		}
	}

	// once we finish calls, queued ones can run immediately
	m.Queue(eventProcess{}, 0)
}

func (e eventNodeAdd) handleEvent(m *Mustapha) {
	m.machinesBooting++
	m.Queue(eventNodeReady{}, nodeBootTime)
}

func (e eventNodeReady) handleEvent(m *Mustapha) {
	m.machinesBooting--
	m.lb.machines = append(m.lb.machines, machine{
		memFree:  8 * 1024, // 8 GB
		memTotal: 8 * 1024,
	})

	// after adding a node, try to run stuff for 0->1 case
	// TODO behavior mismatch, currently we fail requests if no nodes.
	m.Queue(eventProcess{}, 0)
}

func (m *Mustapha) timeoutQueued() {
	for i := 0; i < len(m.requestsQueued); i++ {
		r := m.requestsQueued[i]
		if r.queuedAt+r.timeout < m.now {
			m.requestsTimeout++

			m.requestsQueued = append(m.requestsQueued[:i], m.requestsQueued[i+1:]...)
			i--
		}
	}
}

func (e eventRecord) handleEvent(m *Mustapha) {
	// clear any queued requests that have timed out before stat collection
	// this is imprecise but since we're sampling, we won't know any better.
	m.timeoutQueued()

	pt := collect(m)
	m.hist = append(m.hist, pt)

	m.Queue(eventRecord{}, recordInterval)
}

func collect(m *Mustapha) point {
	return point{
		Time:             m.now,
		RequestsQueued:   len(m.requestsQueued),
		RequestsRunning:  len(m.requestsRunning),
		RequestsComplete: m.requestsComplete,
		RequestsTimeout:  m.requestsTimeout,
		NodesBooting:     m.machinesBooting,
		NodesLive:        len(m.lb.machines),
	}
}

// ------------------- consistent hash -------------------------
// TODO import this instead

// this program is single threaded... wahoo
var rng = rand.New(rand.NewSource(time.Now().Unix()))

// Route in this form relies on the nodes being in sorted order so
// that the output will be consistent (yes, slightly unfortunate).
func Route(nodes []machine, key string) *machine {
	// crc not unique enough & sha is too slow, it's 1 import
	sum64 := siphash.Hash(0, 0x4c617279426f6174, []byte(key))

	i := int(jumpConsistentHash(sum64, int32(len(nodes))))
	return besti(i, nodes)
}

// A Fast, Minimal Memory, Consistent Hash Algorithm:
// https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
func jumpConsistentHash(key uint64, num_buckets int32) int32 {
	var b, j int64 = -1, 0
	for j < int64(num_buckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = (b + 1) * int64((1<<31)/(key>>33)+1)
	}
	return int32(b)
}

func besti(i int, nodes []machine) *machine {
	for ; ; i++ {
		// theoretically this could take infinite time, but practically improbable...
		// TODO we need a way to add a node for a given key from down here if a node is overloaded.
		if checkLoad(nodes[i]) {
			return &nodes[i]
		}

		if i == len(nodes)-1 {
			i = -1 // reset i to 0
		}
	}
}

func checkLoad(n machine) bool {
	load := n.waitTime

	const (
		// TODO we should probably use deltas rather than fixed wait times. for 'cold'
		// functions these could always trigger. i.e. if wait time increased 5x over last
		// 100 data points, point the cannon elsewhere (we'd have to track 2 numbers but meh)
		lowerLat = 500 * time.Millisecond
		upperLat = 2 * time.Second
	)

	// TODO flesh out these values.
	// if we send < 50% of traffic off to other nodes when loaded
	// then as function scales nodes will get flooded, need to be careful.
	//
	// back off loaded node/function combos slightly to spread load
	if load < lowerLat {
		return true
	} else if load > upperLat {
		// really loaded
		if rng.Intn(100) < 10 { // XXX (reed): 10% could be problematic, should sliding scale prob with log(x) ?
			return true
		}
	} else {
		// 10 < x < 40, as load approaches upperLat, x decreases [linearly]
		x := translate(int64(load), int64(lowerLat), int64(upperLat), 10, 40)
		if rng.Intn(100) < x {
			return true
		}
	}

	// return invalid node to try next node
	return false
}

func translate(val, inFrom, inTo, outFrom, outTo int64) int {
	outRange := outTo - outFrom
	inRange := inTo - inFrom
	inVal := val - inFrom
	// we want the number to be lower as intensity increases
	return int(float64(outTo) - (float64(inVal)/float64(inRange))*float64(outRange))
}
