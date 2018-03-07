## WARNING: this is largely in flux

In current state, bugs are abound (and aware of) and this is not entirely
useful due to that, massaging into workable state is underway.

# scale sim

This is a simulator for fn workloads against mocked out fn infrastructure.

We can use this to get quick feedback on policy decisions for things like hot
slot acquisition policy, load balancing algorithms, node scaling algorithms
and tune things without having to stand up any infrastructure or run prolonged
tests.

Current aims:

* fn node addition / management
* fn load balancing for sync http requests
* distribution trade offs for cpu/mem

# running

```
$ go build
$ ./scalesim
$ open http://localhost:5000
```

a dashboard should appear with a chart after refreshing until the sim is done (~10s),
this needs ux improvement ;)

TODO the dashboard should allow configuring a sim, getting the results and
allow flipping through all results (so as to compare)
