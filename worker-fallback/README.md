Heracles Worker Fallback
========================

Heracles worker fallback is a full heracles compatible worker written in Go.
It was designed as an alternative for the worker.

## Dependencies:

- Heracles project in `$GOPATH/src/github.com/cpssd/heracles` (symlink will do)
- Go (1.10+ recommended, but probably works with older releases above go1.7)
- protobuf compiler
- Go gRPC plugin: `go get -u google.golang.org/grpc`

## Building
You can either run `make` from this directory, or `make worker-fallback` from
the top level `Makefile`. If you want to build a statically compiled version
run `make release`. The binaries would be placed in top level
`target/debug/worker-fallback` and `target/release/worker-fallback`.

## Running
You can run this as any normal binary:

```
./target/debug/worker-fallback
```

At the very least you need to provide the following flags:
- AMQP broker location: `--broker.address=amqp://valid-address:5672/`
- Location to save state: `--state.location=/tmp/heracles-worker-fallback`

## Known problems

- Explicit errors would be more useful
- Reducer has to be fully added
- Tests need to use relative paths