Heracles Worker
===============

## Dependencies:

- Heracles project in `$GOPATH/src/github.com/cpssd/heracles` (symlink will do)
- Go (1.10+ recommended, but probably works with older releases above go1.7)
- protobuf compiler

## Building
Run `make go-build` from the top level directory to build the worker. This would
create a dynamically linked version of the worker at `target/debug/worker`.

## Running
You can run this as any normal binary:

```
./target/debug/worker
```

At the very least you need to provide the following flags:
- AMQP broker location: `--broker.address=amqp://valid-address:5672/`
- Location to save state: `--state.location=/tmp/heracles-worker-fallback`

## Known problems

- Explicit errors would be more useful
- Does not use a proper community project structure (to keep it consistent with
  rust code)