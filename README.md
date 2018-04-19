# Heracles
CA4019 Project

[![CircleCI](https://circleci.com/gh/CPSSD/heracles/tree/develop.svg?style=shield&circle-token=26f1c1fad4fca1912b59c58cc8095b01499340f3)](https://circleci.com/gh/CPSSD/heracles/tree/develop)

Heracles (/ˈhɛrəkliːz/ HERR-ə-kleez) was a divine hero in Greek mythology.
> Underworld, Hercules encountered Cerberus. Undaunted, the hero threw his strong arms around
> the beast, perhaps grasping all three heads at once, and wrestled Cerberus into submission.

Fork of [cerberus](https://github.com/cpssd/cerberus)

---

__NOTE: The project is currently in heavy development. Some documentation might
be out of date before the project is finalized. It is absolutely not yet suited
for production use.__

---

## Requirements:

#### Development
- Rust Nightly
- Protobuf Compiler
- net-tools (if running worker locally)
- Go (1.10+ recommended)
- Dep (`go get github.com/golang/dep/cmd/dep`)

#### Deployment
- Docker
- Docker Compose

---

## Building the project

Build everything by running:

```
$ make build
$ make go-build
```

---

## Running

To run the project, please run:
- `dep ensure` to download all Go dependencies
- `cargo build --all --examples` to build a demo example
- Make sure docker is running, or optionally run your own RabbitMQ

Run `tools/demo.sh` to setup the testing directory. If you use your own
RabbitMQ remove the first lines regarding docker.

There are convenience bash scripts located in `tools/` which will guide you
through the required flags for manager (manager-fallback), worker and the
hrctl CLI tool.

## System Requirements
The system was tested on Linux only. There are no guarantees it works or even
compiles on other systems.