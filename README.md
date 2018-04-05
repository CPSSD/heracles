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

## Running benchmarks

The following is required to run the benchmarking script:
```
apt-get install python3-pip python3-tk
pip3 install numpy matplotlib
```

Run the benchmarking script with:
```
python3 benchmarks.py
```

---

## System Requirements
The project currently only works on Linux. macOS and other platforms are planned for the future.
