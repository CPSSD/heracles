# Heracles
CA4019 Project

[![Build Status](https://travis-ci.com/CPSSD/heracles.svg?token=Ty8HySwL3To4YV7AZfi2&branch=develop)](https://travis-ci.com/CPSSD/heracles)

Heracles (/ˈhɛrəkliːz/ HERR-ə-kleez) was a divine hero in Greek mythology.
> Underworld, Hercules encountered Cerberus. Undaunted, the hero threw his strong arms around
> the beast, perhaps grasping all three heads at once, and wrestled Cerberus into submission.

Fork of [cerberus](https://github.com/cpssd/cerberus)

---

## Requirements:

#### Development
- Rust Nightly
- Protobuf Compiler
- net-tools (if running worker locally)

#### Deployment
- Docker
- Docker Compose

---

## Building the project

Build everything by running:

```
$ cargo build --all
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
