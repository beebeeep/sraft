#!/bin/bash

export RUST_LOG=${RUST_LOG:-DEBUG}
cargo run --bin sraft -- -c config3.toml -f log3
