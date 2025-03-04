#!/bin/bash

export RUST_LOG=${RUST_LOG:-DEBUG}
cargo run --bin sraft -- -c config2.toml -f log2
