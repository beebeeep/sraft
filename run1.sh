#!/bin/bash

export RUST_LOG=${RUST_LOG:-DEBUG}
cargo run --bin sraft -- -c config1.toml -f log1
