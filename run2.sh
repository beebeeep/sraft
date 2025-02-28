#!/bin/bash

export RUST_LOG=${RUST_LOG:-DEBUG}
cargo run -- -c config2.toml -f log2
