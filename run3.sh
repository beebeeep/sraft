#!/bin/bash

export RUST_LOG=${RUST_LOG:-DEBUG}
cargo run ${RELEASE} -- -c config3.toml -f log3
