#!/bin/bash

cargo build || exit -1

trap 'jobs -p | xargs kill' EXIT
export RUST_LOG=${RUST_LOG:-DEBUG}
./target/debug/sraft -c config1.toml &
./target/debug/sraft -c config2.toml &
./target/debug/sraft -c config3.toml &
wait
