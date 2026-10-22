#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
export TICH_RUST_TOOLCHAIN=1.98.0
export CARGO_BUILD_JOBS=4
RUSTC=$(rustup which --toolchain "$TICH_RUST_TOOLCHAIN" rustc)
RUSTDOC=$(rustup which --toolchain "$TICH_RUST_TOOLCHAIN" rustdoc)
export RUSTC RUSTDOC
rustup run "$TICH_RUST_TOOLCHAIN" cargo run --locked -p xtask -- swift build-framework \
  --release --target aarch64-apple-ios --target aarch64-apple-ios-sim \
  --ios-deployment-target 18.5 --sequentially
