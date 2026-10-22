#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
export TICH_RUST_TOOLCHAIN=1.98.0
export CARGO_BUILD_JOBS=4
rustup run "$TICH_RUST_TOOLCHAIN" cargo xtask swift build-framework \
  --release --target aarch64-apple-ios --target aarch64-apple-ios-sim \
  --ios-deployment-target 18.5 --sequentially
