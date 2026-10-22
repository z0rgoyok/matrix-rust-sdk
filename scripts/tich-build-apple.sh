#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ -n "$(git status --porcelain)" ]; then
  echo 'Commit SDK changes before building a release artifact.' >&2
  exit 1
fi
tich_source_sha=$(git rev-parse HEAD)
export TICH_RUST_TOOLCHAIN=1.98.0
export CARGO_BUILD_JOBS=4
RUSTC=$(rustup which --toolchain "$TICH_RUST_TOOLCHAIN" rustc)
RUSTDOC=$(rustup which --toolchain "$TICH_RUST_TOOLCHAIN" rustdoc)
export RUSTC RUSTDOC
rustup run "$TICH_RUST_TOOLCHAIN" cargo run --locked -p xtask -- swift build-framework \
  --release --target aarch64-apple-ios --target aarch64-apple-ios-sim \
  --ios-deployment-target 18.5 --sequentially
python3 scripts/tich-record-build.py "$tich_source_sha"
