#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -x "$root_dir/target/debug/dtk" ]]; then
  exec "$root_dir/target/debug/dtk" install
fi

exec cargo run --quiet --bin dtk -- install
