#!/usr/bin/env sh
set -eu

INSTALL_DIR="${DTK_INSTALL_DIR:-$HOME/.local/bin}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

info() {
  printf '[INFO] %s\n' "$1"
}

error() {
  printf '[ERROR] %s\n' "$1" >&2
  exit 1
}

main() {
  if ! command -v cargo >/dev/null 2>&1; then
    error "cargo is required for local development installation"
  fi

  if [ ! -f "$SCRIPT_DIR/Cargo.toml" ]; then
    error "install-dev.sh must be run from a DTK checkout"
  fi

  info "Building DTK from local source at $SCRIPT_DIR"
  (cd "$SCRIPT_DIR" && cargo build --release --bins) || error "cargo build failed"

  mkdir -p "$INSTALL_DIR" || error "failed to create install dir: $INSTALL_DIR"

  for binary_name in dtk dtk_detect_json dtk_inspect_json dtk_filter_json dtk_recover_json dtk_retrieve_json dtk_cleanup_store dtk_exec dtk_hook_route; do
    cp "$SCRIPT_DIR/target/release/$binary_name" "$INSTALL_DIR/$binary_name" || error "failed to copy $binary_name"
    chmod +x "$INSTALL_DIR/$binary_name" || error "failed to mark $binary_name executable"
  done

  info "Installed DTK binaries to $INSTALL_DIR"
  if [ -x "$INSTALL_DIR/dtk" ]; then
    info "Verification: $("$INSTALL_DIR/dtk" version)"
  else
    info "Add $INSTALL_DIR to your PATH if needed"
  fi
}

main
