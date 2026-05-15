#!/usr/bin/env sh
set -eu

REPO="${DTK_REPO:-rtk-ai/dtk}"
INSTALL_DIR="${DTK_INSTALL_DIR:-$HOME/.local/bin}"
BINARY_NAMES="dtk dtk_detect_json dtk_inspect_json dtk_filter_json dtk_recover_json dtk_retrieve_json dtk_cleanup_store dtk_exec dtk_hook_route"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
LOCAL_SOURCE_DIR="$SCRIPT_DIR"

info() {
  printf '[INFO] %s\n' "$1"
}

warn() {
  printf '[WARN] %s\n' "$1"
}

error() {
  printf '[ERROR] %s\n' "$1" >&2
  exit 1
}

detect_os() {
  case "$(uname -s)" in
    Linux*) OS="linux" ;;
    Darwin*) OS="darwin" ;;
    *) error "Unsupported operating system: $(uname -s)" ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64) ARCH="x86_64" ;;
    arm64|aarch64) ARCH="aarch64" ;;
    *) error "Unsupported architecture: $(uname -m)" ;;
  esac
}

get_target() {
  case "$OS" in
    linux) TARGET="${ARCH}-unknown-linux-gnu" ;;
    darwin) TARGET="${ARCH}-apple-darwin" ;;
  esac
}

resolve_latest_version() {
  VERSION=$(curl -sI "https://github.com/${REPO}/releases/latest" \
    | grep -i '^location:' \
    | sed -E 's|.*/tag/([^[:space:]]+).*|\1|' \
    | tr -d '\r')

  if [ -z "$VERSION" ]; then
    warn "Redirect lookup failed, falling back to GitHub API..."
    VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
      | grep '"tag_name":' \
      | sed -E 's/.*"([^"]+)".*/\1/')
  fi

  [ -n "$VERSION" ]
}

install_from_release() {
  info "Detected: $OS $ARCH"
  info "Target: $TARGET"
  info "Version: $VERSION"

  TEMP_DIR=$(mktemp -d)
  ARCHIVE="${TEMP_DIR}/dtk.tar.gz"
  DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/dtk-${TARGET}.tar.gz"

  info "Downloading from: $DOWNLOAD_URL"
  if ! curl -fsSL "$DOWNLOAD_URL" -o "$ARCHIVE"; then
    return 1
  fi

  info "Extracting release bundle..."
  tar -xzf "$ARCHIVE" -C "$TEMP_DIR"
  mkdir -p "$INSTALL_DIR"

  for binary_name in $BINARY_NAMES; do
    if [ ! -f "$TEMP_DIR/$binary_name" ]; then
      error "release bundle is missing $binary_name"
    fi
    cp "$TEMP_DIR/$binary_name" "$INSTALL_DIR/$binary_name"
    chmod +x "$INSTALL_DIR/$binary_name"
  done

  rm -rf "$TEMP_DIR"
}

install_from_source() {
  if [ ! -f "$LOCAL_SOURCE_DIR/Cargo.toml" ]; then
    return 1
  fi

  if ! command -v cargo >/dev/null 2>&1; then
    error "cargo is required for local source installation"
  fi

  info "Falling back to local build from $LOCAL_SOURCE_DIR"
  if ! (cd "$LOCAL_SOURCE_DIR" && cargo build --release --bins); then
    warn "If Cargo reports that lock file version 4 is unsupported, update Rust via rustup and retry."
    warn "Suggested fix: rustup update stable && rustup default stable"
    error "cargo build failed"
  fi

  mkdir -p "$INSTALL_DIR" || error "failed to create install dir: $INSTALL_DIR"
  for binary_name in $BINARY_NAMES; do
    cp "$LOCAL_SOURCE_DIR/target/release/$binary_name" "$INSTALL_DIR/$binary_name" || error "failed to copy $binary_name"
    chmod +x "$INSTALL_DIR/$binary_name" || error "failed to mark $binary_name executable"
  done
}

verify() {
  if [ -x "$INSTALL_DIR/dtk" ]; then
    info "Verification: $("$INSTALL_DIR/dtk" version)"
  elif command -v dtk >/dev/null 2>&1; then
    info "Verification: $(dtk version)"
  else
    warn "Binary installed but not in PATH."
    warn "Add $INSTALL_DIR to your shell profile:"
    warn "  export PATH=\"$HOME/.local/bin:\$PATH\""
  fi
}

main() {
  detect_os
  detect_arch
  get_target

  if [ -n "${DTK_VERSION:-}" ]; then
    VERSION="$DTK_VERSION"
    info "Using pinned version from DTK_VERSION: $VERSION"
  elif resolve_latest_version; then
    info "Latest release: $VERSION"
  else
    VERSION=""
    warn "Could not resolve the latest release version"
  fi

  if [ -n "$VERSION" ] && install_from_release; then
    :
  else
    warn "Release install failed, trying local source build"
    install_from_source || error "install failed"
  fi

  info "Installed DTK binaries to $INSTALL_DIR"
  verify
}

main
