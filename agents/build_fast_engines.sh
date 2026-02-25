#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CC_BIN="${CC:-cc}"
DRY_RUN=0

for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    *)
      echo "Unknown arg: $arg" >&2
      echo "Usage: $0 [--dry-run]" >&2
      exit 2
      ;;
  esac
done

UNAME_S="$(uname -s)"
UNAME_M="$(uname -m)"

BASE_FLAGS=(-O3 -DNDEBUG -fPIC -Wall -Wextra)
ARCH_FLAGS=()

if [[ "$UNAME_S" == "Darwin" && "$UNAME_M" == "arm64" ]]; then
  # Apple Silicon optimization target for M3-class machines.
  ARCH_FLAGS=(-mcpu=apple-m3 -ffast-math -funroll-loops)
elif [[ "$UNAME_S" == "Linux" ]]; then
  ARCH_FLAGS=(-march=native -mtune=native)
fi

SHARED_FLAG=(-shared)
if [[ "$UNAME_S" == "Darwin" ]]; then
  SHARED_FLAG=(-dynamiclib)
fi

compile_shared() {
  local src="$1"
  local out="$2"
  shift 2
  local libs=("$@")
  local cmd=("$CC_BIN" "${BASE_FLAGS[@]}" "${ARCH_FLAGS[@]}" "${SHARED_FLAG[@]}" "$src" -o "$out")
  if [[ ${#libs[@]} -gt 0 ]]; then
    cmd+=("${libs[@]}")
  fi
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf '[dry-run] '
    printf '%q ' "${cmd[@]}"
    printf '\n'
  else
    echo "Building $(basename "$out") from $(basename "$src")"
    "${cmd[@]}"
  fi
}

compile_shared "$SCRIPT_DIR/fast_exec.c" "$SCRIPT_DIR/fast_exec.so" -lm
compile_shared "$SCRIPT_DIR/fast_engine.c" "$SCRIPT_DIR/fast_engine.so" -lm
compile_shared "$SCRIPT_DIR/fast_signal_mesh.c" "$SCRIPT_DIR/fast_signal_mesh.so"

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "Dry run complete."
else
  echo "Build complete."
fi

