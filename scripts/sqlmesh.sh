#!/usr/bin/env bash
# SQLMesh 本地安装与常用命令（不依赖 flowgpt-data-platform）
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROJECT_DIR="$ROOT/projects/warehouse"
VENV="$ROOT/.venv-sqlmesh"
PY="${SQLMESH_PYTHON:-python3.11}"
SQLMESH_VERSION="${SQLMESH_VERSION:-0.236.1}"

die() { echo "✗ $*" >&2; exit 1; }

ensure_env() {
  mkdir -p "$ROOT/data"
  if [ ! -f "$PROJECT_DIR/.env" ]; then
    cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
    echo "  已生成 $PROJECT_DIR/.env"
  fi
}

cmd_install() {
  command -v "$PY" >/dev/null || die "需要 $PY（可用 SQLMESH_PYTHON=python3.12 覆盖）"
  ensure_env
  echo ">>> 安装 SQLMesh $SQLMESH_VERSION → $VENV ($PY)"
  "$PY" -m venv "$VENV"
  # shellcheck disable=SC1091
  source "$VENV/bin/activate"
  pip install -U pip wheel
  pip install "sqlmesh[web,starrocks]==${SQLMESH_VERSION}"
  sqlmesh --version
  echo "✓ 完成。用法: make sqlmesh-info | make sqlmesh-ui"
}

sqlmesh_bin() {
  if [ -x "$VENV/bin/sqlmesh" ]; then
    echo "$VENV/bin/sqlmesh"
  elif command -v sqlmesh >/dev/null; then
    command -v sqlmesh
  else
    die "未安装 SQLMesh。先运行: make sqlmesh-install"
  fi
}

run_in_project() {
  ensure_env
  local bin
  bin="$(sqlmesh_bin)"
  cd "$PROJECT_DIR"
  set -a
  # shellcheck disable=SC1091
  [ -f .env ] && source .env
  set +a
  "$bin" "$@"
}

cmd_info() { run_in_project info; }
cmd_ui() {
  local port="${SQLMESH_UI_PORT:-8082}"
  echo ">>> SQLMesh UI → http://127.0.0.1:${port}"
  run_in_project ui --host 127.0.0.1 --port "$port"
}

cmd_help() {
  cat <<EOF
用法: $0 <install|info|ui|...>

  install   创建 .venv-sqlmesh 并安装 sqlmesh[web]==${SQLMESH_VERSION}
  info      sqlmesh info（连 StarRocks + DuckDB state）
  ui        启动 Web UI（默认 :8082）
  *         其余参数原样传给 sqlmesh（在 projects/warehouse 下）
EOF
}

main() {
  local cmd="${1:-help}"
  shift || true
  case "$cmd" in
    install) cmd_install ;;
    info) cmd_info ;;
    ui) cmd_ui ;;
    help|-h|--help) cmd_help ;;
    *) run_in_project "$cmd" "$@" ;;
  esac
}

main "$@"
