#!/usr/bin/env bash
set -euo pipefail

cd /project

case "${1:-ui}" in
  ui)
    exec sqlmesh ui --host 0.0.0.0 --port "${SQLMESH_UI_PORT:-8080}"
    ;;
  info|plan|run|audit|dag|migrate|janitor|clean)
    exec sqlmesh "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
