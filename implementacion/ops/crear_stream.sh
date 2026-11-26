#!/bin/sh
set -eu
dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec "$dir/../../parcial3-harness-estudiantes/.venv/bin/python" "$dir/stream.py" create
