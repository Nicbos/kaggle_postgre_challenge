#!/bin/bash
# Run benchmark/bench.py inside the runner container; PDM_* variables are forwarded.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENVS=""
for v in $(env | grep -o '^PDM_[A-Z_]*'); do ENVS="$ENVS -e $v=${!v}"; done
docker run --rm --network pgnet -e SSH_HOST=pgbench -e SSH_PORT=22 $ENVS -v "$ROOT":/work pgrunner python benchmark/bench.py "$@"
