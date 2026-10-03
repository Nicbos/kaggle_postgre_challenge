#!/bin/bash
# Regenerate kaggle_challenge with N rows (default 100000) using the official generator.
N=${1:-100000}
docker exec -u bench -e PGPASSWORD=bench pgbench bash -c "cd /home/bench && python3 generate_database.py --rows $N --host 127.0.0.1 --user bench --user_pass bench --replace 2>&1 | tail -1"
