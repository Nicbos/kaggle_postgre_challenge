#!/bin/bash
# Build the two containers: pgbench (PostgreSQL 16 + sshd, no published ports)
# and pgrunner (Python client). They talk over the private docker network pgnet.
set -e
cd "$(dirname "$0")"
docker network create pgnet >/dev/null 2>&1 || true
docker build -q -t pgbench-ssh docker
docker build -q -t pgrunner -f docker/Dockerfile.runner docker
docker rm -f pgbench >/dev/null 2>&1 || true
docker run -d --name pgbench --network pgnet -e POSTGRES_PASSWORD=bench --shm-size=2g pgbench-ssh
sleep 8
docker cp ../generate_database.py pgbench:/home/bench/
./run.sh init
