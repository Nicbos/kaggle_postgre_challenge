#!/bin/bash
/usr/sbin/sshd
exec docker-entrypoint.sh postgres -c shared_buffers=1GB -c max_wal_size=4GB
