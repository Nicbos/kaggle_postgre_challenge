#!/bin/bash
# Content fingerprint of all destinations (surrogate ids replaced by natural keys) + counter check.
docker exec -i pgbench psql -U bench -d kaggle_challenge -At -F ' ' -f - < "$(dirname "$0")/verify.sql"
