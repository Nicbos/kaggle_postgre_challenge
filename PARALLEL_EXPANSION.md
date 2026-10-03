# Parallel, resumable Expand: same recipes, same guarantees, up to 4.4× faster and crash-safe

## TL;DR
- New execution engine for the Expand operation (`services/parallel_expansion.py`, ~700 lines). It plugs into the existing `execute_cross_table_expansion` with a 16-line hook. Everything before execution (recipe validation, preflight, function safety, dependency checks) and after it (manifest-based version registration) is unchanged.
- **Primary goal (speed):** row-local recipes run per `raw_id` range on several PostgreSQL sessions at once, and independent global recipes run side by side. On an 8-core test box: **1M rows 93.5 s → 38.1 s, 5M rows 469.1 s → 146.8 s**; with the optional bulk FK mode, **5M rows → 106.1 s (4.4×)**. The output is identical to the original engine (content fingerprints of all six tables plus the row counters).
- **Secondary goal (recovery):** every chunk commits together with its checkpoint row and its row-counter update. Kill the connection at any moment, start the same expansion again, and it continues from the last committed chunk. A run whose data is committed but whose versions are not yet registered goes straight to registration. Tested by interrupting a 1M-row run twice: the resumed result matched the uninterrupted baseline exactly.
- Memory and disk: there are no staging copies of the source. Each worker holds one psql session, and the state tables hold one row per chunk (~600 rows at 300M).

## Where the time went (baseline profile)
I ran the DB Manager headless against PostgreSQL 16 (Docker, SSH as in the original architecture) and profiled with the app's own timing report plus `log_min_duration_statement`:

| rows | MEASURED PROCESSING TOTAL | share in the six INSERT…SELECT |
|---|---|---|
| 100k | 13.3 s | 85% |
| 1M | 96.1 s | 98% |
| 5M | 469.1 s | ~98% |

The whole expansion is **one REPEATABLE READ transaction on one backend**. Each of the six recipes rescans `raw_data` and re-parses the JSONB, so the work is CPU-bound on a single core while the rest of the server sits idle. Extrapolated, 300M rows would take about 8 hours, and a dropped SSH session at hour 7 throws everything away.

Wait-event sampling of a parallel prototype showed ~80% CPU and ~15% `WALWrite`. Per-row FK checks (RI triggers on table1/2/5/6) account for roughly a third of the insert CPU.

## Design

### 1. Plan: global prefix + row-local suffix
A recipe is **row-local** if its output for a source row depends only on that row and on already-filled destinations. The planner classifies it from the existing tokenizer:
- a recipe is **global** if it contains `DISTINCT`, `GROUP BY`, `HAVING`, `LIMIT/OFFSET/FETCH`, window functions, set operations, or aggregate functions;
- otherwise it is row-local.

Global recipes must form a prefix of the destination order. If a global recipe comes after a row-local one, the engine **falls back to the original single-transaction path**, so correctness never depends on a guess. The same fallback applies when the source has no `raw_id` or when `PDM_EXPANSION_WORKERS=1`.

For the challenge recipes: table3 and table4 (DISTINCT dimensions) are global; table1, table5, table6 and table2 are row-local. Every row-local recipe joins its parents through `source_row.raw_id`.

### 2. Global phase
Each global recipe runs exactly as before: REPEATABLE READ, `LOCK … SHARE ROW EXCLUSIVE`, the INSERT, then the row-counter update, all in one transaction. Global recipes that do not read each other run concurrently on separate sessions (1M rows: 8.4 s → 5.8 s).

### 3. Chunk phase
The `raw_id` span is cut into ranges of up to 500k source rows (about 4 per worker). For each chunk, a single transaction:
1. **claims** its checkpoint row (`pending → running`; a duplicate attempt, e.g. a psql left over from a dropped SSH session, blocks on that row and then aborts because the chunk is no longer pending);
2. runs **every row-local recipe in dependency order**, with the validator's `{{source}}` subquery narrowed by `AND raw_id >= lo AND raw_id < hi` (index `(raw_schema, raw_id)`);
3. adds the chunk's row counts to `pgdm_table_row_counts` **as the last statement**, in a fixed lock order, so the counter row locks are held for milliseconds instead of serialising the chunks;
4. marks the chunk `done` and commits.

Data, counters and the checkpoint therefore always move together: a chunk is either fully applied or not at all.

Chunks run on N sessions (default `min(nproc, 16)`, override with `PDM_EXPANSION_WORKERS`; chunk size `PDM_EXPANSION_CHUNK_ROWS`). Each worker gets **its own SSH connection**, because sshd's default `MaxSessions=10` refuses the 11th channel on a shared connection. Connections are opened one after another with retry, because `MaxStartups` drops bursts of handshakes. Both limits were hit in testing.

### 4. Finalize and register
The source hash manifest (the app's existing `raw_hash` manifest) is captured when the run starts and **recomputed at the end**. If the source changed, the run is marked `source_changed` and no version is registered. Otherwise the run becomes `data_committed`. The unchanged `register_cross_table_expansion_versions` then writes the versions, and the run is marked `completed`.

### State tables (`app_control` schema of the data database, hidden from the table list)
- `pgdm_expansion_runs`: run key (a hash of source, scope and recipe SQL), status, captured manifest, chunk plan.
- `pgdm_expansion_chunks`: range, status, rows and server seconds per destination.
- `pgdm_expansion_globals`: per global destination, rows and seconds (its presence marks the recipe done).

## Guarantees: preserved, moved, or changed
| guarantee | original | new engine |
|---|---|---|
| recipe validation, function safety, managed dependencies | preflight | **unchanged** (same code) |
| destination validation, row counters | in one transaction | **unchanged semantics**: counters are updated in the same transaction as the rows they count |
| source consistency | one REPEATABLE READ snapshot + manifest | manifest captured at start, **re-verified at the end**; mismatch means no version |
| version registration, dump/version handling, traceability | after commit | **unchanged code**; resumable if it was the step that failed |
| atomicity | all-or-nothing | **per chunk**; the run is complete only when every chunk is done and the manifest re-verifies |
| external writers during the run | blocked by table locks for the whole run | not blocked during the chunk phase (sessions cannot share a lock). The run state marks the expansion as in progress and the end-of-run manifest check catches source changes |

The last row is the deliberate trade-off. Holding `SHARE ROW EXCLUSIVE` across many sessions is impossible in PostgreSQL, and serialising them would cancel the gain.

## Results (8-core / 16-thread laptop, PostgreSQL 16 in Docker, 8 workers)
| rows | original | parallel | speed-up | output identical |
|---|---|---|---|---|
| 1M | 93.5 s | 38.1 s | 2.45× | yes |
| 5M | 469.1 s | 146.8 s | 3.2× | yes |
| 1M, bulk FK mode | 89–93.5 s | 23.4 s | ~3.9× | yes |
| 5M, bulk FK mode | 469.1 s | 106.1 s | 4.4× | yes |
| resume test, 1M, interrupted twice | — | completed | — | yes |

The parallel time scales linearly (1M → 5M ≈ ×3.9 for 5× the rows), so the speed-up is limited by the server's cores: the chunk phase is CPU-bound, and 8 and 12 workers give the same time on this box. On a server with more cores the gain grows proportionally.

Verification: `verify.sql` fingerprints every destination with surrogate ids replaced by natural keys (raw_id and the dimension's natural columns), and checks each `pgdm_table_row_counts` entry against `count(*)`.

### 5. Optional bulk FK mode (`PDM_EXPANSION_BULK_FK=1`)
Per-row RI triggers cost about a third of the chunk time: each inserted child row runs its own `SELECT … FOR KEY SHARE` against the parent. In bulk mode a chunk transaction:
- sets `session_replication_role = replica` (for that transaction only), which switches the RI triggers off;
- turns every recipe INSERT into `WITH ins AS (<recipe> RETURNING <fk columns>)` and checks each foreign key **once for the whole statement** with a `NOT EXISTS` probe against the parent (MATCH SIMPLE semantics: rows with a NULL key column are skipped). A single violating row raises `foreign_key_violation` and rolls the chunk back;
- takes `LOCK … IN SHARE MODE` on parents that the chunks never write (`raw_data`, table3, table4), so they cannot change under the check. SHARE is compatible across concurrent chunks.

Parents that are written by the chunks themselves (table1 for table2/5/6) are created in the same transaction, so nobody else can see or delete them before the check runs.

The mode is **opt-in** and refuses to start, falling back to per-row checks with a note in the timing report, when:
- the role is not a superuser (`session_replication_role` requires one);
- any user trigger exists on a chunk destination or on the counter table (replica mode would silence it too);
- a recipe already uses `RETURNING`;
- a foreign key is not MATCH SIMPLE or is self-referencing.

It was tested positively (identical output at 1M and 5M) and negatively (an INSERT that references a missing parent is rejected with the violating-row count per constraint).

## Not done / next steps
- **Global DISTINCT recipes** are still single-threaded per recipe (≈5 s per 1M rows for table3). Parallelising them needs splitting the recipe into a parallel SELECT and an INSERT, which would mean rewriting user SQL.
- Making the end-of-run manifest check incremental (per chunk) would shave two full scans at 300M.

## Reproduce
Everything runs in Docker (PostgreSQL 16 + sshd, plus a Python client container), following the challenge's SSH architecture:
```
benchmark/setup.sh                                   # build containers, init control DB
benchmark/reset.sh 1000000                           # official generator, fixed seed
PDM_EXPANSION_WORKERS=1 benchmark/run.sh expand      # original engine
benchmark/run.sh expand --report run.json            # parallel engine
PDM_EXPANSION_BULK_FK=1 benchmark/run.sh expand      # parallel + bulk FK checks
benchmark/verify.sh                                  # content fingerprints + counter check
```
`benchmark/bench.py` drives the same `PostgresAdminService` methods the GUI calls (`ensure_admin_schema` → `execute_cross_table_expansion` → `register_cross_table_expansion_versions`), so the Windows GUI is not needed to benchmark. In the GUI, the new engine is used automatically.

Configuration (environment variables): `PDM_EXPANSION_WORKERS` (default `min(nproc, 16)`; `1` = original engine), `PDM_EXPANSION_CHUNK_ROWS` (default: about 4 chunks per worker, capped at 500k rows), `PDM_EXPANSION_BULK_FK` (default off).
