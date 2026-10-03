"""Parallel, checkpointed execution engine for the cross-table Expand operation.

The original engine runs every recipe over the whole source inside a single
REPEATABLE READ transaction. That keeps one CPU core busy, scans the JSONB
source once per recipe and, if the SSH session drops, loses all the work.

This engine keeps the same recipes, validation, counters and version
registration, but changes how the INSERT ... SELECT statements are executed:

1. Global phase: recipes that are not row-local (DISTINCT, GROUP BY,
   aggregates, window functions, LIMIT, set operations) run once over the full
   scope, in one transaction, exactly like the original engine.
2. Chunk phase: the remaining (row-local) recipes run per raw_id range. Each
   chunk is one transaction that executes every row-local recipe for its range
   in dependency order, adds its row counts to pgdm_table_row_counts and marks
   itself done. Chunks run in parallel on several PostgreSQL sessions.
3. Finalize: the source hash manifest is recomputed and must match the one
   captured when the run started; only then is the run marked data_committed
   and handed to the regular version registration.

All progress lives in app_control.pgdm_expansion_runs / pgdm_expansion_chunks
inside the data database, in the same transactions as the data. A run that is
interrupted at any point is resumed by starting the same expansion again:
committed chunks are skipped, and a run whose data is already committed goes
straight to version registration.
"""

import hashlib
import json
import math
import os
import queue
import shlex
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from config import CONTROL_SCHEMA
from utils import sql_ident, sql_literal

RUNS_TABLE = f"{sql_ident(CONTROL_SCHEMA)}.{sql_ident('pgdm_expansion_runs')}"
CHUNKS_TABLE = f"{sql_ident(CONTROL_SCHEMA)}.{sql_ident('pgdm_expansion_chunks')}"
GLOBALS_TABLE = f"{sql_ident(CONTROL_SCHEMA)}.{sql_ident('pgdm_expansion_globals')}"
CHUNK_SKIPPED_MARKER = "PGDM_CHUNK_NOT_PENDING"

# Tokens that make a recipe depend on more than the source row it reads.
NON_ROW_LOCAL_WORDS = {
    "distinct", "group", "having", "limit", "offset", "fetch", "over",
    "window", "union", "intersect", "except",
}
AGGREGATE_FUNCTIONS = {
    "count", "sum", "min", "max", "avg", "array_agg", "string_agg",
    "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg",
    "bool_and", "bool_or", "every", "bit_and", "bit_or", "stddev",
    "variance", "percentile_cont", "percentile_disc", "mode",
}

STATE_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {sql_ident(CONTROL_SCHEMA)};
CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
    run_id bigserial PRIMARY KEY,
    run_key text NOT NULL,
    status text NOT NULL,
    source_table text NOT NULL,
    destinations jsonb NOT NULL,
    raw_id_min bigint,
    raw_id_max bigint,
    chunk_count integer NOT NULL DEFAULT 0,
    global_done boolean NOT NULL DEFAULT false,
    manifest jsonb,
    error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS pgdm_expansion_runs_key_idx
    ON {RUNS_TABLE} (run_key, status);
CREATE TABLE IF NOT EXISTS {CHUNKS_TABLE} (
    run_id bigint NOT NULL REFERENCES {RUNS_TABLE} (run_id) ON DELETE CASCADE,
    chunk_no integer NOT NULL,
    raw_id_from bigint NOT NULL,
    raw_id_to bigint NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    rows jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    seconds jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    finished_at timestamptz,
    PRIMARY KEY (run_id, chunk_no)
);
CREATE TABLE IF NOT EXISTS {GLOBALS_TABLE} (
    run_id bigint NOT NULL REFERENCES {RUNS_TABLE} (run_id) ON DELETE CASCADE,
    table_name text NOT NULL,
    rows bigint NOT NULL,
    seconds float8 NOT NULL,
    PRIMARY KEY (run_id, table_name)
);
""".strip()


def _dollar_tag(base: str, *texts: str) -> str:
    tag = f"${base}$"
    suffix = 0
    while any(tag in text for text in texts):
        suffix += 1
        tag = f"${base}_{suffix}$"
    return tag


class ParallelExpansionMixin:
    """Mixed into PostgresAdminService; relies on its psql-over-SSH helpers."""

    # ------------------------------------------------------------------ config
    def _expansion_worker_count(self, cancel_event=None) -> int:
        configured = os.getenv("PDM_EXPANSION_WORKERS", "").strip()
        if configured:
            return max(1, int(configured))
        try:
            cores = int(self.run_remote_command("nproc", cancel_event=cancel_event).strip())
        except Exception:
            cores = 1
        return max(1, min(cores, 16))

    @staticmethod
    def _expansion_chunk_rows(total_rows: int, workers: int) -> int:
        configured = os.getenv("PDM_EXPANSION_CHUNK_ROWS", "").strip()
        if configured:
            return max(1, int(configured))
        # About 4 chunks per worker keeps the pool busy until the end, while
        # 500k rows per chunk bounds the work lost when a chunk is interrupted.
        return max(10_000, min(500_000, math.ceil(total_rows / max(workers * 4, 1))))

    # ---------------------------------------------------------------- planning
    @classmethod
    def _is_row_local_destination(cls, destination: dict) -> bool:
        for statement in destination["statements"]:
            words = {
                value
                for kind, value, _depth in cls._tokenize_managed_sql(statement)
                if kind == "word"
            }
            if words & NON_ROW_LOCAL_WORDS:
                return False
        functions = {name.lower() for _schema, name in destination["function_references"]}
        return not (functions & AGGREGATE_FUNCTIONS)

    @classmethod
    def _plan_parallel_expansion(cls, prepared: dict):
        """Split destinations into a global prefix and a row-local suffix.

        Returns None when the order does not allow it (a global recipe after a
        row-local one), in which case the original engine is used.
        """
        global_destinations = []
        chunk_destinations = []
        for destination in prepared["destinations"]:
            if cls._is_row_local_destination(destination):
                chunk_destinations.append(destination)
            elif chunk_destinations:
                return None
            else:
                global_destinations.append(destination)
        if not chunk_destinations:
            return None
        return global_destinations, chunk_destinations

    @staticmethod
    def _expansion_run_key(database_name: str, prepared: dict) -> str:
        payload = json.dumps(
            {
                "database": database_name,
                "source": [prepared["source_schema_name"], prepared["source_table_name"]],
                "raw_schemas": list(prepared["raw_schemas"]),
                "destinations": [
                    [item["table_name"], item["statements"]]
                    for item in prepared["destinations"]
                ],
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _scope_statement_to_chunk(statement: str, raw_schemas, raw_id_from: int, raw_id_to: int) -> str:
        """Restrict the {{source}} subquery built by the recipe validator to a raw_id range."""
        scope_sql = ", ".join(sql_literal(value) for value in raw_schemas)
        marker = f"    WHERE {sql_ident('raw_schema')} IN ({scope_sql})\n)"
        occurrences = statement.count(marker)
        if occurrences <= 0:
            raise RuntimeError("Could not locate the source scope in an expansion statement.")
        scoped = (
            f"    WHERE {sql_ident('raw_schema')} IN ({scope_sql})\n"
            f"      AND {sql_ident('raw_id')} >= {int(raw_id_from)}\n"
            f"      AND {sql_ident('raw_id')} < {int(raw_id_to)}\n)"
        )
        return statement.replace(marker, scoped)

    # ----------------------------------------------------------------- helpers
    def _run_expansion_psql(self, database_name: str, sql: str, cancel_event=None) -> str:
        command = (
            f"psql -h localhost -U {shlex.quote(self.sql_username)} "
            f"-d {shlex.quote(database_name)} "
            "-X -qAt -v ON_ERROR_STOP=1 -f -"
        )
        # run_remote_command treats any stderr output as a failure, so keep
        # harmless NOTICEs (e.g. CREATE ... IF NOT EXISTS) off stderr.
        sql = "SET client_min_messages = warning;\n" + sql
        return self.run_remote_command(command, stdin_text=sql, cancel_event=cancel_event)

    def _run_expansion_json(self, database_name: str, sql: str, cancel_event=None):
        output = self._run_expansion_psql(database_name, sql, cancel_event=cancel_event)
        for line in reversed(output.splitlines()):
            line = line.strip()
            if line.startswith("{") or line.startswith("["):
                return json.loads(line)
        return None

    @staticmethod
    def _tracked_insert_sql(statement: str, destination_name: str, run_id: int, chunk_no=None) -> str:
        """Run one recipe statement and accumulate its row count and server time.

        Global statements are tracked on the run row, chunk statements on their
        chunk row. Neither touches pgdm_table_row_counts, so concurrent chunks
        never wait on the shared counter rows while they insert.
        """
        tag = _dollar_tag("pgdm_tracked_insert", statement)
        key = sql_literal(destination_name)
        if chunk_no is None:
            track_sql = f"""
    INSERT INTO {GLOBALS_TABLE} AS g (run_id, table_name, rows, seconds)
    VALUES ({int(run_id)}, {key}, pgdm_rows,
            EXTRACT(EPOCH FROM clock_timestamp() - pgdm_started_at))
    ON CONFLICT (run_id, table_name) DO UPDATE
    SET rows = g.rows + EXCLUDED.rows, seconds = g.seconds + EXCLUDED.seconds;"""
        else:
            track_sql = f"""
    UPDATE {CHUNKS_TABLE}
    SET rows = rows || jsonb_build_object(
            {key}, COALESCE((rows ->> {key})::bigint, 0) + pgdm_rows),
        seconds = seconds || jsonb_build_object(
            {key}, COALESCE((seconds ->> {key})::float8, 0)
                + EXTRACT(EPOCH FROM clock_timestamp() - pgdm_started_at))
    WHERE run_id = {int(run_id)} AND chunk_no = {int(chunk_no)};"""
        return f"""
DO {tag}
DECLARE
    pgdm_rows bigint;
    pgdm_started_at timestamptz := clock_timestamp();
BEGIN
    {statement}
    ;
    GET DIAGNOSTICS pgdm_rows = ROW_COUNT;{track_sql}
END
{tag};""".strip()

    @staticmethod
    def _apply_counters_sql(rows_source_sql: str, destinations) -> str:
        """Add tracked row counts to pgdm_table_row_counts in a fixed lock order.

        Runs as the last statement of the transaction that inserted the rows,
        so the counters stay exactly in step with the committed data while the
        counter row locks are held only for milliseconds.
        """
        relations = sorted({(item["schema_name"], item["pure_table_name"], item["table_name"]) for item in destinations})
        updates = "\n".join(
            f"""
    UPDATE public.pgdm_table_row_counts
    SET row_count = row_count + COALESCE((pgdm_counts ->> {sql_literal(full_name)})::bigint, 0),
        updated_at = clock_timestamp()
    WHERE schema_name = {sql_literal(schema_name)} AND table_name = {sql_literal(table_name)};
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Missing row counter for %.% during expansion.',
            {sql_literal(schema_name)}, {sql_literal(table_name)};
    END IF;"""
            for schema_name, table_name, full_name in relations
        )
        return f"""
DO $pgdm_apply_counters$
DECLARE
    pgdm_counts jsonb;
BEGIN
    SELECT {rows_source_sql} INTO pgdm_counts;{updates}
END
$pgdm_apply_counters$;""".strip()

    # ------------------------------------------------------------------ phases
    def _open_or_resume_expansion_run(self, database_name, prepared, run_key, global_destinations, workers, timing_report, cancel_event):
        self._run_expansion_psql(database_name, STATE_DDL, cancel_event=cancel_event)
        existing = self._run_expansion_json(
            database_name,
            f"""
SELECT row_to_json(run_row)::text FROM (
    SELECT run_id, status, chunk_count, global_done, manifest,
           (SELECT count(*) FROM {CHUNKS_TABLE} c WHERE c.run_id = r.run_id AND c.status = 'done') AS chunks_done
    FROM {RUNS_TABLE} r
    WHERE run_key = {sql_literal(run_key)} AND status IN ('running', 'data_committed')
    ORDER BY run_id DESC LIMIT 1
) AS run_row;""",
            cancel_event=cancel_event,
        )
        if existing:
            existing["resumed"] = True
            return existing

        source_sql = f"{sql_ident(prepared['source_schema_name'])}.{sql_ident(prepared['source_table_name'])}"
        scope_sql = ", ".join(sql_literal(value) for value in prepared["raw_schemas"])
        started_at = time.perf_counter()
        manifest_sql = self._build_expansion_manifest_sql(
            prepared["source_schema_name"], prepared["source_table_name"], prepared["raw_schemas"]
        )
        manifest = self._run_expansion_json(
            database_name,
            "BEGIN ISOLATION LEVEL REPEATABLE READ;\n" + manifest_sql + "\nCOMMIT;",
            cancel_event=cancel_event,
        )
        if not manifest or "raw_hash_manifest" not in manifest:
            raise RuntimeError("Could not capture the source manifest for the expansion.")
        bounds = self._run_expansion_json(
            database_name,
            f"""
SELECT json_build_object('min', min({sql_ident('raw_id')}), 'max', max({sql_ident('raw_id')}))::text
FROM {source_sql} WHERE {sql_ident('raw_schema')} IN ({scope_sql});""",
            cancel_event=cancel_event,
        )
        total_rows = int(manifest.get("expected_row_count") or 0)
        raw_id_min, raw_id_max = int(bounds["min"]), int(bounds["max"])
        chunk_rows = self._expansion_chunk_rows(total_rows, workers)
        chunk_count = max(1, math.ceil(total_rows / chunk_rows))
        span = raw_id_max - raw_id_min + 1
        step = math.ceil(span / chunk_count)
        chunk_values = ",\n".join(
            f"({i}, {raw_id_min + i * step}, {min(raw_id_min + (i + 1) * step, raw_id_max + 1)})"
            for i in range(chunk_count)
            if raw_id_min + i * step <= raw_id_max
        )
        destinations_json = json.dumps([item["table_name"] for item in prepared["destinations"]])
        created = self._run_expansion_json(
            database_name,
            f"""
BEGIN;
WITH new_run AS (
    INSERT INTO {RUNS_TABLE} (run_key, status, source_table, destinations, raw_id_min, raw_id_max,
                              chunk_count, global_done, manifest)
    VALUES ({sql_literal(run_key)}, 'running', {sql_literal(prepared['source_schema_name'] + '.' + prepared['source_table_name'])},
            {sql_literal(destinations_json)}::jsonb, {raw_id_min}, {raw_id_max}, {chunk_count},
            {'false' if global_destinations else 'true'}, {sql_literal(json.dumps(manifest))}::jsonb)
    RETURNING run_id
), new_chunks AS (
    INSERT INTO {CHUNKS_TABLE} (run_id, chunk_no, raw_id_from, raw_id_to)
    SELECT new_run.run_id, v.chunk_no, v.raw_id_from, v.raw_id_to
    FROM new_run, (VALUES {chunk_values}) AS v(chunk_no, raw_id_from, raw_id_to)
    RETURNING 1
)
SELECT json_build_object('run_id', (SELECT run_id FROM new_run), 'chunks', (SELECT count(*) FROM new_chunks))::text;
COMMIT;""",
            cancel_event=cancel_event,
        )
        self._append_expansion_timing(
            timing_report, "execute", "Capture source manifest and plan raw_id chunks",
            time.perf_counter() - started_at, "client_wall",
            details=f"{created['chunks']} chunk(s) of about {chunk_rows:,} source rows.",
        )
        return {
            "run_id": created["run_id"], "status": "running", "chunk_count": created["chunks"],
            "global_done": not global_destinations, "manifest": manifest, "chunks_done": 0, "resumed": False,
        }

    def _global_destination_sql(self, run_id, destination) -> str:
        """One global recipe in its own transaction, like the original engine.

        Its row count lands in pgdm_expansion_globals under its table name in
        the same transaction, so the key's presence marks the recipe as done.
        """
        return "\n\n".join(
            [
                "BEGIN ISOLATION LEVEL REPEATABLE READ;",
                "SET LOCAL client_min_messages = warning;",
                self._build_cross_table_expansion_lock_sql([destination]),
                *[
                    self._tracked_insert_sql(statement, destination["table_name"], run_id)
                    for statement in destination["statements"]
                ],
                self._apply_counters_sql(
                    f"jsonb_object_agg(table_name, rows) FROM {GLOBALS_TABLE} "
                    f"WHERE run_id = {int(run_id)} AND table_name = {sql_literal(destination['table_name'])}",
                    [destination],
                ),
                "COMMIT;",
            ]
        )

    def _run_global_expansion_phase(self, database_name, run_id, global_destinations, cancel_event):
        done = self._run_expansion_json(
            database_name,
            f"SELECT COALESCE(jsonb_object_agg(table_name, rows), '{{}}')::text FROM {GLOBALS_TABLE} "
            f"WHERE run_id = {int(run_id)};",
            cancel_event=cancel_event,
        ) or {}
        remaining = [item for item in global_destinations if item["table_name"] not in done]
        global_names = {(item["schema_name"], item["pure_table_name"]) for item in global_destinations}
        independent = not any(
            tuple(relation) in global_names
            for item in global_destinations
            for relation in item["read_relations"]
        )
        if independent and len(remaining) > 1:
            # Global recipes that do not read each other run side by side,
            # each on its own SSH connection and transaction.
            def run_one(destination):
                session = self.__class__()
                session.connect(
                    self.ssh_host, self.ssh_port, self.ssh_username, self.ssh_password,
                    self.postgres_port, self.sql_username, self.sql_password,
                )
                try:
                    session._run_expansion_psql(
                        database_name, self._global_destination_sql(run_id, destination), cancel_event=cancel_event
                    )
                finally:
                    session.close()

            with ThreadPoolExecutor(max_workers=len(remaining)) as pool:
                for future in [pool.submit(run_one, item) for item in remaining]:
                    future.result()
        else:
            for destination in remaining:
                self._run_expansion_psql(
                    database_name, self._global_destination_sql(run_id, destination), cancel_event=cancel_event
                )
        self._run_expansion_psql(
            database_name,
            f"UPDATE {RUNS_TABLE} SET global_done = true, updated_at = clock_timestamp() "
            f"WHERE run_id = {int(run_id)};",
            cancel_event=cancel_event,
        )

    def _chunk_sql(self, run_id, chunk, chunk_destinations, raw_schemas) -> str:
        chunk_no = int(chunk["chunk_no"])
        where = f"run_id = {int(run_id)} AND chunk_no = {chunk_no}"
        statements = [
            self._tracked_insert_sql(
                self._scope_statement_to_chunk(statement, raw_schemas, chunk["raw_id_from"], chunk["raw_id_to"]),
                destination["table_name"],
                run_id,
                chunk_no,
            )
            for destination in chunk_destinations
            for statement in destination["statements"]
        ]
        return "\n\n".join(
            [
                "BEGIN;",
                "SET LOCAL client_min_messages = warning;",
                # Claiming the chunk row first serialises duplicate attempts
                # (for example a worker left over from a dropped SSH session):
                # the second one waits, then finds the chunk no longer pending.
                f"""DO $pgdm_claim$ BEGIN
    UPDATE {CHUNKS_TABLE} SET status = 'running', rows = '{{}}'::jsonb, seconds = '{{}}'::jsonb
    WHERE {where} AND status = 'pending';
    IF NOT FOUND THEN RAISE EXCEPTION '{CHUNK_SKIPPED_MARKER}'; END IF;
END $pgdm_claim$;""",
                *statements,
                self._apply_counters_sql(f"rows FROM {CHUNKS_TABLE} WHERE {where}", chunk_destinations),
                f"UPDATE {CHUNKS_TABLE} SET status = 'done', finished_at = clock_timestamp() WHERE {where};",
                "COMMIT;",
            ]
        )

    def _run_chunk_expansion_phase(self, database_name, run_id, chunk_destinations, raw_schemas, workers,
                                   cancel_event, progress_callback):
        chunks = self._run_expansion_json(
            database_name,
            f"""
SELECT COALESCE(json_agg(json_build_object('chunk_no', chunk_no, 'raw_id_from', raw_id_from,
                                           'raw_id_to', raw_id_to) ORDER BY chunk_no), '[]')::text
FROM {CHUNKS_TABLE} WHERE run_id = {int(run_id)} AND status <> 'done';""",
            cancel_event=cancel_event,
        ) or []
        total = len(chunks)
        if not total:
            return 0

        # sshd allows only MaxSessions (default 10) channels per connection and
        # drops bursts of handshakes (MaxStartups), so the worker connections
        # are opened one after another up front and handed out through a queue.
        sessions = queue.SimpleQueue()
        opened = []
        try:
            for _ in range(min(workers, total)):
                session = self.__class__()
                for attempt in range(1, 6):
                    try:
                        session.connect(
                            self.ssh_host, self.ssh_port, self.ssh_username, self.ssh_password,
                            self.postgres_port, self.sql_username, self.sql_password,
                        )
                        break
                    except Exception:
                        if attempt == 5:
                            raise
                        time.sleep(0.5 * attempt)
                opened.append(session)
                sessions.put(session)
        except Exception:
            for session in opened:
                session.close()
            raise
        workers = len(opened)

        def run_chunk(chunk):
            try:
                session = sessions.get()
                try:
                    session._run_expansion_psql(
                        database_name,
                        self._chunk_sql(run_id, chunk, chunk_destinations, raw_schemas),
                        cancel_event=cancel_event,
                    )
                finally:
                    sessions.put(session)
            except RuntimeError as exc:
                if CHUNK_SKIPPED_MARKER not in str(exc):
                    raise
            return chunk["chunk_no"]

        pending = list(chunks)
        finished = 0
        failure = None
        with ThreadPoolExecutor(max_workers=workers) as pool:
            in_flight = set()
            while pending or in_flight:
                while pending and len(in_flight) < workers and failure is None:
                    if cancel_event is not None and cancel_event.is_set():
                        pending.clear()
                        break
                    in_flight.add(pool.submit(run_chunk, pending.pop(0)))
                if not in_flight:
                    break
                done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    try:
                        future.result()
                        finished += 1
                        self._notify_progress(
                            progress_callback, "execute", 10 + 80 * finished / total,
                            f"Committed chunk {finished}/{total}.",
                        )
                    except Exception as exc:  # keep committed chunks; stop scheduling new ones
                        failure = failure or exc
                        pending.clear()
        for session in opened:
            try:
                session.close()
            except Exception:
                pass
        if failure is not None:
            raise RuntimeError(
                f"Expansion stopped after {finished}/{total} chunk(s); committed chunks are kept "
                f"and will be skipped when the same expansion is started again. Cause: {failure}"
            )
        self._raise_if_cancelled(cancel_event)
        return total

    def _finalize_parallel_expansion(self, database_name, prepared, run, cancel_event):
        manifest_sql = self._build_expansion_manifest_sql(
            prepared["source_schema_name"], prepared["source_table_name"], prepared["raw_schemas"]
        )
        current = self._run_expansion_json(
            database_name,
            "BEGIN ISOLATION LEVEL REPEATABLE READ;\n" + manifest_sql + "\nCOMMIT;",
            cancel_event=cancel_event,
        )
        captured = run["manifest"]
        keys = ("expected_row_count", "raw_hash_manifest")
        if not current or any(current.get(key) != captured.get(key) for key in keys):
            self._run_expansion_psql(
                database_name,
                f"UPDATE {RUNS_TABLE} SET status = 'source_changed', updated_at = clock_timestamp(), "
                f"error = 'Source rows changed while the expansion was running.' "
                f"WHERE run_id = {int(run['run_id'])};",
                cancel_event=cancel_event,
            )
            raise RuntimeError(
                "The source changed while the expansion was running; the run was marked "
                "source_changed and no version was registered."
            )
        return self._run_expansion_json(
            database_name,
            f"""
BEGIN;
DO $pgdm_finalize$ BEGIN
    IF EXISTS (SELECT 1 FROM {CHUNKS_TABLE} WHERE run_id = {int(run['run_id'])} AND status <> 'done')
       OR NOT (SELECT global_done FROM {RUNS_TABLE} WHERE run_id = {int(run['run_id'])}) THEN
        RAISE EXCEPTION 'Expansion run % still has unfinished work.', {int(run['run_id'])};
    END IF;
END $pgdm_finalize$;
UPDATE {RUNS_TABLE} SET status = 'data_committed', updated_at = clock_timestamp()
WHERE run_id = {int(run['run_id'])} AND status = 'running';
COMMIT;
{self._run_totals_sql(run['run_id'])}""",
            cancel_event=cancel_event,
        )

    @staticmethod
    def _run_totals_sql(run_id) -> str:
        return f"""
SELECT json_build_object(
    'global_rows', (SELECT COALESCE(jsonb_object_agg(table_name, rows), '{{}}'::jsonb)
                    FROM {GLOBALS_TABLE} WHERE run_id = {int(run_id)}),
    'chunk_rows', (SELECT COALESCE(jsonb_object_agg(key, total), '{{}}'::jsonb) FROM (
        SELECT key, sum(value::bigint) AS total FROM {CHUNKS_TABLE}, jsonb_each_text(rows)
        WHERE run_id = {int(run_id)} GROUP BY key) t),
    'chunk_seconds', (SELECT COALESCE(jsonb_object_agg(key, total), '{{}}'::jsonb) FROM (
        SELECT key, sum(value::float8) AS total FROM {CHUNKS_TABLE}, jsonb_each_text(seconds)
        WHERE run_id = {int(run_id)} GROUP BY key) t)
)::text;"""

    def complete_cross_table_expansion_run(self, expansion_result: dict, cancel_event=None):
        run_id = (expansion_result or {}).get("parallel_run_id")
        if not run_id:
            return
        self._run_expansion_psql(
            expansion_result["source_database_name"],
            f"UPDATE {RUNS_TABLE} SET status = 'completed', updated_at = clock_timestamp() "
            f"WHERE run_id = {int(run_id)} AND status = 'data_committed';",
            cancel_event=cancel_event,
        )

    # -------------------------------------------------------------- entry point
    def _try_execute_parallel_cross_table_expansion(
        self, database_name, prepared, source_columns, source_versions,
        timing_report, cancel_event=None, progress_callback=None, post_commit_callback=None,
    ):
        """Return the expansion result, or None to fall back to the original engine."""
        if "raw_id" not in source_columns:
            return None
        plan = self._plan_parallel_expansion(prepared)
        if plan is None:
            return None
        workers = self._expansion_worker_count(cancel_event=cancel_event)
        if workers <= 1:
            return None
        global_destinations, chunk_destinations = plan
        run_key = self._expansion_run_key(database_name, prepared)

        run = self._open_or_resume_expansion_run(
            database_name, prepared, run_key, global_destinations, workers, timing_report, cancel_event
        )
        if run.get("resumed"):
            self._notify_progress(
                progress_callback, "execute", 5,
                f"Resuming expansion run {run['run_id']} ({run['chunks_done']}/{run['chunk_count']} chunks done).",
            )

        if run["status"] == "running":
            if not run["global_done"]:
                started_at = time.perf_counter()
                self._run_global_expansion_phase(database_name, run["run_id"], global_destinations, cancel_event)
                self._append_expansion_timing(
                    timing_report, "execute",
                    "Global recipes (" + ", ".join(d["table_name"] for d in global_destinations) + ")",
                    time.perf_counter() - started_at, "client_wall",
                )
            started_at = time.perf_counter()
            chunks_run = self._run_chunk_expansion_phase(
                database_name, run["run_id"], chunk_destinations, prepared["raw_schemas"],
                workers, cancel_event, progress_callback,
            )
            self._append_expansion_timing(
                timing_report, "execute",
                f"Row-local recipes in {chunks_run} chunk(s) on {workers} parallel session(s)",
                time.perf_counter() - started_at, "client_wall",
            )
            started_at = time.perf_counter()
            totals = self._finalize_parallel_expansion(database_name, prepared, run, cancel_event)
            self._append_expansion_timing(
                timing_report, "execute", "Verify source manifest and close the run",
                time.perf_counter() - started_at, "client_wall",
            )
        else:
            totals = self._run_expansion_json(database_name, self._run_totals_sql(run["run_id"]), cancel_event=cancel_event)

        if post_commit_callback:
            post_commit_callback()

        rows_by_destination = {}
        for group in ("global_rows", "chunk_rows"):
            for name, value in (totals.get(group) or {}).items():
                rows_by_destination[name] = rows_by_destination.get(name, 0) + int(value)
        for destination in prepared["destinations"]:
            name = destination["table_name"]
            self._append_expansion_timing(
                timing_report, "execute", f"Insert into {name} (server time summed over chunks)",
                float((totals.get("chunk_seconds") or {}).get(name) or 0.0), "postgresql_backend_cumulative",
                rows=rows_by_destination.get(name, 0), destination=name, include_in_total=False,
                details="Sum of per-chunk server time; chunks overlap, so this exceeds wall time.",
            )
        manifest = run["manifest"]
        timing_report["source_rows"] = int(manifest.get("expected_row_count") or 0)
        timing_report["destination_rows"] = [
            {"table_name": d["table_name"], "rows": int(rows_by_destination.get(d["table_name"], 0))}
            for d in prepared["destinations"]
        ]
        timing_report["total_rows_inserted"] = sum(item["rows"] for item in timing_report["destination_rows"])
        return {
            "source_database_name": database_name,
            "source_schema_name": prepared["source_schema_name"],
            "source_table_name": prepared["source_table_name"],
            "raw_schemas": list(prepared["raw_schemas"]),
            "source_versions": source_versions,
            "dependency_manifest": manifest,
            "destinations": prepared["destinations"],
            "timing_report": timing_report,
            "parallel_run_id": run["run_id"],
        }
