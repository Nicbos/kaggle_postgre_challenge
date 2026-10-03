"""Headless runner for the DB Manager Expand operation (no GUI).

Usage (inside the runner container, from the repo root): python benchmark/bench.py [init|expand] [--report file.json]
"""
import argparse
import json
import os
import socket
import sys
import time
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("cmd", choices=["init", "expand"])
ap.add_argument("--repo", default=".")
ap.add_argument("--db", default="kaggle_challenge")
ap.add_argument("--report", default=None)
args = ap.parse_args()

repo = Path(args.repo).resolve()
sys.path.insert(0, str(repo))
from services.postgres_service import PostgresAdminService  # noqa: E402

RECIPES = (
    ("01_table3.sql", "public.table3"),
    ("02_table4.sql", "public.table4"),
    ("03_table1.sql", "public.table1"),
    ("04_table5.sql", "public.table5"),
    ("05_table6.sql", "public.table6"),
    ("06_table2.sql", "public.table2"),
)

svc = PostgresAdminService()
svc.connect(os.environ.get("SSH_HOST", "127.0.0.1"), int(os.environ.get("SSH_PORT", "2222")), "bench", "bench", 5432, "bench", "bench")

if args.cmd == "init":
    svc.ensure_admin_schema()
    print("admin schema ready")
    sys.exit(0)

source = "public.raw_data"
schemas = svc.list_table_raw_schemas(args.db, source)
raw_schemas = svc.resolve_raw_schema_interval(schemas, schemas[0], schemas[-1])
destinations = [
    {
        "table_name": table,
        "version_title": "bench",
        "sql": (repo / "SQL files" / name).read_text(encoding="utf-8"),
    }
    for name, table in RECIPES
]

t0 = time.perf_counter()
report = svc.create_cross_table_expansion_timing_report(
    args.db, source, raw_schemas, [d["table_name"] for d in destinations]
)
with svc.expansion_timing_scope(report, "prepare", "Ensure control database metadata"):
    svc.ensure_admin_schema()
result = svc.execute_cross_table_expansion(
    database_name=args.db,
    source_full_table_name=source,
    raw_schemas=raw_schemas,
    destinations=destinations,
    timing_report=report,
)
versions = svc.register_cross_table_expansion_versions(
    expansion_result=result,
    requested_by="bench",
    workstation_name=socket.gethostname(),
    timing_report=report,
)
wall = time.perf_counter() - t0
print("versions:", ", ".join(f"{v['table_name']}={v['version_code']}" for v in versions))
print(f"WALL {wall:.2f}s")
if args.report:
    Path(args.report).write_text(json.dumps(report, default=str, indent=1))
