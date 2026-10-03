import os
import unittest
from pathlib import Path
from unittest import mock

from services.postgres_service import PostgresAdminService


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RECIPE_DIR = PROJECT_ROOT / "SQL files"
RECIPE_DESTINATIONS = (
    ("01_table3.sql", "public.table3"),
    ("02_table4.sql", "public.table4"),
    ("03_table1.sql", "public.table1"),
    ("04_table5.sql", "public.table5"),
    ("05_table6.sql", "public.table6"),
    ("06_table2.sql", "public.table2"),
)


def prepare(destinations, raw_schemas=("group001",)):
    return PostgresAdminService._prepare_cross_table_expansion(
        "public.raw_data", list(raw_schemas), destinations
    )


def challenge_destinations():
    return [
        {
            "table_name": table_name,
            "version_title": "Synthetic expansion",
            "sql": (RECIPE_DIR / file_name).read_text(encoding="utf-8"),
        }
        for file_name, table_name in RECIPE_DESTINATIONS
    ]


class ParallelExpansionPlanTests(unittest.TestCase):
    def test_challenge_recipes_split_into_global_and_row_local(self):
        plan = PostgresAdminService._plan_parallel_expansion(prepare(challenge_destinations()))
        self.assertIsNotNone(plan)
        global_destinations, chunk_destinations = plan
        self.assertEqual(
            [item["table_name"] for item in global_destinations],
            ["public.table3", "public.table4"],
        )
        self.assertEqual(
            [item["table_name"] for item in chunk_destinations],
            ["public.table1", "public.table5", "public.table6", "public.table2"],
        )

    def test_global_recipe_after_row_local_falls_back(self):
        destinations = challenge_destinations()
        # table3 (DISTINCT) after table4 is fine, but a DISTINCT recipe placed
        # after a row-local one cannot be chunked.
        destinations = [destinations[1], destinations[0]] + destinations[2:]
        row_local_first = [
            {
                "table_name": "public.table4",
                "version_title": "x",
                "sql": "INSERT INTO public.table4 (text_column_1) "
                       "SELECT source_row.raw ->> 'a' FROM {{source}} AS source_row",
            },
            {
                "table_name": "public.table3",
                "version_title": "x",
                "sql": "INSERT INTO public.table3 (text_column_1) "
                       "SELECT DISTINCT source_row.raw ->> 'a' FROM {{source}} AS source_row",
            },
        ]
        self.assertIsNone(PostgresAdminService._plan_parallel_expansion(prepare(row_local_first)))
        self.assertIsNotNone(PostgresAdminService._plan_parallel_expansion(prepare(destinations)))

    def test_aggregate_recipe_is_global(self):
        destination = prepare(
            [
                {
                    "table_name": "public.table4",
                    "version_title": "x",
                    "sql": "INSERT INTO public.table4 (text_column_1) "
                           "SELECT max(source_row.raw ->> 'a') FROM {{source}} AS source_row",
                }
            ]
        )["destinations"][0]
        self.assertFalse(PostgresAdminService._is_row_local_destination(destination))

    def test_chunk_scope_restricts_every_source_reference(self):
        prepared = prepare(challenge_destinations(), raw_schemas=("group001", "group002"))
        for destination in prepared["destinations"]:
            for statement in destination["statements"]:
                scoped = PostgresAdminService._scope_statement_to_chunk(
                    statement, prepared["raw_schemas"], 100, 200
                )
                self.assertEqual(scoped.count('"raw_id" >= 100'), statement.count("FROM \"public\".\"raw_data\""))
                self.assertEqual(scoped.count('"raw_id" < 200'), scoped.count('"raw_id" >= 100'))

    def test_run_key_changes_with_recipe(self):
        first = prepare(challenge_destinations())
        changed_destinations = challenge_destinations()
        changed_destinations[2]["sql"] = changed_destinations[2]["sql"].replace("* 1000", "* 100")
        second = prepare(changed_destinations)
        self.assertNotEqual(
            PostgresAdminService._expansion_run_key("db", first),
            PostgresAdminService._expansion_run_key("db", second),
        )
        self.assertEqual(
            PostgresAdminService._expansion_run_key("db", first),
            PostgresAdminService._expansion_run_key("db", prepare(challenge_destinations())),
        )


TABLE6_FKS = [
    {
        "name": "table6_id_column_2_fkey", "match": "s",
        "child_schema": "public", "child_table": "table6", "child_cols": ["id_column_2"],
        "parent_schema": "public", "parent_table": "table1", "parent_cols": ["id_column_1"],
    },
    {
        "name": "table6_id_column_3_fkey", "match": "s",
        "child_schema": "public", "child_table": "table6", "child_cols": ["id_column_3"],
        "parent_schema": "public", "parent_table": "table4", "parent_cols": ["id_column_1"],
    },
]


class BulkForeignKeyModeTests(unittest.TestCase):
    def setUp(self):
        prepared = prepare(challenge_destinations())
        self.prepared = prepared
        self.chunk_destinations = PostgresAdminService._plan_parallel_expansion(prepared)[1]

    def plan(self, info, env="1"):
        service = PostgresAdminService()
        with mock.patch.dict(os.environ, {"PDM_EXPANSION_BULK_FK": env}), \
                mock.patch.object(service, "_run_expansion_json", return_value=info):
            return service._plan_bulk_fk("db", self.prepared, self.chunk_destinations)

    def test_disabled_unless_opted_in(self):
        self.assertEqual(self.plan({}, env=""), (None, None))

    def test_refused_without_superuser(self):
        plan, note = self.plan({"superuser": False, "user_triggers": 0, "foreign_keys": TABLE6_FKS})
        self.assertIsNone(plan)
        self.assertIn("superuser", note)

    def test_refused_with_user_triggers(self):
        plan, note = self.plan({"superuser": True, "user_triggers": 1, "foreign_keys": TABLE6_FKS})
        self.assertIsNone(plan)
        self.assertIn("trigger", note)

    def test_parents_outside_chunks_get_share_locked(self):
        plan, note = self.plan({"superuser": True, "user_triggers": 0, "foreign_keys": TABLE6_FKS})
        self.assertIsNone(note)
        self.assertEqual(plan["share_lock_parents"], ['"public"."table4"'])
        self.assertEqual(len(plan["foreign_keys"]["public.table6"]), 2)

    def test_checked_insert_validates_every_constraint(self):
        body = PostgresAdminService._checked_insert_body(
            "INSERT INTO public.table6 (id_column_2, id_column_3) SELECT 1, 2", TABLE6_FKS
        )
        self.assertIn('RETURNING "id_column_2", "id_column_3"', body)
        self.assertEqual(body.count("NOT EXISTS"), 2)
        self.assertIn("foreign_key_violation", body)


if __name__ == "__main__":
    unittest.main()
