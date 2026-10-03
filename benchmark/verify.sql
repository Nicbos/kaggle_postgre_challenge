-- Content fingerprint of the expansion output, independent of surrogate ids.
WITH t3 AS (SELECT id_column_1 AS id, to_jsonb(t) - 'id_column_1' AS nat FROM table3 t),
t1 AS (SELECT t.id_column_1 AS id, t.id_column_3 AS raw_id,
              (to_jsonb(t) - 'id_column_1' - 'id_column_4') || jsonb_build_object('dim', t3.nat) AS j
       FROM table1 t LEFT JOIN t3 ON t3.id = t.id_column_4)
SELECT 'table3', count(*), sum(('x' || left(md5((nat::text)::text), 15))::bit(60)::bigint::numeric)::text FROM t3
UNION ALL SELECT 'table4', count(*), md5(string_agg(to_jsonb(t)::text, '|' ORDER BY to_jsonb(t)::text)) FROM table4 t
UNION ALL SELECT 'table1', count(*), sum(('x' || left(md5((j::text)::text), 15))::bit(60)::bigint::numeric)::text FROM t1
UNION ALL SELECT 'table2', count(*), sum(('x' || left(md5((x::text)::text), 15))::bit(60)::bigint::numeric)::text FROM (
    SELECT (to_jsonb(c) - 'id_column_1' - 'id_column_2') || jsonb_build_object('raw', t1.raw_id) AS x
    FROM table2 c JOIN t1 ON t1.id = c.id_column_2) s
UNION ALL SELECT 'table5', count(*), sum(('x' || left(md5((x::text)::text), 15))::bit(60)::bigint::numeric)::text FROM (
    SELECT (to_jsonb(c) - 'id_column_1' - 'id_column_2') || jsonb_build_object('raw', t1.raw_id) AS x
    FROM table5 c JOIN t1 ON t1.id = c.id_column_2) s
UNION ALL SELECT 'table6', count(*), sum(('x' || left(md5((x::text)::text), 15))::bit(60)::bigint::numeric)::text FROM (
    SELECT (to_jsonb(c) - 'id_column_1' - 'id_column_2') || jsonb_build_object('raw', t1.raw_id) AS x
    FROM table6 c JOIN t1 ON t1.id = c.id_column_2) s
UNION ALL SELECT 'counters_ok', count(*) FILTER (WHERE ok), string_agg(table_name || '=' || ok, ',' ORDER BY table_name) FROM (
    SELECT table_name, row_count = (xpath('/row/c/text()', query_to_xml(format('select count(*) as c from public.%I', table_name), false, true, '')))[1]::text::bigint AS ok
    FROM public.pgdm_table_row_counts WHERE schema_name = 'public' AND table_name LIKE 'table%') s
ORDER BY 1;
