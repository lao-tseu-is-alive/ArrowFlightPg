-- tableGet
WITH table_info AS (
    SELECT
        c.oid AS table_oid,
        n.nspname AS schema_name,
        c.relname AS table_name,
        c.reltuples AS row_count,
        pg_total_relation_size(c.oid) AS size_bytes,
        c.relkind AS table_type,
        CASE
            WHEN c.relkind = 'r' THEN 'table'
            WHEN c.relkind = 'v' THEN 'view'
            WHEN c.relkind = 't' THEN 'TOAST table'
            WHEN c.relkind = 'm' THEN 'materialized view'
            ELSE 'other'
            END AS table_type_name,
        pg_stat_get_last_vacuum_time(c.oid) AS last_vacuum,
        pg_stat_get_last_autovacuum_time(c.oid) AS last_autovacuum,
        obj_description(c.oid, 'pg_class') AS table_comment  -- Retrieve table comment
    FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'v', 'm')  -- r: table, v: view, t: TOAST table, m: materialized view
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
),
     column_info AS (
         SELECT
             c.table_schema AS schema_name,
             c.table_name,
             c.column_name AS name,
             c.data_type,
             c.is_nullable = 'YES' AS is_nullable
         FROM information_schema.columns c
     ),
     primary_keys AS (
         SELECT
             tc.table_schema AS schema_name,
             tc.table_name,
             kcu.column_name
         FROM information_schema.table_constraints tc
                  JOIN information_schema.key_column_usage kcu
                       ON tc.constraint_name = kcu.constraint_name
                           AND tc.table_schema = kcu.table_schema
                           AND tc.table_name = kcu.table_name
         WHERE tc.constraint_type = 'PRIMARY KEY'
     ),
     foreign_keys AS (
         SELECT
             tc.table_schema AS schema_name,
             tc.table_name,
             kcu.column_name AS column_name,
             ccu.table_schema AS referenced_schema,
             ccu.table_name AS referenced_table,
             ccu.column_name AS referenced_column
         FROM information_schema.table_constraints tc
                  JOIN information_schema.key_column_usage kcu
                       ON tc.constraint_name = kcu.constraint_name
                  JOIN information_schema.constraint_column_usage ccu
                       ON tc.constraint_name = ccu.constraint_name
         WHERE tc.constraint_type = 'FOREIGN KEY'
     ),
     indexes AS (
         SELECT
             n.nspname AS schema_name,
             c.relname AS table_name,
             i.relname AS index_name
         FROM pg_index idx
                  JOIN pg_class c ON c.oid = idx.indrelid
                  JOIN pg_class i ON i.oid = idx.indexrelid
                  JOIN pg_namespace n ON c.relnamespace = n.oid
     )
SELECT
    ti.table_oid,
    ti.schema_name,
    ti.table_name,
    ti.row_count,
    ti.size_bytes,
    ti.table_type,
    ti.table_type_name,  -- Added human-readable table type
    ti.table_comment,     -- Table comment
    json_agg(DISTINCT jsonb_build_object(
            'name', ci.name,
            'data_type', ci.data_type,
            'is_nullable', ci.is_nullable
                      )) AS columns,
    json_agg(DISTINCT pk.column_name) FILTER (WHERE pk.column_name IS NOT NULL) AS primary_key,
    json_agg(DISTINCT jsonb_build_object(
            'column', fk.column_name,
            'references', jsonb_build_object(
                    'schema', fk.referenced_schema,
                    'table', fk.referenced_table,
                    'column', fk.referenced_column
                          )
                      )) FILTER (WHERE fk.column_name IS NOT NULL) AS foreign_keys,
    json_agg(DISTINCT idx.index_name) FILTER (WHERE idx.index_name IS NOT NULL) AS indexes
FROM table_info ti
         LEFT JOIN column_info ci ON ti.schema_name = ci.schema_name AND ti.table_name = ci.table_name
         LEFT JOIN primary_keys pk ON ti.schema_name = pk.schema_name AND ti.table_name = pk.table_name
         LEFT JOIN foreign_keys fk ON ti.schema_name = fk.schema_name AND ti.table_name = fk.table_name
         LEFT JOIN indexes idx ON ti.schema_name = idx.schema_name AND ti.table_name = idx.table_name
GROUP BY ti.table_oid,ti.schema_name, ti.table_name, ti.row_count, ti.size_bytes, ti.table_type, ti.table_type_name, ti.table_comment
ORDER BY ti.schema_name, ti.table_name, ti.table_type_name;

SELECT
    c.oid::int AS table_id,
    n.nspname AS schema_name,
    c.relname AS table_name,
    c.relkind::text AS table_type
FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schema_name, table_name, table_type;

SELECT
    c.oid:int AS table_id,
    n.nspname AS schema_name,
    c.relname AS table_name,
    c.relkind AS table_type
FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schema_name, table_name, table_type; -- r: table, v: view, t: TOAST table, m: materialized view

--tablesCount
SELECT COUNT(*) as num_tables
FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
AND n.nspname = 'public';

SELECT COUNT(*) as num_tables, n.nspname as schema_name
FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
GROUP BY n.nspname;

--schemasList
SELECT  DISTINCT(n.nspname) as schema_name
FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm') AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY n.nspname;

--tableSchema
SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE
            table_schema = 'public'
            AND table_name = 'goeland_addresse_lausanne'
ORDER BY ordinal_position;



GRANT CONNECT ON DATABASE lausanne TO arrow_flight_pg;
GRANT USAGE ON SCHEMA public TO arrow_flight_pg;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO arrow_flight_pg;
GRANT USAGE ON SCHEMA geodata_altimetrie TO arrow_flight_pg;
GRANT SELECT ON ALL TABLES IN SCHEMA geodata_altimetrie TO arrow_flight_pg;
GRANT USAGE ON SCHEMA geodata_bdcad TO arrow_flight_pg;
GRANT SELECT ON ALL TABLES IN SCHEMA geodata_bdcad TO arrow_flight_pg;