-- pg_oracle 0.1 -- Oracle compatibility for Postgres. Part of the Polygres
-- extension collection (see db/pg_oracle/README.md for scope and phasing).
--
-- Layout: one schema per "thing a real Oracle client expects to find
-- unqualified" -- oracle_catalog for V$/GV$/DBA_*/USER_*/ALL_* views,
-- dbms_output/utl_file/etc. per package (Oracle's own PACKAGE.PROCEDURE
-- naming maps onto Postgres schema.function this way with zero renaming).
-- `SET db_emulation = 'oracle'` (src/pg_oracle.c) puts these schemas on
-- search_path so unqualified references resolve exactly as Oracle client
-- code expects; nothing here requires the caller to know these schema
-- names exist.

-- ================================================================
-- Phase 1a: V$ / GV$ views
--
-- Built as thin views over Postgres's own stats/catalog views, matching
-- Oracle's column names and rough semantics -- not Oracle's actual
-- internal structures (there's no redo log, no SGA in Postgres's sense,
-- etc.). GV$x = V$x plus a leading INST_ID column pinned to 1: this
-- Postgres instance is "instance 1"; a real multi-instance GV$ (Polywire
-- sharding fan-out) is future work, not this file's problem to solve.
-- ================================================================

CREATE SCHEMA oracle_catalog;
COMMENT ON SCHEMA oracle_catalog IS
  'Oracle-compatible V$/GV$/DBA_*/USER_*/ALL_* catalog views (pg_oracle, part of Polygres).';

CREATE VIEW oracle_catalog."v$version" AS
SELECT 'PostgreSQL (pg_oracle emulation) ' || current_setting('server_version') AS banner,
       1 AS con_id;
COMMENT ON VIEW oracle_catalog."v$version" IS 'Oracle V$VERSION -- server identification.';

CREATE VIEW oracle_catalog."v$instance" AS
SELECT 1 AS instance_number,
       current_setting('cluster_name') AS instance_name,
       inet_server_addr()::text AS host_name,
       current_setting('server_version') AS version,
       'OPEN' AS status,
       pg_postmaster_start_time() AS startup_time;
COMMENT ON VIEW oracle_catalog."v$instance" IS 'Oracle V$INSTANCE -- one row, this Postgres instance.';

CREATE VIEW oracle_catalog."v$database" AS
SELECT current_database() AS name,
       system_identifier::text AS dbid,
       'READ WRITE' AS open_mode,
       pg_postmaster_start_time() AS created
FROM pg_control_system();
COMMENT ON VIEW oracle_catalog."v$database" IS 'Oracle V$DATABASE.';

CREATE VIEW oracle_catalog."v$parameter" AS
SELECT row_number() OVER (ORDER BY name) AS num,
       name,
       setting AS value,
       vartype AS type,
       context,
       short_desc AS description,
       (source = 'default') AS isdefault
FROM pg_settings;
COMMENT ON VIEW oracle_catalog."v$parameter" IS 'Oracle V$PARAMETER over pg_settings -- names/semantics differ per-GUC, this is a mapping of shape not of every individual parameter.';

CREATE VIEW oracle_catalog."v$session" AS
SELECT pid AS sid,
       0 AS "serial#",
       usename AS username,
       application_name AS program,
       client_addr::text AS machine,
       backend_start,
       state AS status,
       wait_event_type,
       wait_event,
       query AS sql_text,
       datname AS schemaname
FROM pg_stat_activity;
COMMENT ON VIEW oracle_catalog."v$session" IS 'Oracle V$SESSION over pg_stat_activity.';

CREATE VIEW oracle_catalog."gv$session" AS
SELECT 1 AS inst_id, * FROM oracle_catalog."v$session";
COMMENT ON VIEW oracle_catalog."gv$session" IS 'Oracle GV$SESSION -- single-instance today, see file header.';

CREATE VIEW oracle_catalog."v$sql" AS
SELECT queryid::text AS sql_id,
       query AS sql_text,
       calls AS executions,
       round(total_exec_time) AS elapsed_time,
       round(mean_exec_time) AS avg_elapsed_time,
       rows
FROM pg_stat_statements;
COMMENT ON VIEW oracle_catalog."v$sql" IS 'Oracle V$SQL over pg_stat_statements -- requires that extension also be loaded.';

-- V$SQL_PLAN: Oracle serves this straight from the shared pool's already-
-- computed plan for a cursor. Stock Postgres has no equivalent cache of
-- physical plans keyed by statement id -- pg_stat_statements tracks
-- execution *stats*, not the plan itself -- so this re-derives a plan on
-- demand via EXPLAIN against the sql_id's stored query text. Two real
-- limits, stated rather than hidden: (1) a freshly-derived plan can differ
-- from whatever plan actually ran a given execution (parameter sniffing,
-- stats changes since) -- Oracle's is the literal cached plan, this is a
-- best-effort re-explain; (2) pg_stat_statements normalizes literals to
-- `$1`-style placeholders, and EXPLAIN needs real values, so a normalized
-- statement returns an explanatory row instead of a plan. A query run with
-- literal values inline (no bind params) explains cleanly.
-- Called as a function, not queried as a bare view (`SELECT * FROM
-- v$sql_plan('<sql_id>')`), since a real per-row correlated EXPLAIN
-- doesn't fit a plain view -- see README.md for the exact syntax delta.
CREATE FUNCTION oracle_catalog."v$sql_plan"(p_sql_id text)
RETURNS TABLE(sql_id text, id int, operation text, plan_line text)
LANGUAGE plpgsql AS $$
DECLARE
  v_query text;
  v_line  text;
  v_id    int := 0;
BEGIN
  SELECT s.sql_text INTO v_query FROM oracle_catalog."v$sql" s WHERE s.sql_id = p_sql_id;

  IF v_query IS NULL THEN
    RETURN;
  END IF;

  IF v_query ~ '\$\d+' THEN
    sql_id := p_sql_id;
    id := 0;
    operation := 'N/A';
    plan_line := 'pg_oracle: this sql_id''s text is bind-parameterized ($1, $2, ...) -- '
                 || 'EXPLAIN needs literal values, so no plan can be derived. See '
                 || 'db/pg_oracle/README.md''s V$SQL_PLAN section.';
    RETURN NEXT;
    RETURN;
  END IF;

  FOR v_line IN EXECUTE format('EXPLAIN %s', v_query) LOOP
    sql_id := p_sql_id;
    id := v_id;
    operation := split_part(btrim(v_line), ' ', 1);
    plan_line := v_line;
    RETURN NEXT;
    v_id := v_id + 1;
  END LOOP;
END;
$$;
COMMENT ON FUNCTION oracle_catalog."v$sql_plan"(text) IS 'Oracle V$SQL_PLAN -- best-effort re-EXPLAIN of a v$sql row''s query text; see the function''s own comment above for the two real fidelity limits vs. real Oracle.';

CREATE VIEW oracle_catalog."v$lock" AS
SELECT pid AS sid,
       locktype AS type,
       mode AS lmode,
       granted,
       relation::regclass::text AS object_name
FROM pg_locks;
COMMENT ON VIEW oracle_catalog."v$lock" IS 'Oracle V$LOCK over pg_locks.';

CREATE VIEW oracle_catalog."v$transaction" AS
SELECT pid AS addr,
       backend_xid::text AS xidusn,
       xact_start AS start_time,
       state AS status
FROM pg_stat_activity
WHERE backend_xid IS NOT NULL;
COMMENT ON VIEW oracle_catalog."v$transaction" IS 'Oracle V$TRANSACTION over pg_stat_activity.';

-- ================================================================
-- Phase 1b: DBA_* / USER_* / ALL_* views
--
-- DBA_* sees everything; USER_* filters to objects owned by
-- current_user; ALL_* filters to objects the current user can see via
-- privilege (approximated here as "owned by current_user OR in a schema
-- on search_path" -- a real privilege-accurate ALL_* is future work, not
-- v1: most migrated tooling only ever queries its own schema anyway).
-- ================================================================

CREATE VIEW oracle_catalog."dba_tables" AS
SELECT schemaname AS owner,
       tablename AS table_name,
       tableowner AS tablespace_name
FROM pg_catalog.pg_tables;
COMMENT ON VIEW oracle_catalog."dba_tables" IS 'Oracle DBA_TABLES over pg_tables.';

CREATE VIEW oracle_catalog."user_tables" AS
SELECT table_name, tablespace_name FROM oracle_catalog."dba_tables" WHERE owner = current_user;

CREATE VIEW oracle_catalog."all_tables" AS
SELECT * FROM oracle_catalog."dba_tables"
WHERE owner = current_user OR owner = ANY (current_schemas(false));

CREATE VIEW oracle_catalog."dba_tab_columns" AS
SELECT table_schema AS owner,
       table_name,
       column_name,
       data_type,
       character_maximum_length AS data_length,
       numeric_precision AS data_precision,
       numeric_scale AS data_scale,
       is_nullable AS nullable,
       ordinal_position AS column_id
FROM information_schema.columns;
COMMENT ON VIEW oracle_catalog."dba_tab_columns" IS 'Oracle DBA_TAB_COLUMNS over information_schema.columns.';

CREATE VIEW oracle_catalog."user_tab_columns" AS
SELECT table_name, column_name, data_type, data_length, data_precision, data_scale, nullable, column_id
FROM oracle_catalog."dba_tab_columns" WHERE owner = current_user;

CREATE VIEW oracle_catalog."dba_indexes" AS
SELECT schemaname AS owner,
       tablename AS table_name,
       indexname AS index_name,
       'VALID' AS status
FROM pg_catalog.pg_indexes;
COMMENT ON VIEW oracle_catalog."dba_indexes" IS 'Oracle DBA_INDEXES over pg_indexes.';

CREATE VIEW oracle_catalog."dba_objects" AS
SELECT n.nspname AS owner,
       c.relname AS object_name,
       CASE c.relkind
         WHEN 'r' THEN 'TABLE'
         WHEN 'v' THEN 'VIEW'
         WHEN 'i' THEN 'INDEX'
         WHEN 'S' THEN 'SEQUENCE'
         ELSE 'OTHER'
       END AS object_type,
       c.oid::text AS object_id
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'oracle_catalog', 'dbms_output', 'utl_file');
COMMENT ON VIEW oracle_catalog."dba_objects" IS 'Oracle DBA_OBJECTS over pg_class -- excludes this extension''s own objects, same reasoning as V$SESSION excluding its own backend.';

CREATE VIEW oracle_catalog."dba_views" AS
SELECT schemaname AS owner, viewname AS view_name, definition AS text
FROM pg_catalog.pg_views;
COMMENT ON VIEW oracle_catalog."dba_views" IS 'Oracle DBA_VIEWS over pg_views.';

CREATE VIEW oracle_catalog."dba_constraints" AS
SELECT n.nspname AS owner,
       c.conname AS constraint_name,
       t.relname AS table_name,
       CASE c.contype
         WHEN 'p' THEN 'P' WHEN 'f' THEN 'R' WHEN 'u' THEN 'U'
         WHEN 'c' THEN 'C' ELSE c.contype::text
       END AS constraint_type
FROM pg_catalog.pg_constraint c
JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace;
COMMENT ON VIEW oracle_catalog."dba_constraints" IS 'Oracle DBA_CONSTRAINTS over pg_constraint (P/R/U/C mapped to Oracle''s single-letter codes).';

CREATE VIEW oracle_catalog."dba_sequences" AS
SELECT schemaname AS sequence_owner, sequencename AS sequence_name,
       increment_by, min_value, max_value, last_value
FROM pg_catalog.pg_sequences;
COMMENT ON VIEW oracle_catalog."dba_sequences" IS 'Oracle DBA_SEQUENCES over pg_sequences.';

CREATE VIEW oracle_catalog."dba_users" AS
SELECT usename AS username, usesysid AS user_id, valuntil AS expiry_date,
       CASE WHEN usesuper THEN 'OPEN' ELSE 'OPEN' END AS account_status
FROM pg_catalog.pg_user;
COMMENT ON VIEW oracle_catalog."dba_users" IS 'Oracle DBA_USERS over pg_user.';

-- ================================================================
-- Phase 2 (first package): DBMS_OUTPUT
-- C implementation in src/dbms_output.c -- see that file's header for why
-- this one package needed C instead of plpgsql.
-- ================================================================

CREATE SCHEMA dbms_output;
COMMENT ON SCHEMA dbms_output IS 'Oracle DBMS_OUTPUT package (pg_oracle, part of Polygres).';

CREATE FUNCTION dbms_output.enable(buffer_size int DEFAULT 20000) RETURNS void
  AS 'MODULE_PATHNAME', 'dbms_output_enable' LANGUAGE C VOLATILE;
CREATE FUNCTION dbms_output.disable() RETURNS void
  AS 'MODULE_PATHNAME', 'dbms_output_disable' LANGUAGE C VOLATILE;
CREATE FUNCTION dbms_output.put_line(text) RETURNS void
  AS 'MODULE_PATHNAME', 'dbms_output_put_line' LANGUAGE C VOLATILE;
CREATE FUNCTION dbms_output.put(text) RETURNS void
  AS 'MODULE_PATHNAME', 'dbms_output_put' LANGUAGE C VOLATILE;
CREATE FUNCTION dbms_output.new_line() RETURNS void
  AS 'MODULE_PATHNAME', 'dbms_output_new_line' LANGUAGE C VOLATILE;
CREATE FUNCTION dbms_output.get_line(OUT line text, OUT status int) RETURNS record
  AS 'MODULE_PATHNAME', 'dbms_output_get_line' LANGUAGE C VOLATILE;

-- ================================================================
-- db_emulation support function -- lets SQL/plpgsql code (and Polywire's
-- own admin checks) ask "is Oracle emulation active in this session"
-- without parsing current_setting('db_emulation') itself.
-- ================================================================

CREATE FUNCTION oracle_catalog.emulation_active() RETURNS boolean
  AS 'MODULE_PATHNAME', 'pg_oracle_emulation_active' LANGUAGE C STABLE;
COMMENT ON FUNCTION oracle_catalog.emulation_active() IS 'True when this session has SET db_emulation = ''oracle''.';
