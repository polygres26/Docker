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
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'oracle_catalog',
                         'dbms_output', 'dbms_random', 'dbms_utility', 'dbms_assert', 'utl_file');
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

-- ================================================================
-- Phase 2 (packages 2-4 of the top-20): DBMS_RANDOM, DBMS_UTILITY,
-- DBMS_ASSERT -- all pure computation over Postgres's own random()/
-- catalog, no session state needed, so plain SQL/plpgsql, not C (compare
-- DBMS_OUTPUT above, which needed C specifically for its buffer).
-- ================================================================

CREATE SCHEMA dbms_random;
COMMENT ON SCHEMA dbms_random IS 'Oracle DBMS_RANDOM package (pg_oracle, part of Polygres).';

CREATE FUNCTION dbms_random.value() RETURNS double precision
  AS $$ SELECT random(); $$ LANGUAGE sql VOLATILE;
CREATE FUNCTION dbms_random.value(low double precision, high double precision) RETURNS double precision
  AS $$ SELECT low + random() * (high - low); $$ LANGUAGE sql VOLATILE;
COMMENT ON FUNCTION dbms_random.value() IS 'Oracle DBMS_RANDOM.VALUE -- random number in [0,1).';

-- Oracle's RANDOM() returns a binary_integer across the full signed
-- 32-bit range, not [0,1) -- kept as its own overload rather than folded
-- into value() so a caller who wrote RANDOM() explicitly still gets an
-- integer back, matching what real Oracle code expects from that name.
CREATE FUNCTION dbms_random.random() RETURNS integer
  AS $$ SELECT (random() * 4294967295.0 - 2147483648.0)::integer; $$ LANGUAGE sql VOLATILE;

-- setseed() wants a double in [-1,1]; val::int's own range (-2^31..2^31-1)
-- divided by 2147483647 already lands almost exactly there -- clamped at
-- the low end because -2147483648 / 2147483647 is *barely* < -1. (First
-- version of this function computed val % 2147483647 + 2147483647 in
-- int4 arithmetic instead, which overflows int4 for ordinary seed values
-- like 42 -- found live via "ERROR: integer out of range" on the very
-- first smoke-test call, not by inspection.)
CREATE FUNCTION dbms_random.seed(val integer) RETURNS void
  AS $$ SELECT setseed(GREATEST(-1.0, LEAST(1.0, val::double precision / 2147483647.0))); $$ LANGUAGE sql VOLATILE;
CREATE FUNCTION dbms_random.seed(val text) RETURNS void
  AS $$ SELECT setseed(GREATEST(-1.0, LEAST(1.0, (('x' || md5(val))::bit(32)::int)::double precision / 2147483647.0))); $$ LANGUAGE sql VOLATILE;
COMMENT ON FUNCTION dbms_random.seed(integer) IS 'Oracle DBMS_RANDOM.SEED -- re-seeds via Postgres''s own setseed(), same determinism guarantee: same seed, same subsequent sequence.';

-- STRING(opt, len): opt is Oracle's single-letter charset selector --
-- U/u uppercase, L/l lowercase, A/a alpha (mixed case), X/x alphanumeric
-- uppercase (Oracle's default for any option it doesn't recognize), P/p
-- any printable ASCII character.
CREATE FUNCTION dbms_random.string(opt text, len int) RETURNS text
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
  chars  text;
  result text := '';
BEGIN
  IF len IS NULL OR len < 0 THEN
    RAISE EXCEPTION 'ORA-20000: DBMS_RANDOM.STRING: length must be a non-negative integer';
  END IF;
  chars := CASE upper(coalesce(opt, 'X'))
    WHEN 'U' THEN 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    WHEN 'L' THEN 'abcdefghijklmnopqrstuvwxyz'
    WHEN 'A' THEN 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
    WHEN 'P' THEN '!"#$%&''()*+,-./:;<=>?@[\]^_`{|}~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
    ELSE '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'	-- 'X' and any unrecognized option, matching Oracle's own fallback
  END;
  FOR i IN 1..len LOOP
    result := result || substr(chars, 1 + floor(random() * length(chars))::int, 1);
  END LOOP;
  RETURN result;
END;
$$;
COMMENT ON FUNCTION dbms_random.string(text, int) IS 'Oracle DBMS_RANDOM.STRING.';

CREATE SCHEMA dbms_utility;
COMMENT ON SCHEMA dbms_utility IS 'Oracle DBMS_UTILITY package (pg_oracle, part of Polygres) -- partial: see README.md for what''s not here yet and why.';

-- Oracle's GET_TIME returns hundredths of a second since an arbitrary
-- epoch -- callers only ever diff two calls, never read it as a real
-- timestamp, so clock_timestamp()-based hundredths-since-epoch satisfies
-- every real usage pattern (elapsed := (t2 - t1) / 100 for seconds).
CREATE FUNCTION dbms_utility.get_time() RETURNS bigint
  AS $$ SELECT (extract(epoch FROM clock_timestamp()) * 100)::bigint; $$ LANGUAGE sql VOLATILE;
COMMENT ON FUNCTION dbms_utility.get_time() IS 'Oracle DBMS_UTILITY.GET_TIME -- hundredths of a second, for elapsed-time diffs only (not a real timestamp).';

CREATE FUNCTION dbms_utility.db_version(OUT version text, OUT compatibility text) RETURNS record
LANGUAGE plpgsql STABLE AS $$
BEGIN
  version := current_setting('server_version');
  compatibility := current_setting('server_version_num');
END;
$$;
COMMENT ON FUNCTION dbms_utility.db_version() IS 'Oracle DBMS_UTILITY.DB_VERSION -- reports the real Postgres version, not an Oracle version string.';

-- FORMAT_ERROR_STACK/FORMAT_CALL_STACK are deliberately NOT here: Oracle
-- callers invoke them from inside an exception handler and they reach
-- into that handler's own error context implicitly. There's no
-- zero-argument way to fake that faithfully from a plain SQL/plpgsql
-- function -- a caller's own `EXCEPTION WHEN OTHERS THEN ... SQLERRM ...`
-- (plpgsql's native equivalent) is the honest answer today. Tracked as
-- future work alongside anonymous-block support generally, since real
-- use of this pair is almost always inside one.

CREATE SCHEMA dbms_assert;
COMMENT ON SCHEMA dbms_assert IS 'Oracle DBMS_ASSERT package (pg_oracle, part of Polygres) -- SQL-injection-defense helpers for dynamic SQL.';

CREATE FUNCTION dbms_assert.simple_sql_name(str text) RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF str IS NULL OR str !~ '^[A-Za-z][A-Za-z0-9_$#]*$' THEN
    RAISE EXCEPTION 'ORA-44003: invalid SQL name';
  END IF;
  RETURN str;
END;
$$;
COMMENT ON FUNCTION dbms_assert.simple_sql_name(text) IS 'Oracle DBMS_ASSERT.SIMPLE_SQL_NAME.';

CREATE FUNCTION dbms_assert.qualified_sql_name(str text) RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  IF str IS NULL OR str !~ '^[A-Za-z][A-Za-z0-9_$#]*(\.[A-Za-z][A-Za-z0-9_$#]*)*$' THEN
    RAISE EXCEPTION 'ORA-44001: invalid schema name';
  END IF;
  RETURN str;
END;
$$;
COMMENT ON FUNCTION dbms_assert.qualified_sql_name(text) IS 'Oracle DBMS_ASSERT.QUALIFIED_SQL_NAME.';

CREATE FUNCTION dbms_assert.schema_name(str text) RETURNS text
LANGUAGE plpgsql STABLE AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = str) THEN
    RAISE EXCEPTION 'ORA-44001: invalid schema name';
  END IF;
  RETURN str;
END;
$$;
COMMENT ON FUNCTION dbms_assert.schema_name(text) IS 'Oracle DBMS_ASSERT.SCHEMA_NAME.';

CREATE FUNCTION dbms_assert.enquote_name(str text, capitalize boolean DEFAULT true) RETURNS text
  AS $$ SELECT quote_ident(CASE WHEN capitalize THEN upper(str) ELSE str END); $$ LANGUAGE sql IMMUTABLE;
COMMENT ON FUNCTION dbms_assert.enquote_name(text, boolean) IS 'Oracle DBMS_ASSERT.ENQUOTE_NAME.';

CREATE FUNCTION dbms_assert.enquote_literal(str text) RETURNS text
  AS $$ SELECT quote_literal(str); $$ LANGUAGE sql IMMUTABLE;
COMMENT ON FUNCTION dbms_assert.enquote_literal(text) IS 'Oracle DBMS_ASSERT.ENQUOTE_LITERAL.';

CREATE FUNCTION dbms_assert.noop(str text) RETURNS text
  AS $$ SELECT str; $$ LANGUAGE sql IMMUTABLE;
COMMENT ON FUNCTION dbms_assert.noop(text) IS 'Oracle DBMS_ASSERT.NOOP -- passthrough, exists for source compatibility only.';

-- ================================================================
-- Phase 2 (package 5 of the top-20): UTL_FILE
-- C implementation in src/utl_file.c -- see that file's header for why
-- (session-lifetime file handles, and the security model: gated on
-- Postgres's own pg_read_server_files/pg_write_server_files predefined
-- roles rather than a second, parallel privilege system).
--
-- UTL_FILE.FILE_TYPE is Oracle's own opaque record; here a handle is
-- just an integer (returned by fopen(), passed to every other call) --
-- documented simplification, not a hidden one.
-- ================================================================

CREATE SCHEMA utl_file;
COMMENT ON SCHEMA utl_file IS 'Oracle UTL_FILE package (pg_oracle, part of Polygres). See create_directory() for the privilege model.';

CREATE TABLE utl_file.directories(
  directory_name text PRIMARY KEY,
  path           text NOT NULL
);
COMMENT ON TABLE utl_file.directories IS 'Maps an Oracle-style directory-object name to a real filesystem path. No privilege lives in this table -- see create_directory().';

-- Parameters prefixed p_ deliberately: an unprefixed `directory_name`
-- parameter here shadowed the column of the same name and made the
-- ON CONFLICT clause below ambiguous ("could refer to either a PL/pgSQL
-- variable or a table column") -- found live on the very first real
-- create_directory() call, not by inspection.
CREATE FUNCTION utl_file.create_directory(p_directory_name text, p_path text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  IF NOT (pg_has_role(current_user, 'pg_write_server_files', 'MEMBER')
          OR pg_has_role(current_user, 'pg_read_server_files', 'MEMBER'))
  THEN
    RAISE EXCEPTION 'ORA-01031: insufficient privileges'
      USING DETAIL = format('Creating a UTL_FILE directory object requires membership in '
                             'pg_read_server_files or pg_write_server_files (or superuser) -- '
                             'the same privilege Postgres itself uses for filesystem access, '
                             'not a separate grant this extension invented.');
  END IF;
  INSERT INTO utl_file.directories(directory_name, path) VALUES (p_directory_name, p_path)
    ON CONFLICT (directory_name) DO UPDATE SET path = EXCLUDED.path;
END;
$$;
COMMENT ON FUNCTION utl_file.create_directory(text, text) IS 'Oracle CREATE DIRECTORY equivalent -- see this function''s own privilege check, and utl_file.directories'' comment.';

CREATE FUNCTION utl_file.drop_directory(p_directory_name text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  IF NOT (pg_has_role(current_user, 'pg_write_server_files', 'MEMBER')
          OR pg_has_role(current_user, 'pg_read_server_files', 'MEMBER'))
  THEN
    RAISE EXCEPTION 'ORA-01031: insufficient privileges';
  END IF;
  DELETE FROM utl_file.directories WHERE directory_name = p_directory_name;
END;
$$;

CREATE FUNCTION utl_file.fopen(location text, filename text, open_mode text) RETURNS integer
  AS 'MODULE_PATHNAME', 'utl_file_fopen' LANGUAGE C VOLATILE;
CREATE FUNCTION utl_file.is_open(file_id integer) RETURNS boolean
  AS 'MODULE_PATHNAME', 'utl_file_is_open' LANGUAGE C VOLATILE;
CREATE FUNCTION utl_file.put(file_id integer, buffer text) RETURNS void
  AS 'MODULE_PATHNAME', 'utl_file_put' LANGUAGE C VOLATILE;
CREATE FUNCTION utl_file.put_line(file_id integer, buffer text) RETURNS void
  AS 'MODULE_PATHNAME', 'utl_file_put_line' LANGUAGE C VOLATILE;
CREATE FUNCTION utl_file.new_line(file_id integer, lines integer DEFAULT 1) RETURNS void
  AS 'MODULE_PATHNAME', 'utl_file_new_line' LANGUAGE C VOLATILE;
CREATE FUNCTION utl_file.get_line(file_id integer) RETURNS text
  AS 'MODULE_PATHNAME', 'utl_file_get_line' LANGUAGE C VOLATILE;
COMMENT ON FUNCTION utl_file.get_line(integer) IS 'Raises SQLSTATE P0002 (no_data_found) at end of file -- catch with EXCEPTION WHEN NO_DATA_FOUND, same as real Oracle GET_LINE.';
CREATE FUNCTION utl_file.fclose(file_id integer) RETURNS void
  AS 'MODULE_PATHNAME', 'utl_file_fclose' LANGUAGE C VOLATILE;
CREATE FUNCTION utl_file.fclose_all() RETURNS void
  AS 'MODULE_PATHNAME', 'utl_file_fclose_all' LANGUAGE C VOLATILE;

-- ================================================================
-- Grants -- without these, `SET db_emulation = 'oracle'` (available to
-- any role, PGC_USERSET) silently fails the moment that role queries
-- v$session or calls dbms_output.put_line, since a fresh schema in
-- Postgres grants USAGE/EXECUTE to nobody but its owner by default.
-- Found live: a non-superuser test role hit "permission denied for
-- schema oracle_catalog" on its very first query after SET succeeded.
-- Every object here is read-only (views) or side-effect-free within the
-- caller's own session (DBMS_OUTPUT's buffer is per-backend), so PUBLIC
-- is the right grantee -- there's no privilege being handed out that a
-- role couldn't already get some other way.
-- ================================================================

GRANT USAGE ON SCHEMA oracle_catalog TO PUBLIC;
GRANT USAGE ON SCHEMA dbms_output TO PUBLIC;
GRANT USAGE ON SCHEMA dbms_random TO PUBLIC;
GRANT USAGE ON SCHEMA dbms_utility TO PUBLIC;
GRANT USAGE ON SCHEMA dbms_assert TO PUBLIC;
GRANT USAGE ON SCHEMA utl_file TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA oracle_catalog TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA oracle_catalog TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_output TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_random TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_utility TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_assert TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA utl_file TO PUBLIC;
-- SELECT (not INSERT/UPDATE/DELETE) on the directories table: every role
-- needs to read it for FOPEN's own internal lookup to work at all, but
-- registering/changing a directory must go through create_directory()/
-- drop_directory() above, which enforce the real privilege check --
-- direct table writes would bypass that entirely.
GRANT SELECT ON utl_file.directories TO PUBLIC;
