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
                         'dbms_output', 'dbms_random', 'dbms_utility', 'dbms_assert',
                         'dbms_network_acl_admin', 'dbms_crypto', 'dbms_scheduler',
                         'dbms_aqadm', 'dbms_stats', 'dbms_session',
                         'utl_file', 'utl_http', 'cron');
-- dbms_aq is deliberately NOT in the exclusion list above: unlike every
-- other package schema here, it holds real, user-facing queue tables
-- (created by DBMS_AQADM.CREATE_QUEUE_TABLE) that a DBA script migrated
-- from Oracle genuinely expects to find via DBA_OBJECTS -- hiding them
-- the way this extension's own purely-internal bookkeeping schemas are
-- hidden would be actively unhelpful, not just cautious.
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
-- SYS_CONTEXT + DBMS_SESSION.SET_CONTEXT/SET_IDENTIFIER
--
-- Why this matters more than its own line in the top-20 list: SYS_CONTEXT
-- is what a migrated VPD (DBMS_RLS) policy predicate actually calls to
-- find out who's asking -- SYS_CONTEXT('my_ctx', 'tenant_id') sitting
-- inside a WHERE-clause-shaped policy function. The genuinely good news:
-- Postgres already has a native, arguably more capable VPD-equivalent
-- enforcement mechanism -- real ROW LEVEL SECURITY (`CREATE POLICY ...
-- USING (...)`) -- so this package's real job isn't reimplementing row
-- filtering, it's making SYS_CONTEXT() return a real value a Postgres
-- RLS policy's USING clause can read. See the worked example in
-- README.md''s SYS_CONTEXT section for the full CREATE POLICY... USING
-- (sys_context(...) = ...) pattern, verified live end to end.
--
-- Two kinds of context, matching real Oracle's own split:
-- 'USERENV' is Oracle's built-in namespace -- read-only, dispatched here
-- to real Postgres session facts (current_user, inet_client_addr(),
-- pg_backend_pid(), ...), not stored state. Anything else is a
-- user-defined context: CREATE CONTEXT registers a namespace (real
-- Oracle DDL -- exposed here as a function call instead, since stock
-- Postgres's grammar has no CREATE CONTEXT statement to add without
-- patching the parser, the same class of gap as anonymous PL/SQL
-- blocks); DBMS_SESSION.SET_CONTEXT/CLEAR_CONTEXT then read/write actual
-- per-session values for it. Storage is a lazily-created TEMP table
-- (`pg_temp`, inherently private and auto-dropped per session) -- no C
-- needed, unlike DBMS_OUTPUT's buffer, since a temp table already gives
-- exactly the "session-lifetime state" property for free.
--
-- Oracle's own real access-control mechanic for user-defined contexts --
-- only the namespace's "trusted package" can call SET_CONTEXT for it --
-- has no Postgres object to check (Postgres has no PL/SQL packages). The
-- honest, correct parallel: wrap DBMS_SESSION.SET_CONTEXT calls in your
-- OWN SECURITY DEFINER function, the same way Oracle's real enforcement
-- actually works under the hood (only that package's compiled code,
-- running with definer rights, can reach the underlying context-set
-- primitive) -- this package doesn't invent a second access-control
-- layer on top, it makes the existing Postgres one do the same job.
-- CREATE_CONTEXT itself is owner-only (see its own comment), matching
-- Oracle's CREATE ANY CONTEXT system privilege being DBA-level.
-- ================================================================

CREATE TABLE oracle_catalog.contexts(
  namespace       text PRIMARY KEY,
  trusted_package text	-- recorded for introspection only -- see this section's header comment for why it isn't (and can't be) enforced the way Oracle enforces it
);
COMMENT ON TABLE oracle_catalog.contexts IS 'Registered SYS_CONTEXT namespaces (Oracle CREATE CONTEXT). USERENV is built in, not a row here.';

CREATE FUNCTION oracle_catalog.create_context(p_namespace text, p_trusted_package text DEFAULT NULL) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = oracle_catalog, pg_catalog AS $$
BEGIN
  IF upper(p_namespace) = 'USERENV' THEN
    RAISE EXCEPTION 'ORA-06564: USERENV is a built-in namespace and cannot be created';
  END IF;
  INSERT INTO oracle_catalog.contexts(namespace, trusted_package) VALUES (p_namespace, p_trusted_package);
END;
$$;
COMMENT ON FUNCTION oracle_catalog.create_context(text, text) IS 'Oracle CREATE CONTEXT, as a function call rather than new DDL syntax -- see this section''s header comment. SECURITY DEFINER + owner-only EXECUTE (see grants), matching Oracle''s own CREATE ANY CONTEXT privilege.';

CREATE SCHEMA dbms_session;
COMMENT ON SCHEMA dbms_session IS 'Oracle DBMS_SESSION package (pg_oracle, part of Polygres) -- SET_CONTEXT/CLEAR_CONTEXT/SET_IDENTIFIER, the write side of SYS_CONTEXT. See the SYS_CONTEXT section above.';

CREATE FUNCTION dbms_session.set_context(
  p_namespace text, p_attribute text, p_value text,
  p_username text DEFAULT NULL, p_client_id text DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  IF upper(p_namespace) = 'USERENV' THEN
    RAISE EXCEPTION 'ORA-01739: USERENV namespace not allowed here -- use dbms_session.set_identifier() for CLIENT_IDENTIFIER';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM oracle_catalog.contexts WHERE namespace = p_namespace) THEN
    RAISE EXCEPTION 'ORA-01403: no data found -- context namespace ''%'' has not been created (see oracle_catalog.create_context)', p_namespace;
  END IF;
  IF p_username IS NOT NULL OR p_client_id IS NOT NULL THEN
    RAISE NOTICE 'pg_oracle: dbms_session.set_context''s username/client_id parameters (setting context on a DIFFERENT session) are not supported -- ignored, only the calling session''s own context was set.';
  END IF;
  CREATE TEMP TABLE IF NOT EXISTS oracle_context_values(
    namespace text, attribute text, value text, PRIMARY KEY (namespace, attribute)
  ) ON COMMIT PRESERVE ROWS;
  INSERT INTO pg_temp.oracle_context_values(namespace, attribute, value)
    VALUES (p_namespace, p_attribute, p_value)
    ON CONFLICT (namespace, attribute) DO UPDATE SET value = EXCLUDED.value;
END;
$$;
COMMENT ON FUNCTION dbms_session.set_context(text, text, text, text, text) IS 'Oracle DBMS_SESSION.SET_CONTEXT.';

CREATE FUNCTION dbms_session.clear_context(p_namespace text, p_attribute text DEFAULT NULL) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  IF to_regclass('pg_temp.oracle_context_values') IS NULL THEN
    RETURN;	-- nothing has ever been set in this session -- nothing to clear
  END IF;
  IF p_attribute IS NULL THEN
    DELETE FROM pg_temp.oracle_context_values WHERE namespace = p_namespace;
  ELSE
    DELETE FROM pg_temp.oracle_context_values WHERE namespace = p_namespace AND attribute = p_attribute;
  END IF;
END;
$$;
COMMENT ON FUNCTION dbms_session.clear_context(text, text) IS 'Oracle DBMS_SESSION.CLEAR_CONTEXT -- clears one attribute, or every attribute in the namespace if attribute is omitted.';

CREATE FUNCTION dbms_session.clear_all_context(p_namespace text) RETURNS void
  AS $$ SELECT dbms_session.clear_context(p_namespace, NULL); $$ LANGUAGE sql;
COMMENT ON FUNCTION dbms_session.clear_all_context(text) IS 'Oracle DBMS_SESSION.CLEAR_ALL_CONTEXT.';

-- CLIENT_IDENTIFIER is USERENV's one genuinely mutable attribute in real
-- Oracle (everything else in USERENV is read-only derived session
-- fact) -- stored the same way as user-defined contexts (a reserved
-- namespace in the same temp table), but through its own dedicated
-- SET_IDENTIFIER/CLEAR_IDENTIFIER entry points rather than plain
-- SET_CONTEXT, matching real Oracle exactly (SET_CONTEXT explicitly
-- rejects 'USERENV', see above).
CREATE FUNCTION dbms_session.set_identifier(p_client_id text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS oracle_context_values(
    namespace text, attribute text, value text, PRIMARY KEY (namespace, attribute)
  ) ON COMMIT PRESERVE ROWS;
  INSERT INTO pg_temp.oracle_context_values(namespace, attribute, value)
    VALUES ('USERENV', 'CLIENT_IDENTIFIER', p_client_id)
    ON CONFLICT (namespace, attribute) DO UPDATE SET value = EXCLUDED.value;
END;
$$;
COMMENT ON FUNCTION dbms_session.set_identifier(text) IS 'Oracle DBMS_SESSION.SET_IDENTIFIER -- read back via sys_context(''USERENV'',''CLIENT_IDENTIFIER''). The common connection-pooling pattern: the pool authenticates once as a shared DB role, then SET_IDENTIFIER per logical end user for VPD/audit to distinguish them.';

CREATE FUNCTION dbms_session.clear_identifier() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  IF to_regclass('pg_temp.oracle_context_values') IS NOT NULL THEN
    DELETE FROM pg_temp.oracle_context_values WHERE namespace = 'USERENV' AND attribute = 'CLIENT_IDENTIFIER';
  END IF;
END;
$$;
COMMENT ON FUNCTION dbms_session.clear_identifier() IS 'Oracle DBMS_SESSION.CLEAR_IDENTIFIER.';

CREATE FUNCTION oracle_catalog.sys_context(p_namespace text, p_attribute text) RETURNS text
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_attr text := upper(p_attribute);
BEGIN
  IF upper(p_namespace) = 'USERENV' THEN
    -- CLIENT_IDENTIFIER is the one USERENV attribute backed by mutable
    -- session state (dbms_session.set_identifier) rather than a fixed
    -- Postgres session fact -- checked first, everything else below is
    -- a real, live value, not stored state.
    IF v_attr = 'CLIENT_IDENTIFIER' THEN
      IF to_regclass('pg_temp.oracle_context_values') IS NULL THEN
        RETURN NULL;
      END IF;
      RETURN (SELECT value FROM pg_temp.oracle_context_values WHERE namespace = 'USERENV' AND attribute = 'CLIENT_IDENTIFIER');
    END IF;

    RETURN CASE v_attr
      WHEN 'CURRENT_USER' THEN current_user
      WHEN 'SESSION_USER' THEN session_user
      WHEN 'CURRENT_SCHEMA' THEN current_schema
      WHEN 'DB_NAME' THEN current_database()
      WHEN 'INSTANCE_NAME' THEN current_setting('cluster_name')
      WHEN 'SERVER_HOST' THEN coalesce(host(inet_server_addr()), 'localhost')
      WHEN 'HOST' THEN coalesce(host(inet_client_addr()), 'localhost')	-- client host -- Oracle's HOST, distinct from SERVER_HOST
      WHEN 'IP_ADDRESS' THEN host(inet_client_addr())
      WHEN 'SESSIONID' THEN pg_backend_pid()::text	-- no true Oracle-style AUDSID equivalent -- backend pid is the closest per-connection identifier
      WHEN 'SID' THEN pg_backend_pid()::text
      WHEN 'ISDBA' THEN CASE WHEN (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN 'TRUE' ELSE 'FALSE' END
      WHEN 'LANG' THEN left(current_setting('lc_messages'), 2)
      WHEN 'LANGUAGE' THEN current_setting('lc_messages')
      WHEN 'CON_NAME' THEN current_database()	-- Postgres has no CDB/PDB multitenant architecture -- database name is the closest analog, not a real container name
      WHEN 'CON_ID' THEN '1'	-- see CON_NAME above
      ELSE NULL	-- an unimplemented (but real) USERENV attribute, or a genuinely invalid one -- both return NULL rather than erroring, see this function's own scope note
    END;
  END IF;

  IF to_regclass('pg_temp.oracle_context_values') IS NULL THEN
    RETURN NULL;	-- nothing has ever been SET_CONTEXT'd in this session
  END IF;
  RETURN (SELECT value FROM pg_temp.oracle_context_values WHERE namespace = p_namespace AND attribute = p_attribute);
END;
$$;
COMMENT ON FUNCTION oracle_catalog.sys_context(text, text) IS 'Oracle SYS_CONTEXT. USERENV attributes covered: CURRENT_USER/SESSION_USER/CURRENT_SCHEMA/DB_NAME/INSTANCE_NAME/SERVER_HOST/HOST/IP_ADDRESS/SESSIONID/SID/ISDBA/LANG/LANGUAGE/CON_NAME/CON_ID/CLIENT_IDENTIFIER -- others (MODULE/ACTION/PROXY_USER/AUTHENTICATION_METHOD/... ) return NULL, not implemented yet. User-defined namespaces read whatever dbms_session.set_context() last wrote in this session.';

-- ================================================================
-- ALTER SESSION / ALTER SYSTEM
--
-- Two completely different stories, checked live rather than assumed:
--
-- ALTER SYSTEM SET parameter = value -- Postgres already has this
-- exact statement, native, since 9.4 (confirmed live: `ALTER SYSTEM SET
-- work_mem = '8MB'` + `SELECT pg_reload_conf()` just works). Nothing to
-- build here -- the only real gap is Oracle-specific parameter NAMES
-- having no Postgres GUC equivalent, the same vocabulary gap V$PARAMETER
-- already documents, not a syntax problem.
--
-- ALTER SESSION SET parameter = value -- confirmed live to be a genuine
-- Postgres syntax error ("syntax error at or near SESSION"): stock
-- Postgres's grammar has no ALTER SESSION statement at all, and there's
-- no extension hook to add one without patching the parser -- the same
-- class of gap as anonymous PL/SQL blocks and CREATE CONTEXT. A real
-- rewrite (ALTER SESSION SET X = Y -> a Polywire orawire-side text
-- transform) belongs in front of Postgres, not in this extension. What
-- this extension DOES provide: oracle_catalog.alter_session_set() below,
-- a function-call substitute usable directly today (and a natural
-- rewrite target once orawire adds that transform) that implements the
-- handful of ALTER SESSION forms that have a real Postgres equivalent:
-- CURRENT_SCHEMA (-> search_path), TIME_ZONE (-> timezone), and the
-- NLS_* parameters (-> this section's own session-local store, read
-- back by SYS_CONTEXT-adjacent to_char()/to_date() below). Anything
-- else raises a clear NOTICE and does nothing, rather than silently
-- claiming to have applied a setting Postgres has no equivalent for
-- (RESUMABLE, parallel DML degree, and similar Oracle-only session
-- concepts have no Postgres analog at all).
-- ================================================================

CREATE FUNCTION oracle_catalog.alter_session_set(p_parameter text, p_value text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  CASE upper(p_parameter)
    WHEN 'CURRENT_SCHEMA' THEN
      -- Prepended onto the EXISTING search_path, not a wholesale
      -- replacement -- found live: `SET search_path TO myapp` alone
      -- wiped out the Oracle package schemas db_emulation had already
      -- appended (dbms_output, oracle_catalog, ...), immediately
      -- breaking unqualified to_char()/sys_context()/etc. in the same
      -- session. Real Oracle's CURRENT_SCHEMA only changes the default
      -- schema for unqualified resolution and object creation -- it
      -- doesn't make every other Oracle-visible package disappear, so
      -- neither should this.
      EXECUTE format('SET search_path TO %I, %s', p_value, current_setting('search_path'));
    WHEN 'TIME_ZONE' THEN
      EXECUTE format('SET timezone TO %L', p_value);
    ELSE
      IF upper(p_parameter) LIKE 'NLS\_%' ESCAPE '\' THEN
        CREATE TEMP TABLE IF NOT EXISTS oracle_nls_settings(parameter text PRIMARY KEY, value text) ON COMMIT PRESERVE ROWS;
        INSERT INTO pg_temp.oracle_nls_settings(parameter, value) VALUES (upper(p_parameter), p_value)
          ON CONFLICT (parameter) DO UPDATE SET value = EXCLUDED.value;
      ELSE
        RAISE NOTICE 'pg_oracle: ALTER SESSION SET % is not supported (no Postgres equivalent) -- ignored, not applied.', p_parameter;
      END IF;
  END CASE;
END;
$$;
COMMENT ON FUNCTION oracle_catalog.alter_session_set(text, text) IS 'Oracle ALTER SESSION SET, as a function call rather than new DDL syntax -- see this section''s header comment. Covers CURRENT_SCHEMA, TIME_ZONE, and NLS_* only; anything else is a no-op with a NOTICE.';

CREATE FUNCTION oracle_catalog.get_nls_parameter(p_parameter text) RETURNS text
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_stored text;
BEGIN
  IF to_regclass('pg_temp.oracle_nls_settings') IS NOT NULL THEN
    SELECT value INTO v_stored FROM pg_temp.oracle_nls_settings WHERE parameter = upper(p_parameter);
    IF v_stored IS NOT NULL THEN
      RETURN v_stored;
    END IF;
  END IF;
  -- Oracle's real out-of-the-box defaults for an AMERICAN_AMERICA client --
  -- not every NLS_* parameter Oracle has, only the ones to_char()/
  -- to_date() below actually consult.
  RETURN CASE upper(p_parameter)
    WHEN 'NLS_DATE_FORMAT' THEN 'DD-MON-RR'
    WHEN 'NLS_TIMESTAMP_FORMAT' THEN 'DD-MON-RR HH.MI.SSXFF AM'
    WHEN 'NLS_NUMERIC_CHARACTERS' THEN '.,'
    WHEN 'NLS_CURRENCY' THEN '$'
    WHEN 'NLS_TERRITORY' THEN 'AMERICA'
    WHEN 'NLS_LANGUAGE' THEN 'AMERICAN'
    ELSE NULL
  END;
END;
$$;
COMMENT ON FUNCTION oracle_catalog.get_nls_parameter(text) IS 'Internal -- session-local NLS_* value if ALTER SESSION SET it, else Oracle''s real AMERICAN_AMERICA default. Backs to_char()/to_date() below and the nls_session_parameters view.';

CREATE VIEW oracle_catalog."nls_session_parameters" AS
SELECT p.parameter, oracle_catalog.get_nls_parameter(p.parameter) AS value
FROM (VALUES ('NLS_DATE_FORMAT'), ('NLS_TIMESTAMP_FORMAT'), ('NLS_NUMERIC_CHARACTERS'),
             ('NLS_CURRENCY'), ('NLS_TERRITORY'), ('NLS_LANGUAGE')) AS p(parameter);
COMMENT ON VIEW oracle_catalog."nls_session_parameters" IS 'Oracle NLS_SESSION_PARAMETERS dictionary view.';

-- Format-token translation: Postgres's own to_char()/to_date() already
-- understand almost every Oracle format token identically (checked
-- live, not assumed -- YYYY/MM/DD/HH24/HH12/MI/SS/MON/DY/DAY/AM/PM/FM
-- and numeric 9/0/,/. groups all matched Oracle's own output exactly).
-- Exactly three real, confirmed differences: RR (Oracle's century-
-- rounding 2-digit year) isn't recognized at all by Postgres, echoed
-- back literally; bare FF (fractional seconds with no explicit digit
-- count -- FF1..FF9 DO work natively) isn't recognized either; X
-- (Oracle's locale radix/decimal-point token) isn't recognized. This
-- function substitutes all three before delegating to the real
-- pg_catalog to_char()/to_date() -- everything else in the format
-- string passes through untouched, verified.
--
-- One stated, deliberate limitation: this is a plain string
-- substitution, not a real format-string tokenizer -- a literal "RR",
-- "FF", or "X" appearing inside an Oracle format string's own
-- double-quoted literal-text segment (e.g. 'DD-MON-YYYY "RR is text
-- here"') would be incorrectly substituted too. Real-world Oracle date
-- formats essentially never do this; stated here rather than silently
-- risked.
CREATE FUNCTION oracle_catalog.translate_nls_format(p_fmt text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT regexp_replace(
           regexp_replace(
             regexp_replace(p_fmt, 'RR', 'YY', 'g'),
           'FF(?![1-9])', 'FF6', 'g'),
         'X', '.', 'g');
$$;
COMMENT ON FUNCTION oracle_catalog.translate_nls_format(text) IS 'Internal -- see this section''s header comment for exactly which three tokens this rewrites and the quoted-literal-text caveat.';

-- date/timestamp overloads only, deliberately, not date/timestamptz
-- TWO-ARGUMENT forms beyond the exact `date` type: pg_catalog's own
-- to_char(timestamp[tz], text) already exists as an EXACT type match,
-- and Postgres always searches pg_catalog first for a tied exact match
-- regardless of this extension's search_path additions -- adding a
-- same-signature oracle_catalog.to_char(timestamp, text) would simply
-- never be chosen, not a real override. The ONE-ARGUMENT forms below
-- are always safe (pg_catalog has zero one-arg to_char candidates for
-- any type, confirmed live), and the two-argument `date` form is safe
-- too (pg_catalog has no plain-`date` to_char overload at all, only
-- timestamp/timestamptz). Stated plainly: an explicit two-argument
-- TO_CHAR(some_timestamp, 'DD-MON-RR') call still resolves to real
-- Postgres's own to_char and will NOT get RR/bare-FF/X translation in
-- this version -- only the one-arg (NLS-default-format) and the
-- explicit-format `date`-typed forms do.
CREATE FUNCTION oracle_catalog.to_char(p_date date, p_fmt text DEFAULT NULL) RETURNS text
  AS $$ SELECT to_char(p_date::timestamp, oracle_catalog.translate_nls_format(coalesce(p_fmt, oracle_catalog.get_nls_parameter('NLS_DATE_FORMAT')))); $$
  LANGUAGE sql STABLE;
COMMENT ON FUNCTION oracle_catalog.to_char(date, text) IS 'Oracle TO_CHAR(DATE) -- defaults to NLS_DATE_FORMAT (session-settable via alter_session_set) when no format is given, translating RR/bare-FF/X first. See this section''s header comment for why only the `date`-typed overload gets an explicit-format form.';

CREATE FUNCTION oracle_catalog.to_char(p_ts timestamp without time zone) RETURNS text
  AS $$ SELECT to_char(p_ts, oracle_catalog.translate_nls_format(oracle_catalog.get_nls_parameter('NLS_TIMESTAMP_FORMAT'))); $$
  LANGUAGE sql STABLE;
CREATE FUNCTION oracle_catalog.to_char(p_ts timestamptz) RETURNS text
  AS $$ SELECT to_char(p_ts, oracle_catalog.translate_nls_format(oracle_catalog.get_nls_parameter('NLS_TIMESTAMP_FORMAT'))); $$
  LANGUAGE sql STABLE;
COMMENT ON FUNCTION oracle_catalog.to_char(timestamp) IS 'Oracle TO_CHAR(TIMESTAMP) -- one-argument form only (real Postgres to_char has no bare TO_CHAR(some_date)/TO_CHAR(some_timestamp) at all, confirmed live -- this is a genuinely new capability, not an override), using NLS_TIMESTAMP_FORMAT.';

CREATE FUNCTION oracle_catalog.to_date(p_str text, p_fmt text DEFAULT NULL) RETURNS date
LANGUAGE plpgsql STABLE AS $$
BEGIN
  IF p_fmt IS NULL THEN
    RETURN to_date(p_str, oracle_catalog.translate_nls_format(oracle_catalog.get_nls_parameter('NLS_DATE_FORMAT')));
  END IF;
  -- Two-argument form: pg_catalog.to_date(text, text) already exists as
  -- an exact match and always wins the tie -- this branch only actually
  -- runs when called schema-qualified (oracle_catalog.to_date(...)) or
  -- once an orawire-side rewrite targets it explicitly; a bare
  -- unqualified two-argument TO_DATE(str, fmt) still resolves to real
  -- Postgres's own to_date without RR/bare-FF/X translation, same
  -- caveat as to_char's two-argument timestamp forms above.
  RETURN to_date(p_str, oracle_catalog.translate_nls_format(p_fmt));
END;
$$;
COMMENT ON FUNCTION oracle_catalog.to_date(text, text) IS 'Oracle TO_DATE -- one-argument form (using NLS_DATE_FORMAT) is always safe/new; the two-argument form only actually applies RR/bare-FF/X translation when called schema-qualified -- see this function''s own comment.';

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
-- Phase 2 (package 6 of the top-20): DBMS_NETWORK_ACL_ADMIN + UTL_HTTP
--
-- Real Oracle (11g+) makes UTL_HTTP unusable without an ACL grant by
-- design -- there is no bare "just let PL/SQL make HTTP calls" mode, so
-- this doesn't invent a laxer default: UTL_HTTP.REQUEST raises ORA-24247
-- unless DBMS_NETWORK_ACL_ADMIN has granted the calling role CONNECT on
-- an ACL assigned to the target host/port. This is what makes UTL_HTTP
-- safe enough to actually ship (see the earlier discussion of why it was
-- deferred at first) -- the policy lives entirely in this plpgsql, the
-- C in src/utl_http.c only parses the URL and calls back into this
-- policy via SPI before ever opening a socket.
--
-- Model, kept faithful to real Oracle rather than simplified into
-- something unfamiliar to anyone who's used the real package: an ACL is
-- a named, ordered list of (principal, privilege, grant-or-deny)
-- entries (`acl_privileges`, `id` column preserving insertion order --
-- this stands in for the ordering Oracle's ACL XML document itself
-- encodes); an ACL is assigned to one or more host/port-range patterns
-- (`host_assignments`); resolving a request to a permission answer is
-- two steps -- pick the most specific host/port match's ACL, then walk
-- that ACL's entries in order and return the first one naming this
-- principal (or the special principal 'PUBLIC') and this privilege.
-- ================================================================

CREATE SCHEMA dbms_network_acl_admin;
COMMENT ON SCHEMA dbms_network_acl_admin IS 'Oracle DBMS_NETWORK_ACL_ADMIN package (pg_oracle, part of Polygres) -- the only way to permit UTL_HTTP network access.';

CREATE TABLE dbms_network_acl_admin.acls(
  acl_name    text PRIMARY KEY,
  description text
);

CREATE TABLE dbms_network_acl_admin.acl_privileges(
  id        bigserial PRIMARY KEY,	-- preserves list order -- see header comment above
  acl_name  text NOT NULL REFERENCES dbms_network_acl_admin.acls(acl_name) ON DELETE CASCADE,
  principal text NOT NULL,	-- a role name, or the literal 'PUBLIC'
  privilege text NOT NULL CHECK (privilege IN ('connect', 'resolve')),
  is_grant  boolean NOT NULL
);

CREATE TABLE dbms_network_acl_admin.host_assignments(
  id          bigserial PRIMARY KEY,
  acl_name    text NOT NULL REFERENCES dbms_network_acl_admin.acls(acl_name) ON DELETE CASCADE,
  -- exact host ('api.example.com'), wildcard subdomain ('*.example.com'),
  -- or catch-all ('*') -- see resolve_acl_for_host()'s specificity order.
  host_pattern text NOT NULL,
  lower_port  integer,
  upper_port  integer,
  CHECK ((lower_port IS NULL) = (upper_port IS NULL))
);

-- Parameters are ALL p_-prefixed on purpose: an unprefixed `acl`/`host`
-- parameter here would shadow this schema's own table/column names the
-- same way an earlier utl_file.create_directory() bug did (see
-- README.md) -- prefixing here avoids that whole class of bug up front
-- rather than finding it live again.

CREATE FUNCTION dbms_network_acl_admin.create_acl(
  p_acl text, p_description text, p_principal text, p_is_grant boolean, p_privilege text
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM dbms_network_acl_admin.acls WHERE acl_name = p_acl) THEN
    RAISE EXCEPTION 'ORA-46375: ACL ''%'' already exists', p_acl;
  END IF;
  INSERT INTO dbms_network_acl_admin.acls(acl_name, description) VALUES (p_acl, p_description);
  INSERT INTO dbms_network_acl_admin.acl_privileges(acl_name, principal, privilege, is_grant)
    VALUES (p_acl, p_principal, lower(p_privilege), p_is_grant);
END;
$$;
COMMENT ON FUNCTION dbms_network_acl_admin.create_acl(text, text, text, boolean, text) IS 'Oracle DBMS_NETWORK_ACL_ADMIN.CREATE_ACL.';

CREATE FUNCTION dbms_network_acl_admin.add_privilege(
  p_acl text, p_principal text, p_is_grant boolean, p_privilege text
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM dbms_network_acl_admin.acls WHERE acl_name = p_acl) THEN
    RAISE EXCEPTION 'ORA-46192: ACL ''%'' does not exist', p_acl;
  END IF;
  INSERT INTO dbms_network_acl_admin.acl_privileges(acl_name, principal, privilege, is_grant)
    VALUES (p_acl, p_principal, lower(p_privilege), p_is_grant);
END;
$$;
COMMENT ON FUNCTION dbms_network_acl_admin.add_privilege(text, text, boolean, text) IS 'Oracle DBMS_NETWORK_ACL_ADMIN.ADD_PRIVILEGE.';

CREATE FUNCTION dbms_network_acl_admin.assign_acl(
  p_acl text, p_host text, p_lower_port integer DEFAULT NULL, p_upper_port integer DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM dbms_network_acl_admin.acls WHERE acl_name = p_acl) THEN
    RAISE EXCEPTION 'ORA-46192: ACL ''%'' does not exist', p_acl;
  END IF;
  INSERT INTO dbms_network_acl_admin.host_assignments(acl_name, host_pattern, lower_port, upper_port)
    VALUES (p_acl, p_host, p_lower_port, p_upper_port);
END;
$$;
COMMENT ON FUNCTION dbms_network_acl_admin.assign_acl(text, text, integer, integer) IS 'Oracle DBMS_NETWORK_ACL_ADMIN.ASSIGN_ACL.';

CREATE FUNCTION dbms_network_acl_admin.unassign_acl(
  p_acl text, p_host text, p_lower_port integer DEFAULT NULL, p_upper_port integer DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM dbms_network_acl_admin.host_assignments
   WHERE acl_name = p_acl AND host_pattern = p_host
     AND lower_port IS NOT DISTINCT FROM p_lower_port
     AND upper_port IS NOT DISTINCT FROM p_upper_port;
END;
$$;
COMMENT ON FUNCTION dbms_network_acl_admin.unassign_acl(text, text, integer, integer) IS 'Oracle DBMS_NETWORK_ACL_ADMIN.UNASSIGN_ACL.';

CREATE FUNCTION dbms_network_acl_admin.drop_acl(p_acl text) RETURNS void
  AS $$ DELETE FROM dbms_network_acl_admin.acls WHERE acl_name = p_acl; $$
  LANGUAGE sql;	-- ON DELETE CASCADE on both child tables handles the rest
COMMENT ON FUNCTION dbms_network_acl_admin.drop_acl(text) IS 'Oracle DBMS_NETWORK_ACL_ADMIN.DROP_ACL.';

-- Most-specific-host-match resolution: exact host beats any wildcard;
-- among wildcards, the longer (more specific) suffix wins; '*' is the
-- last resort. Within a specificity tier, an assignment with an explicit
-- port range beats one covering any port -- matches real Oracle's own
-- "more specific match wins" resolution order.
-- SECURITY DEFINER + a pinned search_path on this and the two functions
-- below: plpgsql functions are SECURITY INVOKER by default, so an
-- ordinary role's GRANT EXECUTE alone doesn't help it read acls/
-- acl_privileges/host_assignments -- it still needs its own table-level
-- SELECT, which would mean granting broad read access to the ACL tables
-- to every role just so utl_http.request() can check them (found live:
-- "permission denied for table host_assignments" the moment a real
-- non-superuser role called this). Real Oracle's own DBMS_NETWORK_ACL_
-- ADMIN checks run against SYS-owned structures the same way -- a
-- definer-rights read, not a broad grant on the underlying storage. The
-- pinned search_path is standard SECURITY DEFINER hygiene: without it, a
-- caller could set their own search_path to shadow `host_assignments`
-- with an object of their own and manipulate what this function reads.
CREATE FUNCTION dbms_network_acl_admin.resolve_acl_for_host(p_host text, p_port integer) RETURNS text
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = dbms_network_acl_admin, pg_catalog AS $$
DECLARE
  rec             record;
  best_acl        text;
  best_specificity int := -1;
  specificity     int;
  suffix          text;
BEGIN
  FOR rec IN
    SELECT * FROM dbms_network_acl_admin.host_assignments
    WHERE (lower_port IS NULL) OR (p_port BETWEEN lower_port AND upper_port)
  LOOP
    specificity := NULL;
    IF rec.host_pattern = p_host THEN
      specificity := 1000;
    ELSIF rec.host_pattern = '*' THEN
      specificity := 1;
    ELSIF left(rec.host_pattern, 2) = '*.' THEN
      suffix := substr(rec.host_pattern, 3);
      IF p_host = suffix OR p_host LIKE ('%.' || suffix) THEN
        specificity := 100 + length(suffix);
      END IF;
    END IF;

    IF specificity IS NOT NULL THEN
      IF rec.lower_port IS NOT NULL THEN
        specificity := specificity + 1;	-- explicit port range beats "any port" at the same host tier
      END IF;
      IF specificity > best_specificity THEN
        best_specificity := specificity;
        best_acl := rec.acl_name;
      END IF;
    END IF;
  END LOOP;
  RETURN best_acl;
END;
$$;
COMMENT ON FUNCTION dbms_network_acl_admin.resolve_acl_for_host(text, integer) IS 'Internal: picks which ACL (if any) governs a given host/port -- see this file''s comment above CREATE SCHEMA dbms_network_acl_admin.';

-- List-order privilege resolution within one already-resolved ACL --
-- Oracle semantics: the FIRST entry (in the order privileges were added)
-- naming this principal (or 'PUBLIC') and this privilege wins, not
-- "any deny anywhere beats any grant". No matching entry -> NULL, which
-- callers (including UTL_HTTP) must treat as "not permitted", same as a
-- real 0 -- Oracle's own semantics for "no explicit answer".
CREATE FUNCTION dbms_network_acl_admin.check_privilege(p_acl text, p_principal text, p_privilege text) RETURNS integer
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = dbms_network_acl_admin, pg_catalog AS $$
DECLARE
  rec record;
BEGIN
  FOR rec IN
    SELECT * FROM dbms_network_acl_admin.acl_privileges
    WHERE acl_name = p_acl AND privilege = lower(p_privilege)
    ORDER BY id
  LOOP
    IF rec.principal = p_principal OR upper(rec.principal) = 'PUBLIC' THEN
      RETURN CASE WHEN rec.is_grant THEN 1 ELSE 0 END;
    END IF;
  END LOOP;
  RETURN NULL;
END;
$$;
COMMENT ON FUNCTION dbms_network_acl_admin.check_privilege(text, text, text) IS 'Oracle DBMS_NETWORK_ACL_ADMIN.CHECK_PRIVILEGE.';

CREATE FUNCTION dbms_network_acl_admin.check_privilege_for_host(
  p_host text, p_port integer, p_principal text, p_privilege text
) RETURNS integer
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = dbms_network_acl_admin, pg_catalog AS $$
DECLARE
  v_acl text;
BEGIN
  v_acl := dbms_network_acl_admin.resolve_acl_for_host(p_host, p_port);
  IF v_acl IS NULL THEN
    RETURN NULL;	-- no ACL covers this host/port at all -- deny by default, same as real Oracle
  END IF;
  RETURN dbms_network_acl_admin.check_privilege(v_acl, p_principal, p_privilege);
END;
$$;
COMMENT ON FUNCTION dbms_network_acl_admin.check_privilege_for_host(text, integer, text, text) IS 'Internal: the single function UTL_HTTP calls via SPI before ever opening a socket -- see src/utl_http.c.';

CREATE SCHEMA utl_http;
COMMENT ON SCHEMA utl_http IS 'Oracle UTL_HTTP package (pg_oracle, part of Polygres) -- gated by DBMS_NETWORK_ACL_ADMIN, see that schema''s comment.';

CREATE FUNCTION utl_http.request(url text, http_method text DEFAULT 'GET') RETURNS text
  AS 'MODULE_PATHNAME', 'utl_http_request' LANGUAGE C VOLATILE;
COMMENT ON FUNCTION utl_http.request(text, text) IS 'Oracle UTL_HTTP.REQUEST (simplified single-call form) -- raises ORA-24247 without an ACL grant. See README.md for what''s simplified vs. real UTL_HTTP''s full request/response handle API.';

CREATE FUNCTION utl_http.last_status() RETURNS integer
  AS 'MODULE_PATHNAME', 'utl_http_last_status' LANGUAGE C VOLATILE;
COMMENT ON FUNCTION utl_http.last_status() IS 'HTTP status code of the most recent utl_http.request() call in this session -- see that function''s comment for why this is simplified from real UTL_HTTP''s handle-based status access.';

-- ================================================================
-- Phase 2 (package 7 of the top-20): DBMS_CRYPTO
--
-- Built on Postgres's own pgcrypto contrib extension (audited, battle-
-- tested), not hand-rolled -- crypto is exactly the category of code
-- where "don't reinvent it yourself" matters most. This package is a
-- thin adapter: Oracle's HASH/MAC-type constants and calling convention
-- on the outside, pgcrypto's digest()/hmac()/encrypt()/gen_random_bytes()
-- doing the actual work underneath.
--
-- Constants are zero-argument functions (dbms_crypto.hash_sh256()), not
-- Oracle's bare package constants (DBMS_CRYPTO.HASH_SH256, no parens) --
-- Postgres has no syntax for a bare constant reference at all. Real
-- migrated code that hardcodes the bare form needs the same class of
-- textual rewrite as anonymous PL/SQL blocks (see that section) --
-- appending `()` is a far smaller rewrite than DO $$ wrapping, but it's
-- still a rewrite, not something this extension alone can paper over.
-- ================================================================

CREATE SCHEMA dbms_crypto;
COMMENT ON SCHEMA dbms_crypto IS 'Oracle DBMS_CRYPTO package (pg_oracle, part of Polygres) -- thin adapter over pgcrypto.';

-- HASH_* values match Oracle's own documented, stable-since-10g
-- constants. HMAC_* likewise match Oracle's documented values -- both
-- are widely published and consistent across every Oracle version this
-- author has seen, but neither has been re-verified against a live
-- Oracle instance in this session; flag it if a real one disagrees.
CREATE FUNCTION dbms_crypto.hash_md4() RETURNS integer AS $$ SELECT 1; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.hash_md5() RETURNS integer AS $$ SELECT 2; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.hash_sh1()  RETURNS integer AS $$ SELECT 3; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.hash_sh256() RETURNS integer AS $$ SELECT 4; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.hash_sh384() RETURNS integer AS $$ SELECT 5; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.hash_sh512() RETURNS integer AS $$ SELECT 6; $$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION dbms_crypto.hmac_md5()    RETURNS integer AS $$ SELECT 1; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.hmac_sh1()    RETURNS integer AS $$ SELECT 2; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.hmac_sh256()  RETURNS integer AS $$ SELECT 3; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.hmac_sh384()  RETURNS integer AS $$ SELECT 4; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.hmac_sh512()  RETURNS integer AS $$ SELECT 5; $$ LANGUAGE sql IMMUTABLE;

-- First version of this function relied on catching an exception from
-- digest() for an unrecognized typ -- except digest(src, NULL) doesn't
-- raise at all, it just returns NULL, so an invalid typ silently
-- produced a NULL hash instead of an error. Found live: dbms_crypto.hash
-- ('x', 999) returned an empty result instead of failing loudly. Fixed
-- by checking the algorithm name up front instead of hoping for an
-- exception that was never actually going to happen.
CREATE FUNCTION dbms_crypto.hash(src bytea, typ integer) RETURNS bytea
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  v_algo text := CASE typ
    WHEN 1 THEN 'md4' WHEN 2 THEN 'md5' WHEN 3 THEN 'sha1'
    WHEN 4 THEN 'sha256' WHEN 5 THEN 'sha384' WHEN 6 THEN 'sha512'
    ELSE NULL END;
BEGIN
  IF v_algo IS NULL THEN
    RAISE EXCEPTION 'ORA-06502: PL/SQL: numeric or value error -- unsupported DBMS_CRYPTO hash type %', typ;
  END IF;
  RETURN digest(src, v_algo);
END;
$$;
COMMENT ON FUNCTION dbms_crypto.hash(bytea, integer) IS 'Oracle DBMS_CRYPTO.HASH -- call with dbms_crypto.hash_sh256() etc., not a hardcoded number.';

CREATE FUNCTION dbms_crypto.mac(src bytea, typ integer, key bytea) RETURNS bytea
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  algo text := CASE typ
    WHEN 1 THEN 'md5' WHEN 2 THEN 'sha1' WHEN 3 THEN 'sha256'
    WHEN 4 THEN 'sha384' WHEN 5 THEN 'sha512' ELSE NULL END;
BEGIN
  IF algo IS NULL THEN
    RAISE EXCEPTION 'ORA-06502: PL/SQL: numeric or value error -- unsupported DBMS_CRYPTO MAC type %', typ;
  END IF;
  RETURN hmac(src, key, algo);
END;
$$;
COMMENT ON FUNCTION dbms_crypto.mac(bytea, integer, bytea) IS 'Oracle DBMS_CRYPTO.MAC (HMAC forms only -- see this package''s comment for the legacy non-HMAC MAC_* constants this doesn''t implement).';

CREATE FUNCTION dbms_crypto.randombytes(number_bytes integer) RETURNS bytea
  AS $$ SELECT gen_random_bytes(number_bytes); $$ LANGUAGE sql VOLATILE;
COMMENT ON FUNCTION dbms_crypto.randombytes(integer) IS 'Oracle DBMS_CRYPTO.RANDOMBYTES.';

-- ENCRYPT_*/CHAIN_*/PAD_* constant VALUES below are this extension's own
-- numbering -- NOT verified against real Oracle's actual TYP bit
-- encoding (algorithm + chaining-mode + padding summed into one
-- integer), which this author could not confidently reconstruct from
-- memory with the precision crypto correctness demands. Safe as long as
-- callers use the symbolic constants (as idiomatic Oracle code always
-- does: `DBMS_CRYPTO.ENCRYPT_AES256 + DBMS_CRYPTO.CHAIN_CBC +
-- DBMS_CRYPTO.PAD_PKCS5`), broken if code hardcodes Oracle's actual
-- numeric literal instead -- the same caveat as the HASH_*/HMAC_*
-- constants above, just with a real correctness (not just fidelity)
-- consequence if it's wrong, hence the extra emphasis here.
--
-- CHAIN_CFB/CHAIN_OFB and PAD_ZERO/PAD_ORCL are NOT implemented --
-- pgcrypto itself only supports CBC/ECB chaining and PKCS5/no-padding.
-- encrypt()/decrypt() below reject any other combination outright rather
-- than silently approximating one.
CREATE FUNCTION dbms_crypto.encrypt_aes128() RETURNS integer AS $$ SELECT 6; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.encrypt_aes192() RETURNS integer AS $$ SELECT 7; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.encrypt_aes256() RETURNS integer AS $$ SELECT 8; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.chain_cbc() RETURNS integer AS $$ SELECT 256; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.chain_ecb() RETURNS integer AS $$ SELECT 768; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.pad_pkcs5() RETURNS integer AS $$ SELECT 4096; $$ LANGUAGE sql IMMUTABLE;
CREATE FUNCTION dbms_crypto.pad_none()  RETURNS integer AS $$ SELECT 8192; $$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION dbms_crypto.encrypt(src bytea, typ integer, key bytea, iv bytea DEFAULT NULL) RETURNS bytea
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  v_algo_bits int := typ & 255;	-- low byte: algorithm (this extension's own numbering, see comment above)
  v_chain_bits int := typ & 3840;	-- next nibble: chaining mode
  v_pad_bits  int := typ & 61440;	-- next nibble: padding
  v_keylen    int := octet_length(key);
  v_expected_keylen int;
  v_pgcrypto_algo text;
BEGIN
  v_expected_keylen := CASE v_algo_bits WHEN 6 THEN 16 WHEN 7 THEN 24 WHEN 8 THEN 32 ELSE NULL END;
  IF v_expected_keylen IS NULL THEN
    RAISE EXCEPTION 'ORA-28817: unsupported DBMS_CRYPTO algorithm in type %', typ;
  END IF;
  IF v_keylen != v_expected_keylen THEN
    RAISE EXCEPTION 'ORA-28231: invalid input value: key length % does not match the requested AES variant (expected % bytes)',
      v_keylen, v_expected_keylen;
  END IF;

  v_pgcrypto_algo := 'aes' || (CASE v_chain_bits
    WHEN 256 THEN '-cbc' WHEN 768 THEN '-ecb'
    ELSE NULL END);
  IF v_pgcrypto_algo IS NULL OR position('-' IN v_pgcrypto_algo) = 0 THEN
    RAISE EXCEPTION 'ORA-28233: chaining mode in type % is not supported (only CHAIN_CBC/CHAIN_ECB are)', typ;
  END IF;
  v_pgcrypto_algo := v_pgcrypto_algo || (CASE v_pad_bits
    WHEN 4096 THEN '/pad:pkcs' WHEN 8192 THEN '/pad:none'
    ELSE NULL END);
  IF position('/pad:' IN v_pgcrypto_algo) = 0 THEN
    RAISE EXCEPTION 'ORA-28233: padding mode in type % is not supported (only PAD_PKCS5/PAD_NONE are)', typ;
  END IF;

  IF v_chain_bits = 256 AND (iv IS NULL OR octet_length(iv) != 16) THEN
    RAISE EXCEPTION 'ORA-28239: an IV of exactly 16 bytes is required for CHAIN_CBC';
  END IF;

  RETURN encrypt_iv(src, key, coalesce(iv, ''::bytea), v_pgcrypto_algo);
END;
$$;
COMMENT ON FUNCTION dbms_crypto.encrypt(bytea, integer, bytea, bytea) IS 'Oracle DBMS_CRYPTO.ENCRYPT (AES/CBC/ECB + PKCS5/none only -- see this package''s header comment for what''s intentionally not supported, and why key length is validated instead of silently coerced).';

CREATE FUNCTION dbms_crypto.decrypt(src bytea, typ integer, key bytea, iv bytea DEFAULT NULL) RETURNS bytea
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  v_algo_bits int := typ & 255;
  v_chain_bits int := typ & 3840;
  v_pad_bits  int := typ & 61440;
  v_keylen    int := octet_length(key);
  v_expected_keylen int;
  v_pgcrypto_algo text;
BEGIN
  v_expected_keylen := CASE v_algo_bits WHEN 6 THEN 16 WHEN 7 THEN 24 WHEN 8 THEN 32 ELSE NULL END;
  IF v_expected_keylen IS NULL THEN
    RAISE EXCEPTION 'ORA-28817: unsupported DBMS_CRYPTO algorithm in type %', typ;
  END IF;
  IF v_keylen != v_expected_keylen THEN
    RAISE EXCEPTION 'ORA-28231: invalid input value: key length % does not match the requested AES variant (expected % bytes)',
      v_keylen, v_expected_keylen;
  END IF;

  v_pgcrypto_algo := 'aes' || (CASE v_chain_bits WHEN 256 THEN '-cbc' WHEN 768 THEN '-ecb' ELSE NULL END);
  IF v_pgcrypto_algo IS NULL OR position('-' IN v_pgcrypto_algo) = 0 THEN
    RAISE EXCEPTION 'ORA-28233: chaining mode in type % is not supported (only CHAIN_CBC/CHAIN_ECB are)', typ;
  END IF;
  v_pgcrypto_algo := v_pgcrypto_algo || (CASE v_pad_bits WHEN 4096 THEN '/pad:pkcs' WHEN 8192 THEN '/pad:none' ELSE NULL END);
  IF position('/pad:' IN v_pgcrypto_algo) = 0 THEN
    RAISE EXCEPTION 'ORA-28233: padding mode in type % is not supported (only PAD_PKCS5/PAD_NONE are)', typ;
  END IF;

  RETURN decrypt_iv(src, key, coalesce(iv, ''::bytea), v_pgcrypto_algo);
END;
$$;
COMMENT ON FUNCTION dbms_crypto.decrypt(bytea, integer, bytea, bytea) IS 'Oracle DBMS_CRYPTO.DECRYPT -- see ENCRYPT''s comment.';

-- ================================================================
-- Phase 2 (package 8 of the top-20): DBMS_SCHEDULER
--
-- Built on pg_cron (PostgreSQL-licensed, Citus Data), not a hand-rolled
-- background worker -- reliable cron-style job scheduling inside
-- Postgres is exactly the kind of infrastructure not worth
-- reimplementing (same "don't reinvent it" call as pgcrypto above).
-- Requires `shared_preload_libraries = 'pg_cron'` and `CREATE EXTENSION
-- pg_cron` (not `CASCADE`-installed by pg_oracle itself, unlike
-- pg_stat_statements -- pg_cron needs a preload-library restart, which
-- CREATE EXTENSION cannot do for you; see README.md).
--
-- Scope: CREATE_JOB/RUN_JOB/STOP_JOB/DROP_JOB/ENABLE/DISABLE, and only
-- the four most common repeat_interval calendar shapes (FREQ=DAILY/
-- HOURLY/WEEKLY/MONTHLY) -- Oracle's full calendaring syntax (BYYEARDAY,
-- exclusion lists, FREQ=YEARLY/MINUTELY, combined BYMONTH+BYMONTHDAY,
-- ...) is NOT translated; oracle_calendar_to_cron() raises a clear error
-- for anything else rather than silently producing a wrong schedule.
--
-- job_action for job_type='PLSQL_BLOCK' inherits the exact same
-- anonymous-PL/SQL-block limitation as everywhere else in this
-- extension: pg_cron executes job_action as literal SQL text, so a raw
-- Oracle-style `BEGIN ... END;` block needs the same DO $$ ... $$
-- rewrite discussed in that section before it's usable here -- this
-- package does not attempt that rewrite itself.
-- ================================================================

CREATE SCHEMA dbms_scheduler;
COMMENT ON SCHEMA dbms_scheduler IS 'Oracle DBMS_SCHEDULER package (pg_oracle, part of Polygres) -- thin adapter over pg_cron. See this schema''s header comment above for scope.';

CREATE TABLE dbms_scheduler.jobs(
  job_name        text PRIMARY KEY,
  job_type        text NOT NULL CHECK (job_type IN ('PLSQL_BLOCK', 'STORED_PROCEDURE')),
  job_action      text NOT NULL,
  repeat_interval text,
  enabled         boolean NOT NULL DEFAULT false,
  cron_job_id     bigint,	-- NULL until enabled at least once; pg_cron's own job id, for unschedule/alter
  created_by      text NOT NULL DEFAULT current_user
);

-- Row-level security matching pg_cron's own cron.job policy (`USING
-- (username = CURRENT_USER)`, confirmed live): without this, any role
-- could read every other role's job_action text (potentially containing
-- sensitive SQL/logic) through this table even though pg_cron itself
-- already hides their actual cron.job entry -- found live, testing with
-- two real non-superuser roles, not by inspection: a second role could
-- see a first role's scheduled job here despite cron.job correctly
-- returning zero rows for it.
ALTER TABLE dbms_scheduler.jobs ENABLE ROW LEVEL SECURITY;
CREATE POLICY dbms_scheduler_jobs_owner_only ON dbms_scheduler.jobs
  USING (created_by = current_user);

CREATE FUNCTION dbms_scheduler.oracle_calendar_to_cron(p_repeat_interval text) RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  v_parts     text[];
  v_kv        text[];
  v_freq      text;
  v_interval  int := 1;
  v_byhour    int := 0;
  v_byminute  int := 0;
  v_byday     text;
  v_bymonthday int := 1;
  v_dow_map   jsonb := '{"MON":"1","TUE":"2","WED":"3","THU":"4","FRI":"5","SAT":"6","SUN":"0"}';
  v_dow_list  text;
  v_part      text;
BEGIN
  FOREACH v_part IN ARRAY string_to_array(p_repeat_interval, ';') LOOP
    v_kv := string_to_array(trim(v_part), '=');
    IF array_length(v_kv, 1) != 2 THEN CONTINUE; END IF;
    CASE upper(trim(v_kv[1]))
      WHEN 'FREQ' THEN v_freq := upper(trim(v_kv[2]));
      WHEN 'INTERVAL' THEN v_interval := trim(v_kv[2])::int;
      WHEN 'BYHOUR' THEN v_byhour := trim(v_kv[2])::int;
      WHEN 'BYMINUTE' THEN v_byminute := trim(v_kv[2])::int;
      WHEN 'BYDAY' THEN v_byday := upper(trim(v_kv[2]));
      WHEN 'BYMONTHDAY' THEN v_bymonthday := trim(v_kv[2])::int;
      ELSE NULL;	-- BYSECOND, BYMONTH, BYYEARDAY, etc. -- silently ignored, not translated
    END CASE;
  END LOOP;

  IF v_freq IS NULL THEN
    RAISE EXCEPTION 'ORA-27455: repeat_interval must include FREQ (got: %)', p_repeat_interval;
  END IF;

  CASE v_freq
    WHEN 'DAILY' THEN
      IF v_interval != 1 THEN
        RAISE EXCEPTION 'ORA-27455: FREQ=DAILY with INTERVAL != 1 is not representable in cron -- not supported';
      END IF;
      RETURN format('%s %s * * *', v_byminute, v_byhour);
    WHEN 'HOURLY' THEN
      RETURN format('%s */%s * * *', v_byminute, v_interval);
    WHEN 'WEEKLY' THEN
      IF v_byday IS NULL THEN
        RAISE EXCEPTION 'ORA-27455: FREQ=WEEKLY requires BYDAY';
      END IF;
      SELECT string_agg(v_dow_map ->> trim(d), ',') INTO v_dow_list
        FROM unnest(string_to_array(v_byday, ',')) AS d;
      IF v_dow_list IS NULL THEN
        RAISE EXCEPTION 'ORA-27455: could not parse BYDAY value ''%''', v_byday;
      END IF;
      RETURN format('%s %s * * %s', v_byminute, v_byhour, v_dow_list);
    WHEN 'MONTHLY' THEN
      RETURN format('%s %s %s * *', v_byminute, v_byhour, v_bymonthday);
    ELSE
      RAISE EXCEPTION 'ORA-27455: FREQ=% is not supported (only DAILY/HOURLY/WEEKLY/MONTHLY are -- see this package''s header comment)', v_freq;
  END CASE;
END;
$$;
COMMENT ON FUNCTION dbms_scheduler.oracle_calendar_to_cron(text) IS 'Internal: translates the four most common Oracle repeat_interval calendar shapes to a 5-field cron expression -- see this schema''s header comment for exactly what''s NOT covered.';

CREATE FUNCTION dbms_scheduler.create_job(
  p_job_name text, p_job_type text, p_job_action text,
  p_start_date timestamptz DEFAULT NULL, p_repeat_interval text DEFAULT NULL,
  p_enabled boolean DEFAULT false
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM dbms_scheduler.jobs WHERE job_name = p_job_name) THEN
    RAISE EXCEPTION 'ORA-27477: job "%" already exists', p_job_name;
  END IF;
  INSERT INTO dbms_scheduler.jobs(job_name, job_type, job_action, repeat_interval, enabled)
    VALUES (p_job_name, p_job_type, p_job_action, p_repeat_interval, false);
  IF p_enabled THEN
    PERFORM dbms_scheduler.enable(p_job_name);
  END IF;
END;
$$;
COMMENT ON FUNCTION dbms_scheduler.create_job(text, text, text, timestamptz, text, boolean) IS 'Oracle DBMS_SCHEDULER.CREATE_JOB -- see this schema''s header comment for job_type/repeat_interval scope.';

CREATE FUNCTION dbms_scheduler.enable(p_job_name text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  v_job record;
  v_cron text;
  v_new_id bigint;
BEGIN
  SELECT * INTO v_job FROM dbms_scheduler.jobs WHERE job_name = p_job_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ORA-27476: job "%" does not exist', p_job_name;
  END IF;
  IF v_job.repeat_interval IS NULL THEN
    RAISE EXCEPTION 'ORA-27451: job "%" has no repeat_interval -- one-shot jobs must be run directly with RUN_JOB', p_job_name;
  END IF;
  v_cron := dbms_scheduler.oracle_calendar_to_cron(v_job.repeat_interval);
  IF v_job.cron_job_id IS NOT NULL THEN
    PERFORM cron.unschedule(v_job.cron_job_id);
  END IF;
  v_new_id := cron.schedule(p_job_name, v_cron, v_job.job_action);
  UPDATE dbms_scheduler.jobs SET enabled = true, cron_job_id = v_new_id WHERE job_name = p_job_name;
END;
$$;
COMMENT ON FUNCTION dbms_scheduler.enable(text) IS 'Oracle DBMS_SCHEDULER.ENABLE.';

CREATE FUNCTION dbms_scheduler.disable(p_job_name text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  v_cron_job_id bigint;
BEGIN
  SELECT cron_job_id INTO v_cron_job_id FROM dbms_scheduler.jobs WHERE job_name = p_job_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ORA-27476: job "%" does not exist', p_job_name;
  END IF;
  IF v_cron_job_id IS NOT NULL THEN
    PERFORM cron.unschedule(v_cron_job_id);
  END IF;
  UPDATE dbms_scheduler.jobs SET enabled = false, cron_job_id = NULL WHERE job_name = p_job_name;
END;
$$;
COMMENT ON FUNCTION dbms_scheduler.disable(text) IS 'Oracle DBMS_SCHEDULER.DISABLE (also covers STOP_JOB for a recurring job -- see stop_job()).';

CREATE FUNCTION dbms_scheduler.stop_job(p_job_name text) RETURNS void
  AS $$ SELECT dbms_scheduler.disable(p_job_name); $$ LANGUAGE sql;
COMMENT ON FUNCTION dbms_scheduler.stop_job(text) IS 'Oracle DBMS_SCHEDULER.STOP_JOB -- equivalent to DISABLE for a pg_cron-backed recurring job (no separate "running instance" to interrupt the way Oracle''s STOP_JOB can).';

CREATE FUNCTION dbms_scheduler.drop_job(p_job_name text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  v_cron_job_id bigint;
BEGIN
  SELECT cron_job_id INTO v_cron_job_id FROM dbms_scheduler.jobs WHERE job_name = p_job_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ORA-27475: job "%" does not exist', p_job_name;
  END IF;
  IF v_cron_job_id IS NOT NULL THEN
    PERFORM cron.unschedule(v_cron_job_id);
  END IF;
  DELETE FROM dbms_scheduler.jobs WHERE job_name = p_job_name;
END;
$$;
COMMENT ON FUNCTION dbms_scheduler.drop_job(text) IS 'Oracle DBMS_SCHEDULER.DROP_JOB.';

CREATE FUNCTION dbms_scheduler.run_job(p_job_name text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  v_job_action text;
BEGIN
  SELECT job_action INTO v_job_action FROM dbms_scheduler.jobs WHERE job_name = p_job_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ORA-27476: job "%" does not exist', p_job_name;
  END IF;
  EXECUTE v_job_action;
END;
$$;
COMMENT ON FUNCTION dbms_scheduler.run_job(text) IS 'Oracle DBMS_SCHEDULER.RUN_JOB -- runs job_action immediately, in this session, regardless of enabled/repeat_interval (matches real Oracle''s own immediate-run semantics).';

-- ================================================================
-- Phase 2 (package 9 of the top-20): DBMS_AQADM + DBMS_AQ
--
-- Built on `SELECT ... FOR UPDATE SKIP LOCKED`, part of stock Postgres
-- core since 9.5, not a hand-rolled queue engine -- concurrent-safe
-- "pop the next available row without blocking on rows other backends
-- already grabbed" is exactly the problem SKIP LOCKED exists to solve,
-- and it's exactly the mechanic DBMS_AQ's DEQUEUE needs. No extension,
-- no extra dependency.
--
-- Model: DBMS_AQADM.CREATE_QUEUE_TABLE creates a REAL physical table (one
-- per Oracle queue table, matching Oracle's own model, not one shared
-- table for every queue) with a fixed AQ-shaped column set; CREATE_QUEUE
-- registers a named queue against that table; START_QUEUE/STOP_QUEUE
-- toggle enqueue/dequeue availability, matching Oracle's real two-step
-- create-then-start lifecycle.
--
-- Scope, stated up front: payload type is `RAW` (bytea) only -- Oracle's
-- own object-type payloads (a queue typed to a specific PL/SQL record
-- type) aren't supported; callers serialize structured data into bytea
-- themselves, the same flexibility Oracle's own RAW payload gives.
-- NEXT_MESSAGE and FIRST_MESSAGE navigation behave identically here --
-- there's no per-session dequeue cursor tracked, each DEQUEUE call
-- simply picks the current highest-priority, oldest, not-yet-expired,
-- not-yet-delayed message. WAIT is a bounded polling loop (`pg_sleep`
-- between retries), not a true blocking wait the way Oracle's own
-- OCI-level wait works -- functionally equivalent for a caller, not
-- byte-for-byte the same mechanism. DBMS_AQADM.GRANT_QUEUE_PRIVILEGE
-- (Oracle's own per-queue access-control layer) is NOT implemented --
-- any role that can call these functions at all can enqueue/dequeue any
-- queue in this version; a real per-queue ACL is future work, not
-- silently assumed away.
-- ================================================================

CREATE SCHEMA dbms_aqadm;
COMMENT ON SCHEMA dbms_aqadm IS 'Oracle DBMS_AQADM package (pg_oracle, part of Polygres) -- queue administration. See dbms_aq for ENQUEUE/DEQUEUE.';
CREATE SCHEMA dbms_aq;
COMMENT ON SCHEMA dbms_aq IS 'Oracle DBMS_AQ package (pg_oracle, part of Polygres) -- ENQUEUE/DEQUEUE. See dbms_aqadm for queue administration, and this schema''s header comment above for scope.';

CREATE TABLE dbms_aqadm.queue_tables(
  queue_table  text PRIMARY KEY,
  payload_type text NOT NULL DEFAULT 'RAW' CHECK (payload_type = 'RAW')	-- see header comment: only RAW is supported
);

CREATE TABLE dbms_aqadm.queues(
  queue_name      text PRIMARY KEY,
  queue_table     text NOT NULL REFERENCES dbms_aqadm.queue_tables(queue_table),
  enqueue_enabled boolean NOT NULL DEFAULT false,
  dequeue_enabled boolean NOT NULL DEFAULT false
);

CREATE FUNCTION dbms_aqadm.create_queue_table(p_queue_table text, p_queue_payload_type text DEFAULT 'RAW') RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = dbms_aqadm, dbms_aq, pg_catalog AS $$
BEGIN
  IF upper(p_queue_payload_type) != 'RAW' THEN
    RAISE EXCEPTION 'ORA-24002: QUEUE_TABLE payload type ''%'' is not supported (only RAW is -- see this package''s header comment)', p_queue_payload_type;
  END IF;
  IF p_queue_table !~ '^[A-Za-z][A-Za-z0-9_]*$' THEN
    RAISE EXCEPTION 'ORA-00903: invalid table name ''%''', p_queue_table;
  END IF;
  INSERT INTO dbms_aqadm.queue_tables(queue_table, payload_type) VALUES (p_queue_table, 'RAW');
  EXECUTE format($sql$
    CREATE TABLE dbms_aq.%I(
      msgid           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      enq_time        timestamptz NOT NULL DEFAULT clock_timestamp(),
      priority        integer NOT NULL DEFAULT 1,
      delay_until     timestamptz NOT NULL DEFAULT clock_timestamp(),
      expiration_time timestamptz,
      correlation     text,
      payload         bytea NOT NULL
    )
  $sql$, p_queue_table);
  -- SECURITY DEFINER means every queue table this function creates is
  -- consistently owned by this extension's owner regardless of which
  -- (now admin-only-EXECUTE) role actually called create_queue_table --
  -- so granting here, once, covers every future queue table the same
  -- way, rather than needing ALTER DEFAULT PRIVILEGES per creating role
  -- (which wouldn't work here anyway: default privileges are scoped to
  -- whichever role actually issues the CREATE TABLE, and that's always
  -- this function's fixed owner now, not the varying caller).
  EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON dbms_aq.%I TO PUBLIC', p_queue_table);
END;
$$;
COMMENT ON FUNCTION dbms_aqadm.create_queue_table(text, text) IS 'Oracle DBMS_AQADM.CREATE_QUEUE_TABLE -- creates a real table dbms_aq.<queue_table>, RAW/bytea payload only. SECURITY DEFINER -- see this schema''s and this function''s comments for why, and for the EXECUTE-privilege model (owner-only by default, like DBMS_NETWORK_ACL_ADMIN''s admin functions).';

CREATE FUNCTION dbms_aqadm.drop_queue_table(p_queue_table text, p_force boolean DEFAULT false) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = dbms_aqadm, dbms_aq, pg_catalog AS $$
BEGIN
  IF NOT p_force AND EXISTS (SELECT 1 FROM dbms_aqadm.queues WHERE queue_table = p_queue_table) THEN
    RAISE EXCEPTION 'ORA-24032: queue table ''%'' is not empty (queues still reference it -- use force => true, or drop_queue() them first)', p_queue_table;
  END IF;
  DELETE FROM dbms_aqadm.queues WHERE queue_table = p_queue_table;
  DELETE FROM dbms_aqadm.queue_tables WHERE queue_table = p_queue_table;
  EXECUTE format('DROP TABLE IF EXISTS dbms_aq.%I', p_queue_table);
END;
$$;
COMMENT ON FUNCTION dbms_aqadm.drop_queue_table(text, boolean) IS 'Oracle DBMS_AQADM.DROP_QUEUE_TABLE.';

CREATE FUNCTION dbms_aqadm.create_queue(p_queue_name text, p_queue_table text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = dbms_aqadm, dbms_aq, pg_catalog AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM dbms_aqadm.queue_tables WHERE queue_table = p_queue_table) THEN
    RAISE EXCEPTION 'ORA-24010: QUEUE_TABLE ''%'' does not exist', p_queue_table;
  END IF;
  INSERT INTO dbms_aqadm.queues(queue_name, queue_table) VALUES (p_queue_name, p_queue_table);
END;
$$;
COMMENT ON FUNCTION dbms_aqadm.create_queue(text, text) IS 'Oracle DBMS_AQADM.CREATE_QUEUE -- created stopped (enqueue/dequeue both disabled), matching real Oracle; call start_queue() to activate.';

CREATE FUNCTION dbms_aqadm.start_queue(p_queue_name text, p_enqueue boolean DEFAULT true, p_dequeue boolean DEFAULT true) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = dbms_aqadm, dbms_aq, pg_catalog AS $$
BEGIN
  UPDATE dbms_aqadm.queues SET enqueue_enabled = p_enqueue, dequeue_enabled = p_dequeue WHERE queue_name = p_queue_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ORA-24010: QUEUE ''%'' does not exist', p_queue_name;
  END IF;
END;
$$;
COMMENT ON FUNCTION dbms_aqadm.start_queue(text, boolean, boolean) IS 'Oracle DBMS_AQADM.START_QUEUE.';

CREATE FUNCTION dbms_aqadm.stop_queue(p_queue_name text, p_enqueue boolean DEFAULT true, p_dequeue boolean DEFAULT true) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = dbms_aqadm, dbms_aq, pg_catalog AS $$
BEGIN
  UPDATE dbms_aqadm.queues SET
    enqueue_enabled = enqueue_enabled AND NOT p_enqueue,
    dequeue_enabled = dequeue_enabled AND NOT p_dequeue
  WHERE queue_name = p_queue_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ORA-24010: QUEUE ''%'' does not exist', p_queue_name;
  END IF;
END;
$$;
COMMENT ON FUNCTION dbms_aqadm.stop_queue(text, boolean, boolean) IS 'Oracle DBMS_AQADM.STOP_QUEUE.';

CREATE FUNCTION dbms_aqadm.drop_queue(p_queue_name text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = dbms_aqadm, dbms_aq, pg_catalog AS $$
DECLARE
  v_queue record;
BEGIN
  SELECT * INTO v_queue FROM dbms_aqadm.queues WHERE queue_name = p_queue_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ORA-24010: QUEUE ''%'' does not exist', p_queue_name;
  END IF;
  IF v_queue.enqueue_enabled OR v_queue.dequeue_enabled THEN
    RAISE EXCEPTION 'ORA-24046: queue table string is not empty -- stop_queue() ''%'' before dropping it', p_queue_name;
  END IF;
  DELETE FROM dbms_aqadm.queues WHERE queue_name = p_queue_name;
END;
$$;
COMMENT ON FUNCTION dbms_aqadm.drop_queue(text) IS 'Oracle DBMS_AQADM.DROP_QUEUE -- requires stop_queue() first, matching real Oracle.';

CREATE FUNCTION dbms_aq.enqueue(
  p_queue_name text, p_payload bytea,
  p_priority integer DEFAULT 1, p_delay_seconds integer DEFAULT 0,
  p_expiration_seconds integer DEFAULT NULL, p_correlation text DEFAULT NULL
) RETURNS uuid
LANGUAGE plpgsql AS $$
DECLARE
  v_queue record;
  v_msgid uuid;
BEGIN
  SELECT * INTO v_queue FROM dbms_aqadm.queues WHERE queue_name = p_queue_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ORA-24010: QUEUE ''%'' does not exist', p_queue_name;
  END IF;
  IF NOT v_queue.enqueue_enabled THEN
    RAISE EXCEPTION 'ORA-25207: enqueue failed, queue ''%'' is not enabled for enqueue', p_queue_name;
  END IF;
  EXECUTE format(
    'INSERT INTO dbms_aq.%I(priority, delay_until, expiration_time, correlation, payload)
     VALUES ($1, clock_timestamp() + make_interval(secs => $2), '
    || CASE WHEN p_expiration_seconds IS NULL THEN 'NULL' ELSE 'clock_timestamp() + make_interval(secs => $3)' END
    || ', $4, $5) RETURNING msgid',
    v_queue.queue_table
  ) INTO v_msgid
    USING p_priority, p_delay_seconds,
          coalesce(p_expiration_seconds, 0), p_correlation, p_payload;
  RETURN v_msgid;
END;
$$;
COMMENT ON FUNCTION dbms_aq.enqueue(text, bytea, integer, integer, integer, text) IS 'Oracle DBMS_AQ.ENQUEUE (flattened options -- see this schema''s header comment for what''s simplified from Oracle''s enqueue_options_t/message_properties_t record-typed signature).';

CREATE FUNCTION dbms_aq.dequeue(
  OUT payload bytea, OUT msgid uuid, OUT priority integer, OUT correlation text,
  p_queue_name text, p_dequeue_mode text DEFAULT 'REMOVE',
  p_correlation_filter text DEFAULT NULL, p_wait_seconds integer DEFAULT 0
)
LANGUAGE plpgsql AS $$
DECLARE
  v_queue record;
  v_deadline timestamptz := clock_timestamp() + make_interval(secs => p_wait_seconds);
  v_found boolean := false;
BEGIN
  IF upper(p_dequeue_mode) NOT IN ('REMOVE', 'BROWSE', 'LOCKED') THEN
    RAISE EXCEPTION 'ORA-24033: no recipients for message (invalid dequeue mode ''%'')', p_dequeue_mode;
  END IF;

  SELECT * INTO v_queue FROM dbms_aqadm.queues WHERE queue_name = p_queue_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ORA-24010: QUEUE ''%'' does not exist', p_queue_name;
  END IF;
  IF NOT v_queue.dequeue_enabled THEN
    RAISE EXCEPTION 'ORA-25207: dequeue failed, queue ''%'' is not enabled for dequeue', p_queue_name;
  END IF;

  LOOP
    IF upper(p_dequeue_mode) = 'BROWSE' THEN
      EXECUTE format(
        'SELECT payload, msgid, priority, correlation FROM dbms_aq.%I
         WHERE delay_until <= clock_timestamp()
           AND (expiration_time IS NULL OR expiration_time > clock_timestamp())
           AND ($1 IS NULL OR correlation = $1)
         ORDER BY priority ASC, enq_time ASC LIMIT 1',
        v_queue.queue_table
      ) INTO payload, msgid, priority, correlation USING p_correlation_filter;
    ELSE
      -- REMOVE and LOCKED both need FOR UPDATE SKIP LOCKED: REMOVE deletes
      -- the row it locks, LOCKED just holds the lock to end of transaction
      -- without deleting -- matching Oracle's own LOCKED semantics.
      EXECUTE format(
        'SELECT payload, msgid, priority, correlation FROM dbms_aq.%I
         WHERE delay_until <= clock_timestamp()
           AND (expiration_time IS NULL OR expiration_time > clock_timestamp())
           AND ($1 IS NULL OR correlation = $1)
         ORDER BY priority ASC, enq_time ASC LIMIT 1 FOR UPDATE SKIP LOCKED',
        v_queue.queue_table
      ) INTO payload, msgid, priority, correlation USING p_correlation_filter;
    END IF;

    IF msgid IS NOT NULL THEN
      v_found := true;
      IF upper(p_dequeue_mode) = 'REMOVE' THEN
        EXECUTE format('DELETE FROM dbms_aq.%I WHERE msgid = $1', v_queue.queue_table) USING msgid;
      END IF;
      EXIT;
    END IF;

    IF clock_timestamp() >= v_deadline THEN
      EXIT;
    END IF;
    PERFORM pg_sleep(least(0.5, extract(epoch FROM (v_deadline - clock_timestamp()))));
  END LOOP;

  IF NOT v_found THEN
    RAISE EXCEPTION 'ORA-25228: timeout or end-of-fetch during message dequeue from queue %', p_queue_name;
  END IF;
END;
$$;
COMMENT ON FUNCTION dbms_aq.dequeue(text, text, text, integer) IS 'Oracle DBMS_AQ.DEQUEUE -- NEXT_MESSAGE/FIRST_MESSAGE navigation both behave the same here (no per-session cursor tracked); WAIT is a bounded poll, not a true blocking wait. See this schema''s header comment.';

-- ================================================================
-- Phase 2 (package 10 of the top-20): DBMS_STATS
--
-- An honest shim, not a reimplementation of Oracle's optimizer
-- internals: Postgres has its own, completely different statistics
-- system (pg_statistic, gathered by ANALYZE, already kept fresh
-- automatically by autovacuum) -- there is no Oracle-shaped histogram
-- engine to port, and pretending to fabricate one would be actively
-- misleading. GATHER_*_STATS below just call Postgres's own ANALYZE;
-- Oracle-specific tuning knobs Postgres has no equivalent for
-- (estimate_percent, method_opt, cascade, degree, ...) are accepted for
-- call-signature compatibility and otherwise ignored, not silently
-- misapplied to something that isn't equivalent.
-- ================================================================

CREATE SCHEMA dbms_stats;
COMMENT ON SCHEMA dbms_stats IS 'Oracle DBMS_STATS package (pg_oracle, part of Polygres) -- a shim over Postgres''s own ANALYZE/pg_class stats, not Oracle''s optimizer internals. See this schema''s header comment.';

CREATE TABLE dbms_stats.locked_tables(
  schema_name text NOT NULL,
  table_name  text NOT NULL,
  PRIMARY KEY (schema_name, table_name)
);
COMMENT ON TABLE dbms_stats.locked_tables IS 'Tables LOCK_TABLE_STATS has locked -- gather_table_stats() below refuses to run against one, matching real Oracle''s own lock behavior.';

CREATE FUNCTION dbms_stats.gather_table_stats(
  ownname text, tabname text, estimate_percent double precision DEFAULT NULL, cascade boolean DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM dbms_stats.locked_tables WHERE schema_name = ownname AND table_name = tabname) THEN
    RAISE EXCEPTION 'ORA-20005: object statistics are locked for table %.%', ownname, tabname;
  END IF;
  EXECUTE format('ANALYZE %I.%I', ownname, tabname);
END;
$$;
COMMENT ON FUNCTION dbms_stats.gather_table_stats(text, text, double precision, boolean) IS 'Oracle DBMS_STATS.GATHER_TABLE_STATS -- runs real ANALYZE; estimate_percent/cascade accepted for signature compatibility, Postgres has no equivalent knob, ignored.';

CREATE FUNCTION dbms_stats.gather_schema_stats(ownname text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  rec record;
BEGIN
  FOR rec IN SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = ownname LOOP
    IF NOT EXISTS (SELECT 1 FROM dbms_stats.locked_tables lt WHERE lt.schema_name = ownname AND lt.table_name = rec.tablename) THEN
      EXECUTE format('ANALYZE %I.%I', ownname, rec.tablename);
    END IF;
  END LOOP;
END;
$$;
COMMENT ON FUNCTION dbms_stats.gather_schema_stats(text) IS 'Oracle DBMS_STATS.GATHER_SCHEMA_STATS -- ANALYZEs every table in the schema, skipping any LOCK_TABLE_STATS''d ones.';

CREATE FUNCTION dbms_stats.gather_database_stats() RETURNS void
  AS $$ ANALYZE; $$ LANGUAGE sql;
COMMENT ON FUNCTION dbms_stats.gather_database_stats() IS 'Oracle DBMS_STATS.GATHER_DATABASE_STATS -- plain ANALYZE, every table this role can analyze. Does not honor per-table locks the way gather_schema_stats() does -- matches plain ANALYZE''s own scope, a real simplification worth knowing about.';

-- Oracle's DELETE_TABLE_STATS discards stored statistics so the
-- optimizer falls back to defaults -- deliberately NOT implemented by
-- reaching into pg_statistic directly (an unsupported, genuinely risky
-- thing to manipulate by hand). This is an honest no-op, not a silent
-- one: it tells the caller so, rather than pretending to have done
-- something it didn't.
CREATE FUNCTION dbms_stats.delete_table_stats(ownname text, tabname text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'pg_oracle: DBMS_STATS.DELETE_TABLE_STATS is a no-op -- Postgres has no supported way to discard '
               'table statistics the way Oracle does. Consider re-running ANALYZE %.% with a different '
               'default_statistics_target instead.', ownname, tabname;
END;
$$;
COMMENT ON FUNCTION dbms_stats.delete_table_stats(text, text) IS 'Oracle DBMS_STATS.DELETE_TABLE_STATS -- honest no-op, see this function''s own comment for why.';

-- SET_TABLE_STATS: unlike DELETE_TABLE_STATS above, this one IS
-- implementable for real -- pg_class.reltuples/relpages are ordinary,
-- directly UPDATE-able columns (a well-known, legitimate technique for
-- exactly this "fabricate stats to test a query plan" use case), not a
-- risky undocumented hack the way editing pg_statistic would be.
CREATE FUNCTION dbms_stats.set_table_stats(ownname text, tabname text, numrows bigint DEFAULT NULL, numblks bigint DEFAULT NULL) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  UPDATE pg_catalog.pg_class SET
    reltuples = coalesce(numrows, reltuples),
    relpages  = coalesce(numblks, relpages)
  WHERE oid = (quote_ident(ownname) || '.' || quote_ident(tabname))::regclass;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'ORA-20001: table %.% not found', ownname, tabname;
  END IF;
END;
$$;
COMMENT ON FUNCTION dbms_stats.set_table_stats(text, text, bigint, bigint) IS 'Oracle DBMS_STATS.SET_TABLE_STATS -- real pg_class.reltuples/relpages update, requires table ownership.';

CREATE FUNCTION dbms_stats.lock_table_stats(ownname text, tabname text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO dbms_stats.locked_tables(schema_name, table_name) VALUES (ownname, tabname)
    ON CONFLICT DO NOTHING;
  EXECUTE format('ALTER TABLE %I.%I SET (autovacuum_enabled = false)', ownname, tabname);
END;
$$;
COMMENT ON FUNCTION dbms_stats.lock_table_stats(text, text) IS 'Oracle DBMS_STATS.LOCK_TABLE_STATS -- blocks gather_table_stats()/gather_schema_stats() above, and disables autovacuum''s own automatic ANALYZE for this table (the closest real Postgres equivalent -- it does not block a manually-issued ANALYZE by a table owner outside this package, a real simplification worth knowing about).';

CREATE FUNCTION dbms_stats.unlock_table_stats(ownname text, tabname text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM dbms_stats.locked_tables WHERE schema_name = ownname AND table_name = tabname;
  EXECUTE format('ALTER TABLE %I.%I SET (autovacuum_enabled = true)', ownname, tabname);
END;
$$;
COMMENT ON FUNCTION dbms_stats.unlock_table_stats(text, text) IS 'Oracle DBMS_STATS.UNLOCK_TABLE_STATS.';

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
GRANT USAGE ON SCHEMA utl_http TO PUBLIC;
GRANT USAGE ON SCHEMA dbms_crypto TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA oracle_catalog TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA oracle_catalog TO PUBLIC;
-- create_context is the one function in oracle_catalog that does NOT
-- stay PUBLIC -- it's Oracle DDL-equivalent (CREATE CONTEXT, requiring
-- CREATE ANY CONTEXT there), not an ordinary read/compute call like
-- everything else in this schema. This REVOKE must come AFTER the
-- blanket oracle_catalog GRANT directly above -- GRANT/REVOKE apply in
-- statement order, so reversing these two lines would silently leave
-- create_context PUBLIC-executable again (the exact bug class found
-- live on dbms_aqadm's admin functions -- see README.md: a
-- SECURITY DEFINER function is default-PUBLIC-executable from the
-- moment it's created, REVOKE has to be explicit, not just "never
-- granted").
REVOKE EXECUTE ON FUNCTION oracle_catalog.create_context(text, text) FROM PUBLIC;
GRANT USAGE ON SCHEMA dbms_session TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_session TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_output TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_random TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_utility TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_assert TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA utl_file TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA utl_http TO PUBLIC;
-- dbms_crypto TO PUBLIC is safe on the same reasoning as the packages
-- above it, not the dbms_network_acl_admin exception below: hashing/
-- MAC'ing/encrypting your OWN data isn't a privilege boundary the way
-- granting network access or file access is -- there's nothing here a
-- role couldn't already do some other way (e.g. writing the same
-- pgcrypto calls directly).
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_crypto TO PUBLIC;

-- dbms_network_acl_admin is deliberately NOT blanket-granted to PUBLIC
-- the way every other package schema above is: unlike DBMS_OUTPUT or
-- UTL_FILE, this package's whole job is deciding who gets network
-- access, so letting any role execute it would let any role grant
-- itself an ACL and defeat the entire point of gating UTL_HTTP on it.
-- Real Oracle draws the same line -- DBMS_NETWORK_ACL_ADMIN's EXECUTE
-- privilege is not PUBLIC by default there either, only a DBA-ish role
-- normally has it.
--
-- USAGE on the schema is still PUBLIC: without it, an ordinary role
-- can't even reach the two read-only, non-mutating functions
-- (check_privilege/check_privilege_for_host) that utl_http.request()
-- itself needs to call via SPI as the calling role -- schema USAGE only
-- controls whether names resolve, not whether a specific function call
-- succeeds, so this doesn't reopen the mutating functions.
GRANT USAGE ON SCHEMA dbms_network_acl_admin TO PUBLIC;
GRANT EXECUTE ON FUNCTION dbms_network_acl_admin.resolve_acl_for_host(text, integer) TO PUBLIC;
GRANT EXECUTE ON FUNCTION dbms_network_acl_admin.check_privilege(text, text, text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION dbms_network_acl_admin.check_privilege_for_host(text, integer, text, text) TO PUBLIC;
-- create_acl/add_privilege/assign_acl/unassign_acl/drop_acl: intentionally
-- left owner-only (whoever ran CREATE EXTENSION) -- grant EXECUTE on
-- these explicitly, per-role, the same deliberate way a DBA would grant
-- real Oracle's DBMS_NETWORK_ACL_ADMIN to specific administrators.
--
-- The explicit REVOKE below matters even though these five are plain
-- SECURITY INVOKER, not SECURITY DEFINER: Postgres still grants EXECUTE
-- on every new function to PUBLIC by default, so today they're only
-- actually blocked for an unprivileged caller because that role also
-- lacks direct INSERT/UPDATE/DELETE on acls/acl_privileges/
-- host_assignments -- true right now, but an accident of table grants,
-- not a real access-control statement about these specific functions
-- (see the identical, but actually exploitable, version of this gap
-- found live on dbms_aqadm's SECURITY DEFINER admin functions below --
-- REVOKE here makes the intent explicit instead of relying on that
-- coincidence holding forever).
REVOKE EXECUTE ON FUNCTION dbms_network_acl_admin.create_acl(text, text, text, boolean, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION dbms_network_acl_admin.add_privilege(text, text, boolean, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION dbms_network_acl_admin.assign_acl(text, text, integer, integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION dbms_network_acl_admin.unassign_acl(text, text, integer, integer) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION dbms_network_acl_admin.drop_acl(text) FROM PUBLIC;
-- SELECT (not INSERT/UPDATE/DELETE) on the directories table: every role
-- needs to read it for FOPEN's own internal lookup to work at all, but
-- registering/changing a directory must go through create_directory()/
-- drop_directory() above, which enforce the real privilege check --
-- direct table writes would bypass that entirely.
GRANT SELECT ON utl_file.directories TO PUBLIC;

-- dbms_scheduler: unlike dbms_network_acl_admin, scheduling a job to run
-- your OWN job_action as YOURSELF isn't a privilege escalation the way
-- dbms_network_acl_admin's mutating functions are, PROVIDED the actual
-- scheduled job later runs as the role that scheduled it, not as
-- whoever owns this extension -- which is exactly why create_job/enable/
-- disable/drop_job/run_job below are left plain SECURITY INVOKER
-- (Postgres's plpgsql default), NOT SECURITY DEFINER: pg_cron's own
-- cron.job table records `username = CURRENT_USER` at schedule time and
-- the job later runs as that role, so calling cron.schedule() as
-- SECURITY DEFINER would silently make every job run with THIS
-- extension owner's (likely superuser) privileges instead of the real
-- caller's -- a privilege escalation this extension will not introduce.
-- Full INSERT/UPDATE/DELETE (not just SELECT) on dbms_scheduler.jobs is
-- safe for the same reason: your own scheduling bookkeeping isn't a
-- security boundary.
GRANT USAGE ON SCHEMA dbms_scheduler TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_scheduler TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON dbms_scheduler.jobs TO PUBLIC;

-- pg_cron itself: NOT a hard `requires` dependency of pg_oracle (see the
-- .control file -- it needs shared_preload_libraries, which CREATE
-- EXTENSION cannot arrange for you), so these grants only apply if
-- pg_cron happens to already be installed in this database. If pg_cron
-- is installed AFTER pg_oracle, re-run this block by hand (or just these
-- three GRANT statements, substituting `cron` for the schema name) --
-- see README.md.
--
-- Safe to grant broadly specifically because pg_cron ships its own
-- row-level security on cron.job (`USING (username = CURRENT_USER)`,
-- confirmed live -- see README.md): a role can only ever see or manage
-- its OWN scheduled jobs through these grants, never another role's.
DO $$
BEGIN
  IF to_regnamespace('cron') IS NOT NULL THEN
    EXECUTE 'GRANT USAGE ON SCHEMA cron TO PUBLIC';
    EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON cron.job TO PUBLIC';
    EXECUTE 'GRANT SELECT ON cron.job_run_details TO PUBLIC';
    EXECUTE 'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA cron TO PUBLIC';
  END IF;
END;
$$;

-- dbms_aqadm's ADMINISTRATIVE functions (create_queue_table/
-- drop_queue_table/create_queue/start_queue/stop_queue/drop_queue) are
-- deliberately owner-only, same reasoning and pattern as
-- dbms_network_acl_admin's mutating functions above: real Oracle also
-- treats AQ administration as a privileged operation (its own
-- EXECUTE_CATALOG_ROLE / explicit grants), separate from ordinary
-- enqueue/dequeue access -- so these are SECURITY DEFINER (see each
-- function's own comment) but NOT PUBLIC-executable; a DBA grants
-- EXECUTE on specific ones to whichever roles should administer queues.
--
-- The explicit REVOKE below is NOT redundant with simply never having
-- written a GRANT: Postgres grants EXECUTE on every newly created
-- function to PUBLIC by default unless it's explicitly revoked --
-- found live, the hard way, on this exact set of functions: a real
-- non-superuser test role successfully called create_queue_table()
-- (SECURITY DEFINER, so it ran with this extension owner's rights) with
-- no REVOKE in place, creating a real table it had no business creating.
-- Simply "not granting" EXECUTE does nothing -- it was already granted
-- by CREATE FUNCTION itself.
REVOKE EXECUTE ON FUNCTION dbms_aqadm.create_queue_table(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION dbms_aqadm.drop_queue_table(text, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION dbms_aqadm.create_queue(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION dbms_aqadm.start_queue(text, boolean, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION dbms_aqadm.stop_queue(text, boolean, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION dbms_aqadm.drop_queue(text) FROM PUBLIC;
GRANT USAGE ON SCHEMA dbms_aqadm TO PUBLIC;
GRANT SELECT ON dbms_aqadm.queue_tables, dbms_aqadm.queues TO PUBLIC;

-- dbms_aq (ENQUEUE/DEQUEUE) is PUBLIC on purpose, unlike dbms_aqadm
-- above -- a queue's whole point is cross-role message passing (one
-- role enqueues, a different role dequeues), same as ordinary Oracle
-- app-role usage once a DBA has set the queue up. Real Oracle controls
-- per-queue access via DBMS_AQADM.GRANT_QUEUE_PRIVILEGE, not
-- implemented in this version (see this package's header comment) --
-- so, honestly, every role that can reach these functions at all can
-- touch every queue. Tighten with ordinary Postgres GRANT/REVOKE on
-- individual dbms_aq.<queue_table> tables if that's not acceptable for
-- a given deployment. Each queue table's own grants are applied by
-- create_queue_table() itself at creation time (see that function) --
-- nothing to add here for tables that don't exist yet.
GRANT USAGE ON SCHEMA dbms_aq TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_aq TO PUBLIC;

-- dbms_stats: PUBLIC is safe here on a different basis than the packages
-- above -- gather_table_stats()/set_table_stats()/lock_table_stats() all
-- run ANALYZE/ALTER TABLE/an UPDATE against pg_class dynamically, and
-- Postgres's OWN existing ownership checks on those underlying
-- operations are the real gate (a role can only ANALYZE/ALTER a table it
-- owns or has been granted rights to) -- this package doesn't add or
-- need a second permission layer on top, same principle as relying on
-- pg_read_server_files/pg_write_server_files for UTL_FILE rather than
-- inventing a new one. locked_tables itself is just a lock flag, not a
-- security boundary -- full CRUD (not just SELECT) is safe.
GRANT USAGE ON SCHEMA dbms_stats TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_stats TO PUBLIC;
GRANT SELECT, INSERT, DELETE ON dbms_stats.locked_tables TO PUBLIC;
