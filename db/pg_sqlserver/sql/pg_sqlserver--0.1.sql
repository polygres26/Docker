-- pg_sqlserver -- SQL Server compatibility for Postgres (part of the
-- Polygres extension collection). Companion to db/pg_oracle and
-- db/pg_mysql, activated the same way: `SET db_emulation = 'sqlserver'`
-- appends `sys` onto search_path so unqualified SQL Server-shaped
-- references (sys.tables, OBJECT_ID(...), SCOPE_IDENTITY(), ...) resolve
-- without a caller spelling out sys.* by hand -- see
-- db/pg_oracle/src/pg_oracle.c's own header comment for the full
-- mechanism (this extension doesn't reimplement it, just reuses the same
-- GUC and search_path-append logic; see pg_sqlserver.control for exactly
-- why that lives in pg_oracle's C module and not here).
--
-- Named `sys`, not `sqlserver_catalog`: real SQL Server puts everything --
-- catalog views AND system functions -- in one schema literally named
-- sys, so this follows that instead of inventing a name nothing outside
-- this project would recognize.
--
-- Scope, stated plainly, mirroring db/pg_oracle and db/pg_mysql's own
-- scope sections: this is the introspection surface real tools/drivers
-- actually query (SSMS, sqlcmd, ODBC catalog functions, most ORMs'
-- schema introspection all read sys.tables/sys.columns/sys.objects
-- directly, the SQL Server analog of why V$VERSION/DBA_TABLES mattered
-- for Oracle tooling and SHOW COLUMNS/SHOW CREATE TABLE mattered for
-- MySQL), plus the handful of T-SQL system functions cheap to implement
-- exactly (OBJECT_ID/OBJECT_NAME/DB_NAME/SCHEMA_NAME/COL_NAME/
-- SCOPE_IDENTITY, CHARINDEX/STUFF/LEN/PATINDEX/REPLICATE, IIF). NOT yet
-- covered, real and worth stating: CONVERT() with style codes (the SQL
-- Server analog of pg_oracle's TO_CHAR/TO_DATE format-string problem --
-- and it collides with Postgres's own CONVERT() for encoding conversion,
-- so it likely needs the same schema-qualification-at-translation-time
-- treatment TO_CHAR/TO_DATE got, not yet done), DATEADD/DATEDIFF (real
-- unit-name/argument-order translation work, not a rename), MERGE,
-- OUTPUT clauses, table variables, sp_* system procedures, and
-- @@ROWCOUNT/@@ERROR (need real per-statement server-side state this
-- package doesn't track yet -- @@IDENTITY is the one @@-global handled,
-- via DialectTranslations.java rewriting it to a call to
-- sys.scope_identity() below, since Postgres has no way to declare a
-- function or view literally named with a leading @@).
CREATE SCHEMA sys;
COMMENT ON SCHEMA sys IS 'SQL Server compatibility (pg_sqlserver, part of Polygres) -- sys.* catalog views and T-SQL system functions. See this extension''s own header comment in sql/pg_sqlserver--0.1.sql for real scope/limits.';

CREATE FUNCTION sys.emulation_active() RETURNS boolean
  AS '$libdir/pg_oracle', 'pg_sqlserver_emulation_active' LANGUAGE C STABLE;
COMMENT ON FUNCTION sys.emulation_active() IS 'True when this session has SET db_emulation = ''sqlserver''. Binds directly against pg_oracle''s shared library -- see pg_sqlserver.control for why.';

-- ================================================================
-- Catalog views -- sys.schemas/tables/views/objects/columns/indexes/
-- foreign_keys/databases/types, each a thin view over real pg_catalog/
-- information_schema data (same "real data, SQL-Server-shaped column
-- names" principle as pg_oracle's DBA_*/V$ views over pg_tables/
-- pg_stat_statements).
--
-- object_id is a Postgres pg_class.oid, cast to a plain integer -- a
-- stable, unique-per-object integer the same way real SQL Server's
-- object_id is, even though the two numbering schemes obviously don't
-- (and can't) agree with each other. Every function/view below that
-- deals in object_ids is internally consistent with every other one
-- here; nothing tries to match a real SQL Server database's actual
-- historical object_id values, which would be meaningless across
-- systems anyway.
-- ================================================================

CREATE VIEW sys.schemas AS
  SELECT n.nspname::name AS name, n.oid::int AS schema_id, n.nspowner::int AS principal_id
  FROM pg_catalog.pg_namespace n
  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'sys')
    AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%';
COMMENT ON VIEW sys.schemas IS 'SQL Server sys.schemas, over pg_namespace.';

CREATE VIEW sys.tables AS
  SELECT
    c.relname::name AS name,
    c.oid::int AS object_id,
    c.relnamespace::int AS schema_id,
    'U'::char(2) AS type,
    'USER_TABLE'::varchar(60) AS type_desc,
    NULL::timestamp AS create_date,
    NULL::timestamp AS modify_date
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'sys')
    AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%';
COMMENT ON VIEW sys.tables IS 'SQL Server sys.tables, over pg_class (relkind=''r''). create_date/modify_date are always NULL -- Postgres''s catalog doesn''t track object creation/modification timestamps at all, and fabricating one would be actively misleading.';

CREATE VIEW sys.views AS
  SELECT
    c.relname::name AS name,
    c.oid::int AS object_id,
    c.relnamespace::int AS schema_id,
    'V'::char(2) AS type,
    'VIEW'::varchar(60) AS type_desc,
    NULL::timestamp AS create_date,
    NULL::timestamp AS modify_date
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind = 'v'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'sys')
    AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%';
COMMENT ON VIEW sys.views IS 'SQL Server sys.views, over pg_class (relkind=''v'').';

CREATE VIEW sys.objects AS
  SELECT name, object_id, schema_id, type, type_desc, create_date, modify_date FROM sys.tables
  UNION ALL
  SELECT name, object_id, schema_id, type, type_desc, create_date, modify_date FROM sys.views;
COMMENT ON VIEW sys.objects IS 'SQL Server sys.objects -- tables and views only in this version (real SQL Server also lists constraints, procedures, etc. as objects; not yet covered here).';

CREATE VIEW sys.columns AS
  SELECT
    a.attrelid::int AS object_id,
    a.attname::name AS name,
    a.attnum::int AS column_id,
    t.oid::int AS system_type_id,
    t.oid::int AS user_type_id,
    CASE WHEN a.atttypmod > 0 AND t.typname IN ('varchar', 'bpchar')
         THEN a.atttypmod - 4
         ELSE a.attlen END::int AS max_length,
    CASE WHEN t.typname = 'numeric' AND a.atttypmod > 0
         THEN ((a.atttypmod - 4) >> 16) & 65535 ELSE 0 END::int AS "precision",
    CASE WHEN t.typname = 'numeric' AND a.atttypmod > 0
         THEN (a.atttypmod - 4) & 65535 ELSE 0 END::int AS scale,
    (NOT a.attnotnull) AS is_nullable,
    (a.attidentity <> '') AS is_identity,
    coalesce(ad.oid, 0)::int AS default_object_id
  FROM pg_catalog.pg_attribute a
  JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
  LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
  WHERE a.attnum > 0 AND NOT a.attisdropped
    AND c.relkind IN ('r', 'v')
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'sys')
    AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%';
COMMENT ON VIEW sys.columns IS 'SQL Server sys.columns, over pg_attribute/pg_type. system_type_id/user_type_id are both the Postgres pg_type oid (SQL Server''s own split between "system" and "user" types has no direct Postgres equivalent) -- join sys.types on either to get a name back.';

CREATE VIEW sys.indexes AS
  SELECT
    ix.indrelid::int AS object_id,
    i.relname::name AS name,
    ix.indexrelid::int AS index_id,
    CASE WHEN ix.indisprimary THEN 1 WHEN ix.indisunique THEN 2 ELSE 2 END::int AS type,
    CASE WHEN ix.indisprimary THEN 'CLUSTERED' ELSE 'NONCLUSTERED' END::varchar(60) AS type_desc,
    ix.indisunique AS is_unique,
    ix.indisprimary AS is_primary_key
  FROM pg_catalog.pg_index ix
  JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
  JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'sys')
    AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%';
COMMENT ON VIEW sys.indexes IS 'SQL Server sys.indexes, over pg_index. type/type_desc collapse Postgres''s access methods onto SQL Server''s clustered/nonclustered/heap distinction as a rough approximation -- Postgres has no clustered-index concept the way SQL Server does (CLUSTER is a one-time physical reorder, not an ongoing storage guarantee), so a PRIMARY KEY is reported as CLUSTERED (the common real-world case) and everything else as NONCLUSTERED.';

CREATE VIEW sys.foreign_keys AS
  SELECT
    con.conname::name AS name,
    con.oid::int AS object_id,
    con.conrelid::int AS parent_object_id,
    con.confrelid::int AS referenced_object_id
  FROM pg_catalog.pg_constraint con
  JOIN pg_catalog.pg_namespace n ON n.oid = con.connamespace
  WHERE con.contype = 'f'
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'sys')
    AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%';
COMMENT ON VIEW sys.foreign_keys IS 'SQL Server sys.foreign_keys, over pg_constraint (contype=''f'').';

CREATE VIEW sys.databases AS
  SELECT d.datname::name AS name, d.oid::int AS database_id, NULL::timestamp AS create_date
  FROM pg_catalog.pg_database d
  WHERE NOT d.datistemplate;
COMMENT ON VIEW sys.databases IS 'SQL Server sys.databases, over pg_database. create_date is always NULL -- Postgres doesn''t track database creation time.';

CREATE VIEW sys.types AS
  SELECT t.typname::name AS name, t.oid::int AS system_type_id, t.oid::int AS user_type_id,
         t.typlen::int AS max_length
  FROM pg_catalog.pg_type t
  JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
  WHERE n.nspname = 'pg_catalog' AND t.typtype = 'b';
COMMENT ON VIEW sys.types IS 'SQL Server sys.types, over pg_type (base types in pg_catalog only).';

-- ================================================================
-- System functions -- OBJECT_ID/OBJECT_NAME/DB_NAME/SCHEMA_NAME/COL_NAME
-- (metadata lookups), SCOPE_IDENTITY (session identity tracking, same
-- lastval() trick as pg_mysql's LAST_INSERT_ID -- see that package's own
-- comment for why no session-state tracking of our own is needed),
-- CHARINDEX/STUFF/LEN/PATINDEX/REPLICATE (string functions), IIF.
-- ================================================================

CREATE FUNCTION sys.object_id(p_name text) RETURNS int
LANGUAGE sql STABLE AS $$
  SELECT to_regclass(p_name)::oid::int;
$$;
COMMENT ON FUNCTION sys.object_id(text) IS 'SQL Server OBJECT_ID(name) -- resolves a possibly-schema-qualified name the same way to_regclass does (respects search_path for an unqualified name).';

CREATE FUNCTION sys.object_name(p_object_id int) RETURNS text
LANGUAGE sql STABLE AS $$
  SELECT relname FROM pg_catalog.pg_class WHERE oid = p_object_id;
$$;
COMMENT ON FUNCTION sys.object_name(int) IS 'SQL Server OBJECT_NAME(object_id).';

CREATE FUNCTION sys.db_name() RETURNS text
LANGUAGE sql STABLE AS $$
  SELECT current_database();
$$;
COMMENT ON FUNCTION sys.db_name() IS 'SQL Server DB_NAME().';

CREATE FUNCTION sys.schema_name(p_schema_id int) RETURNS text
LANGUAGE sql STABLE AS $$
  SELECT nspname FROM pg_catalog.pg_namespace WHERE oid = p_schema_id;
$$;
COMMENT ON FUNCTION sys.schema_name(int) IS 'SQL Server SCHEMA_NAME(schema_id).';

CREATE FUNCTION sys.col_name(p_object_id int, p_column_id int) RETURNS text
LANGUAGE sql STABLE AS $$
  SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = p_object_id AND attnum = p_column_id;
$$;
COMMENT ON FUNCTION sys.col_name(int, int) IS 'SQL Server COL_NAME(table_id, column_id).';

CREATE FUNCTION sys.scope_identity() RETURNS numeric
LANGUAGE plpgsql AS $$
BEGIN
  RETURN lastval();
EXCEPTION WHEN OBJECT_NOT_IN_PREREQUISITE_STATE THEN
  RETURN NULL;	-- real SQL Server SCOPE_IDENTITY() returns NULL before any identity insert this session
END;
$$;
COMMENT ON FUNCTION sys.scope_identity() IS 'SQL Server SCOPE_IDENTITY() (and @@IDENTITY, rewritten to this by DialectTranslations.java since Postgres can''t declare a function named with a leading @@) -- thin wrapper over Postgres''s own lastval(), same technique as pg_mysql''s LAST_INSERT_ID(). Real SQL Server distinguishes SCOPE_IDENTITY() (current session+scope) from @@IDENTITY (current session, any scope, including trigger-fired inserts) -- that distinction isn''t preserved here, both map to the same lastval() call.';

CREATE FUNCTION sys.charindex(p_needle text, p_haystack text, p_start integer DEFAULT 1) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN p_needle IS NULL OR p_haystack IS NULL THEN NULL
    WHEN p_start > length(p_haystack) OR p_start < 1 THEN 0
    ELSE (
      SELECT CASE WHEN pos = 0 THEN 0 ELSE pos + p_start - 1 END
      FROM (SELECT position(p_needle IN substring(p_haystack FROM p_start)) AS pos) s
    )
  END;
$$;
COMMENT ON FUNCTION sys.charindex(text, text, integer) IS 'SQL Server CHARINDEX(substring, string, [start]) -- 1-based position, 0 if not found.';

CREATE FUNCTION sys.stuff(p_str text, p_start integer, p_length integer, p_replacement text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN p_str IS NULL OR p_start IS NULL OR p_length IS NULL THEN NULL
    WHEN p_start < 1 OR p_start > length(p_str) THEN NULL	-- matches real SQL Server: out-of-range start returns NULL
    ELSE substring(p_str FROM 1 FOR p_start - 1) || coalesce(p_replacement, '')
           || substring(p_str FROM p_start + p_length)
  END;
$$;
COMMENT ON FUNCTION sys.stuff(text, integer, integer, text) IS 'SQL Server STUFF(str, start, length, replacement) -- deletes length characters starting at start (1-based) and inserts replacement there.';

CREATE FUNCTION sys.len(p_str text) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$
  SELECT length(rtrim(p_str));	-- real SQL Server LEN() excludes trailing spaces; Postgres length() doesn't
$$;
COMMENT ON FUNCTION sys.len(text) IS 'SQL Server LEN(str) -- like Postgres length(), but excludes trailing spaces, matching real SQL Server behavior.';

CREATE FUNCTION sys.patindex(p_pattern text, p_str text) RETURNS integer
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  v_core text;
BEGIN
  IF p_pattern IS NULL OR p_str IS NULL THEN
    RETURN NULL;
  END IF;

  -- The overwhelmingly common real-world shape -- a literal optionally wrapped in a single
  -- leading and/or trailing '%' ('%wor%', 'wor%', '%wor', or a bare literal) -- is handled
  -- exactly via a plain position() search. A naive "does substring(str FROM i) LIKE pattern"
  -- linear scan (an earlier version of this function) gets this shape WRONG: for a pattern
  -- like '%wor%', that test trivially succeeds at i=1 for almost any string, since a leading
  -- '%' in the pattern matches the tail-substring's own leading characters too -- confirmed
  -- live, PATINDEX('%wor%', 'hello world') returned 1 instead of the real answer, 7.
  IF p_pattern ~ '^%?[^%_]*%?$' THEN
    v_core := regexp_replace(p_pattern, '^%?(.*?)%?$', '\1');
    IF v_core = '' THEN
      RETURN CASE WHEN p_str = '' THEN 0 ELSE 1 END;
    END IF;
    RETURN position(v_core IN p_str);	-- 0 if not found, matching PATINDEX's own convention
  END IF;

  -- Any pattern with an embedded '%'/'_' wildcard elsewhere falls back to a linear scan --
  -- NOT fully general (see this function's own header comment): it can still misreport the
  -- leftmost position for a pattern whose OWN leading wildcard is more permissive than a
  -- plain '%', for the same underlying reason the naive version above was wrong. Good enough
  -- for real patterns with one internal wildcard group; not a guaranteed match for every
  -- possible LIKE-style pattern PATINDEX accepts.
  RETURN coalesce((
    SELECT i FROM generate_series(1, length(p_str)) AS i
    WHERE substring(p_str FROM i) LIKE p_pattern
    ORDER BY i LIMIT 1
  ), 0);
END;
$$;
COMMENT ON FUNCTION sys.patindex(text, text) IS 'SQL Server PATINDEX(pattern, str) -- 1-based position of the first substring matching pattern (% and _ wildcards, same as LIKE), 0 if none. Exact for the common %literal%/literal%/%literal shapes; see this function''s own body comment for the fallback''s real limits on more complex patterns.';

CREATE FUNCTION sys.replicate(p_str text, p_count integer) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN p_count < 0 THEN NULL ELSE repeat(p_str, p_count) END;
$$;
COMMENT ON FUNCTION sys.replicate(text, integer) IS 'SQL Server REPLICATE(str, count) -- Postgres''s own repeat(), under SQL Server''s name and NULL-on-negative-count behavior.';

CREATE FUNCTION sys.iif(p_condition boolean, p_true anyelement, p_false anyelement) RETURNS anyelement
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN p_condition THEN p_true ELSE p_false END;
$$;
COMMENT ON FUNCTION sys.iif(boolean, anyelement, anyelement) IS 'SQL Server IIF(condition, true_value, false_value).';

GRANT USAGE ON SCHEMA sys TO PUBLIC;
GRANT SELECT ON sys.schemas, sys.tables, sys.views, sys.objects, sys.columns, sys.indexes, sys.foreign_keys, sys.databases, sys.types TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sys TO PUBLIC;
