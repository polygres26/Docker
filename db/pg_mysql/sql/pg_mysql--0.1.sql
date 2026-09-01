-- pg_mysql -- MySQL compatibility for Postgres (part of the Polygres
-- extension collection). Companion to db/pg_oracle, activated the same
-- way: `SET db_emulation = 'mysql'` appends mysql_catalog onto
-- search_path so unqualified MySQL-shaped function calls (LAST_INSERT_ID(),
-- GROUP_CONCAT(...), DATE_FORMAT(...), ...) resolve without a caller
-- (Warp's mywire frontend, or a human at psql) spelling out
-- mysql_catalog.* by hand -- see db/pg_oracle/src/pg_oracle.c's own
-- header comment for the full mechanism (this extension doesn't
-- reimplement it, just reuses the same GUC and search_path-append logic;
-- see pg_mysql.control for exactly why that lives in pg_oracle's C module
-- and not here).
--
-- Scope: mywire's own SQL-text translation (DialectTranslations.java's
-- normalizeMysql/renderMysql) already rewrites the MySQL syntax that has
-- no valid Postgres function-call shape at all -- backtick identifiers,
-- SHOW TABLES/DATABASES, `LIMIT offset, count` ordering, NVL, sequence
-- NEXTVAL()/LASTVAL() calls. What's left for THIS package is real MySQL
-- functions whose names/signatures don't collide with anything in
-- pg_catalog and so need no schema-qualification-at-translation-time
-- (unlike pg_oracle's TO_CHAR/TO_DATE, which do) -- once mysql_catalog is
-- on search_path, a plain unqualified call just resolves here directly.
-- The one exception is GROUP_CONCAT's `SEPARATOR '...'` clause, which
-- isn't valid Postgres function-call syntax at all -- DialectTranslations
-- rewrites that one specific shape into a plain 2-argument call this
-- package defines, the same "can't express it any other way" reason
-- pg_oracle's own DECODE()/NVL() calls get rewritten in Java rather than
-- left as pure SQL-side overloads.
CREATE SCHEMA mysql_catalog;
COMMENT ON SCHEMA mysql_catalog IS 'MySQL compatibility functions (pg_mysql, part of Polygres). See this extension''s own header comment in sql/pg_mysql--0.1.sql for scope.';

CREATE FUNCTION mysql_catalog.emulation_active() RETURNS boolean
  AS '$libdir/pg_oracle', 'pg_mysql_emulation_active' LANGUAGE C STABLE;
COMMENT ON FUNCTION mysql_catalog.emulation_active() IS 'True when this session has SET db_emulation = ''mysql''. Binds directly against pg_oracle''s shared library -- see pg_mysql.control for why.';

-- ================================================================
-- LAST_INSERT_ID -- Postgres's own lastval() already tracks exactly this:
-- "the value most recently obtained from nextval() in this session",
-- which is precisely what backs every SERIAL/IDENTITY/BIGSERIAL column's
-- auto-increment -- no session-state tracking of our own needed, unlike
-- DBMS_OUTPUT's buffer. The one real gap: lastval() RAISES ("lastval is
-- not yet defined in this session") when no sequence has been touched
-- yet, where real MySQL's LAST_INSERT_ID() just returns 0 -- caught and
-- translated below rather than surfaced as a Postgres-specific error.
-- ================================================================
CREATE FUNCTION mysql_catalog.last_insert_id() RETURNS bigint
LANGUAGE plpgsql AS $$
BEGIN
  RETURN lastval();
EXCEPTION WHEN OBJECT_NOT_IN_PREREQUISITE_STATE THEN
  RETURN 0;	-- real MySQL's own behavior before any auto-increment INSERT this session
END;
$$;
COMMENT ON FUNCTION mysql_catalog.last_insert_id() IS 'MySQL LAST_INSERT_ID() -- thin wrapper over Postgres''s own lastval(), returning 0 (matching MySQL) instead of raising when nothing has been inserted yet this session.';

-- ================================================================
-- IFNULL / FIELD / FIND_IN_SET / SUBSTRING_INDEX -- plain scalar
-- functions with a direct, exact Postgres-expressible equivalent.
-- ================================================================
CREATE FUNCTION mysql_catalog.ifnull(a anyelement, b anyelement) RETURNS anyelement
LANGUAGE sql IMMUTABLE AS $$ SELECT COALESCE(a, b); $$;
COMMENT ON FUNCTION mysql_catalog.ifnull(anyelement, anyelement) IS 'MySQL IFNULL() -- exactly COALESCE with two arguments.';

CREATE FUNCTION mysql_catalog.field(needle text, VARIADIC haystack text[]) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$
  SELECT coalesce((SELECT ord FROM unnest(haystack) WITH ORDINALITY AS t(val, ord)
                     WHERE val = needle LIMIT 1), 0)::integer;
$$;
COMMENT ON FUNCTION mysql_catalog.field(text, text[]) IS 'MySQL FIELD(str, str1, str2, ...) -- 1-based position of str among the following arguments, 0 if not found (or if str is NULL, matching real MySQL).';

CREATE FUNCTION mysql_catalog.find_in_set(needle text, csv text) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$
  SELECT coalesce((SELECT ord FROM unnest(string_to_array(csv, ',')) WITH ORDINALITY AS t(val, ord)
                     WHERE val = needle LIMIT 1), 0)::integer;
$$;
COMMENT ON FUNCTION mysql_catalog.find_in_set(text, text) IS 'MySQL FIND_IN_SET(str, strlist) -- 1-based position of str within a comma-separated list, 0 if not found.';

CREATE FUNCTION mysql_catalog.substring_index(str text, delim text, cnt integer) RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  v_parts text[];
  v_n     integer;
BEGIN
  IF str IS NULL OR delim IS NULL OR cnt IS NULL OR delim = '' THEN
    RETURN str;
  END IF;
  v_parts := string_to_array(str, delim);
  v_n := array_length(v_parts, 1);
  IF cnt >= 0 THEN
    RETURN array_to_string(v_parts[1 : LEAST(cnt, v_n)], delim);
  ELSE
    RETURN array_to_string(v_parts[GREATEST(v_n + cnt + 1, 1) : v_n], delim);
  END IF;
END;
$$;
COMMENT ON FUNCTION mysql_catalog.substring_index(text, text, integer) IS 'MySQL SUBSTRING_INDEX(str, delim, count) -- everything before the count-th delimiter (count > 0, counting from the left) or after the |count|-th delimiter counting from the right (count < 0).';

-- ================================================================
-- UNIX_TIMESTAMP / FROM_UNIXTIME / RAND -- direct Postgres equivalents,
-- just under MySQL's own names for callers that use them unqualified.
-- ================================================================
CREATE FUNCTION mysql_catalog.unix_timestamp() RETURNS bigint
LANGUAGE sql STABLE AS $$ SELECT extract(epoch FROM now())::bigint; $$;
COMMENT ON FUNCTION mysql_catalog.unix_timestamp() IS 'MySQL UNIX_TIMESTAMP() -- current time as epoch seconds.';

CREATE FUNCTION mysql_catalog.unix_timestamp(ts timestamp) RETURNS bigint
LANGUAGE sql IMMUTABLE AS $$ SELECT extract(epoch FROM ts)::bigint; $$;
COMMENT ON FUNCTION mysql_catalog.unix_timestamp(timestamp) IS 'MySQL UNIX_TIMESTAMP(date) -- a specific timestamp as epoch seconds.';

-- A single bigint-typed overload, not also a numeric/double-precision one
-- for fractional seconds -- a plain integer literal argument (the
-- overwhelming common case) is ambiguous between bigint and numeric (both
-- reachable via an implicit cast from integer), and Postgres rejects the
-- call outright rather than guessing -- confirmed live. A caller with a
-- genuine fractional epoch value can still get there with an explicit
-- cast to timestamp arithmetic (`to_timestamp(1767225600.5)`) directly.
CREATE FUNCTION mysql_catalog.from_unixtime(epoch bigint) RETURNS timestamp
LANGUAGE sql IMMUTABLE AS $$ SELECT to_timestamp(epoch)::timestamp; $$;
COMMENT ON FUNCTION mysql_catalog.from_unixtime(bigint) IS 'MySQL FROM_UNIXTIME(unix_timestamp).';

CREATE FUNCTION mysql_catalog.rand() RETURNS double precision
LANGUAGE sql VOLATILE AS $$ SELECT random(); $$;
COMMENT ON FUNCTION mysql_catalog.rand() IS 'MySQL RAND() -- Postgres''s own random(), under MySQL''s name.';

-- ================================================================
-- DATE_FORMAT / STR_TO_DATE -- MySQL's %-prefixed format specifiers
-- translated into Postgres's to_char/to_timestamp pattern language, then
-- handed straight to the real, native implementation. Covers the common,
-- well-established specifiers (year/month/day/hour/minute/second, plus
-- weekday/month names and 12h/AM-PM forms) -- NOT an exhaustive port of
-- every MySQL specifier (the various %U/%u/%V/%v/%X/%x week-of-year
-- variants in particular have no clean Postgres equivalent -- ISO week
-- numbering differs in ways that would be actively misleading to fake --
-- and are left untranslated rather than silently wrong).
-- ================================================================
CREATE FUNCTION mysql_catalog.mysql_format_to_pg(fmt text) RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  v_sentinel constant text := chr(1);	-- placeholder for a literal '%' (MySQL's %%), vanishingly unlikely to appear in a real format string
  v_out text := fmt;
  v_map text[] := ARRAY[
    '%a','Dy',    '%b','Mon',   '%c','FMMM',  '%d','DD',    '%e','FMDD',
    '%H','HH24',  '%h','HH12',  '%I','HH12',  '%i','MI',    '%j','DDD',
    '%k','FMHH24','%l','FMHH12','%M','FMMonth','%m','MM',
    '%p','AM',    '%r','HH12:MI:SS AM', '%S','SS', '%s','SS',
    '%T','HH24:MI:SS', '%W','FMDay', '%Y','YYYY', '%y','YY'
  ];
  i integer;
BEGIN
  v_out := replace(v_out, '%%', v_sentinel);
  FOR i IN 1 .. array_length(v_map, 1) BY 2 LOOP
    v_out := replace(v_out, v_map[i], v_map[i + 1]);
  END LOOP;
  RETURN replace(v_out, v_sentinel, '%');
END;
$$;
COMMENT ON FUNCTION mysql_catalog.mysql_format_to_pg(text) IS 'Internal: translates MySQL DATE_FORMAT/STR_TO_DATE %-specifiers into Postgres to_char/to_timestamp pattern syntax. Not part of the MySQL-compatible API -- use date_format()/str_to_date().';

CREATE FUNCTION mysql_catalog.date_format(ts timestamp, fmt text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT to_char(ts, mysql_catalog.mysql_format_to_pg(fmt));
$$;
COMMENT ON FUNCTION mysql_catalog.date_format(timestamp, text) IS 'MySQL DATE_FORMAT(date, format).';

CREATE FUNCTION mysql_catalog.str_to_date(str text, fmt text) RETURNS timestamp
LANGUAGE sql IMMUTABLE AS $$
  SELECT to_timestamp(str, mysql_catalog.mysql_format_to_pg(fmt))::timestamp;
$$;
COMMENT ON FUNCTION mysql_catalog.str_to_date(text, text) IS 'MySQL STR_TO_DATE(str, format).';

-- ================================================================
-- GROUP_CONCAT -- a real Postgres CREATE AGGREGATE, not a scalar
-- function: MySQL's `GROUP_CONCAT(expr SEPARATOR ',')` clause syntax has
-- no valid plain-function-call form at all, so DialectTranslations.java
-- rewrites that specific shape into a call to the 2-argument form below
-- (value, separator); a bare `GROUP_CONCAT(expr)` (default comma
-- separator, ordinary call syntax) needs no rewriting and resolves here
-- directly once mysql_catalog is on search_path. ORDER BY inside
-- GROUP_CONCAT is not supported (real MySQL: `GROUP_CONCAT(x ORDER BY y
-- SEPARATOR ',')`) -- Postgres's own `x ORDER BY y` inside an aggregate
-- call is valid syntax already (WITHIN GROUP-adjacent aggregate ORDER BY),
-- but DialectTranslations doesn't yet parse that shape out of the
-- SEPARATOR clause; a real, stated limitation, not a silent
-- mistranslation (an ORDER BY clause left in place errors loudly at the
-- Postgres level rather than silently reordering wrong).
-- ================================================================
CREATE FUNCTION mysql_catalog.group_concat_finalfn(state text[]) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT array_to_string(state, ','); $$;
COMMENT ON FUNCTION mysql_catalog.group_concat_finalfn(text[]) IS 'Internal: FINALFUNC for mysql_catalog.group_concat(text). Not part of the MySQL-compatible API.';

CREATE AGGREGATE mysql_catalog.group_concat(text) (
  sfunc = array_append,
  stype = text[],
  finalfunc = mysql_catalog.group_concat_finalfn,
  initcond = '{}'
);
COMMENT ON AGGREGATE mysql_catalog.group_concat(text) IS 'MySQL GROUP_CONCAT(expr) -- default comma separator.';

CREATE TYPE mysql_catalog.group_concat_state AS (vals text[], sep text);

CREATE FUNCTION mysql_catalog.group_concat_sfunc(state mysql_catalog.group_concat_state, val text, sep text)
RETURNS mysql_catalog.group_concat_state
LANGUAGE sql IMMUTABLE AS $$
  SELECT ROW(array_append(COALESCE(state.vals, '{}'), val), COALESCE(state.sep, sep))::mysql_catalog.group_concat_state;
$$;
COMMENT ON FUNCTION mysql_catalog.group_concat_sfunc(mysql_catalog.group_concat_state, text, text) IS 'Internal: SFUNC for mysql_catalog.group_concat(text, text). Not part of the MySQL-compatible API.';

CREATE FUNCTION mysql_catalog.group_concat_finalfn2(state mysql_catalog.group_concat_state) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT array_to_string(state.vals, COALESCE(state.sep, ',')); $$;
COMMENT ON FUNCTION mysql_catalog.group_concat_finalfn2(mysql_catalog.group_concat_state) IS 'Internal: FINALFUNC for mysql_catalog.group_concat(text, text). Not part of the MySQL-compatible API.';

CREATE AGGREGATE mysql_catalog.group_concat(text, text) (
  sfunc = mysql_catalog.group_concat_sfunc,
  stype = mysql_catalog.group_concat_state,
  finalfunc = mysql_catalog.group_concat_finalfn2
);
COMMENT ON AGGREGATE mysql_catalog.group_concat(text, text) IS 'MySQL GROUP_CONCAT(expr SEPARATOR sep) -- explicit separator, as translated by DialectTranslations.java from the real SEPARATOR clause syntax.';

GRANT USAGE ON SCHEMA mysql_catalog TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA mysql_catalog TO PUBLIC;

-- ================================================================
-- SHOW COLUMNS / DESCRIBE, SHOW INDEX, SHOW VARIABLES, SHOW CREATE TABLE --
-- MySQL's introspection commands, the single highest-hit-rate real gap
-- once basic function compatibility (LAST_INSERT_ID, GROUP_CONCAT, etc.)
-- works: `mysqldump`, MySQL Workbench, phpMyAdmin, and most ORMs'
-- introspection (Sequelize, Django, TypeORM) issue these constantly, the
-- same reason SHOW TABLES/SHOW DATABASES were already worth translating
-- first. DialectTranslations.java rewrites each SHOW ... shape into a
-- plain SELECT against one of the table-valued functions below -- this
-- package holds the actual logic (real joins across pg_catalog/
-- information_schema, not expressible as a single flat regex
-- substitution the way SHOW TABLES/SHOW DATABASES were).
--
-- Real, stated fidelity limits, not hidden: MySQL's own SHOW COLUMNS
-- "Type"/SHOW CREATE TABLE column-type text is genuinely MySQL type
-- syntax (`int(11)`, `varchar(50) CHARACTER SET utf8mb4`) -- what's
-- returned here is a best-effort MySQL-shaped approximation from
-- Postgres's own type system (see mysql_shaped_type_name() below), close
-- enough for a human or a tool that only checks "is there a column named
-- X of roughly type Y", not a byte-for-byte match a strict schema-diff
-- tool would accept. SHOW CREATE TABLE similarly reconstructs a
-- plausible CREATE TABLE statement (columns + PRIMARY KEY), not
-- MySQL-specific table options (ENGINE=, CHARSET=, AUTO_INCREMENT=n).
-- ================================================================

CREATE FUNCTION mysql_catalog.mysql_shaped_type_name(
  p_data_type text, p_char_len integer, p_num_precision integer, p_num_scale integer
) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE p_data_type
    WHEN 'character varying' THEN 'varchar(' || coalesce(p_char_len, 255) || ')'
    WHEN 'character' THEN 'char(' || coalesce(p_char_len, 1) || ')'
    WHEN 'text' THEN 'text'
    WHEN 'numeric' THEN 'decimal(' || coalesce(p_num_precision, 10) || ',' || coalesce(p_num_scale, 0) || ')'
    WHEN 'integer' THEN 'int(11)'
    WHEN 'bigint' THEN 'bigint(20)'
    WHEN 'smallint' THEN 'smallint(6)'
    WHEN 'boolean' THEN 'tinyint(1)'
    WHEN 'real' THEN 'float'
    WHEN 'double precision' THEN 'double'
    WHEN 'timestamp without time zone' THEN 'datetime'
    WHEN 'timestamp with time zone' THEN 'timestamp'
    WHEN 'date' THEN 'date'
    WHEN 'uuid' THEN 'char(36)'
    WHEN 'bytea' THEN 'blob'
    WHEN 'jsonb' THEN 'json'
    WHEN 'json' THEN 'json'
    ELSE p_data_type
  END;
$$;
COMMENT ON FUNCTION mysql_catalog.mysql_shaped_type_name(text, integer, integer, integer) IS 'Internal: best-effort MySQL-shaped type name for SHOW COLUMNS/SHOW CREATE TABLE -- see this section''s header comment for real fidelity limits. Not part of the MySQL-compatible API.';

CREATE FUNCTION mysql_catalog.show_columns(p_table text)
RETURNS TABLE("Field" text, "Type" text, "Null" text, "Key" text, "Default" text, "Extra" text)
LANGUAGE sql STABLE AS $$
  SELECT
    c.column_name,
    mysql_catalog.mysql_shaped_type_name(c.data_type, c.character_maximum_length, c.numeric_precision, c.numeric_scale),
    CASE WHEN c.is_nullable = 'YES' THEN 'YES' ELSE 'NO' END,
    coalesce((
      SELECT CASE tc.constraint_type
               WHEN 'PRIMARY KEY' THEN 'PRI'
               WHEN 'UNIQUE' THEN 'UNI'
               ELSE 'MUL'
             END
      FROM information_schema.key_column_usage kcu
      JOIN information_schema.table_constraints tc
        ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
      WHERE kcu.table_schema = c.table_schema AND kcu.table_name = c.table_name AND kcu.column_name = c.column_name
      ORDER BY CASE tc.constraint_type WHEN 'PRIMARY KEY' THEN 1 WHEN 'UNIQUE' THEN 2 ELSE 3 END
      LIMIT 1
    ), ''),
    c.column_default,
    CASE WHEN c.column_default LIKE 'nextval(%' THEN 'auto_increment' ELSE '' END
  FROM information_schema.columns c
  WHERE c.table_name = p_table
    AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
    AND c.table_schema = ANY (current_schemas(false))
  ORDER BY c.ordinal_position;
$$;
COMMENT ON FUNCTION mysql_catalog.show_columns(text) IS 'Backs MySQL SHOW COLUMNS FROM/DESCRIBE/DESC (see DialectTranslations.java''s rewrite). See this section''s header comment for Type fidelity limits.';

CREATE FUNCTION mysql_catalog.show_index(p_table text)
RETURNS TABLE("Table" text, "Non_unique" integer, "Key_name" text, "Seq_in_index" integer,
              "Column_name" text, "Collation" text, "Cardinality" bigint, "Sub_part" integer,
              "Packed" text, "Null" text, "Index_type" text, "Comment" text, "Index_comment" text)
LANGUAGE sql STABLE AS $$
  SELECT
    t.relname,
    (NOT ix.indisunique)::integer,
    i.relname,
    k.ord::integer,
    a.attname,
    'A',
    NULL::bigint,
    NULL::integer,
    NULL::text,
    CASE WHEN a.attnotnull THEN '' ELSE 'YES' END,
    am.amname,
    '',
    ''
  FROM pg_catalog.pg_index ix
  JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
  JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
  JOIN pg_catalog.pg_am am ON am.oid = i.relam
  JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
  JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
  JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
  WHERE t.relname = p_table
    AND n.nspname = ANY (current_schemas(false))
  ORDER BY i.relname, k.ord;
$$;
COMMENT ON FUNCTION mysql_catalog.show_index(text) IS 'Backs MySQL SHOW INDEX FROM (see DialectTranslations.java''s rewrite). Cardinality/Sub_part/Packed are always NULL -- Postgres''s planner statistics don''t map onto MySQL''s per-index cardinality estimate the same way, and fabricating a number would be actively misleading (same principle as DBMS_STATS''s own honest gaps in db/pg_oracle).';

CREATE FUNCTION mysql_catalog.show_variables() RETURNS TABLE("Variable_name" text, "Value" text)
LANGUAGE sql STABLE AS $$
  SELECT * FROM (VALUES
    ('version', current_setting('server_version') || '-warp'),
    ('version_comment', 'Warp mywire (Postgres compatibility)'),
    ('character_set_client', 'utf8mb4'),
    ('character_set_connection', 'utf8mb4'),
    ('character_set_results', 'utf8mb4'),
    ('character_set_server', 'utf8mb4'),
    ('collation_connection', 'utf8mb4_general_ci'),
    ('collation_server', 'utf8mb4_general_ci'),
    ('sql_mode', ''),
    ('autocommit', 'ON'),
    ('time_zone', current_setting('TimeZone')),
    ('max_allowed_packet', '67108864'),
    ('wait_timeout', '28800')
  ) AS v(name, value)
$$;
COMMENT ON FUNCTION mysql_catalog.show_variables() IS 'Backs MySQL SHOW VARIABLES [LIKE ...] (see DialectTranslations.java''s rewrite). A small, curated, mostly-static set covering what tools/ORMs actually probe at connect time -- not an exhaustive port of MySQL''s hundreds of real variables.';

CREATE FUNCTION mysql_catalog.show_create_table(p_table text)
RETURNS TABLE("Table" text, "Create Table" text)
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_schema text;
  v_cols text;
  v_pk text;
  v_body text;
BEGIN
  SELECT c.table_schema INTO v_schema
  FROM information_schema.columns c
  WHERE c.table_name = p_table
    AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
    AND c.table_schema = ANY (current_schemas(false))
  LIMIT 1;

  IF v_schema IS NULL THEN
    RETURN;	-- no such table on search_path -- caller sees an empty result, same as a real miss
  END IF;

  SELECT string_agg(
           '  `' || c.column_name || '` '
             || mysql_catalog.mysql_shaped_type_name(c.data_type, c.character_maximum_length, c.numeric_precision, c.numeric_scale)
             || CASE WHEN c.is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END
             || CASE WHEN c.column_default LIKE 'nextval(%' THEN ' AUTO_INCREMENT' ELSE '' END,
           E',\n' ORDER BY c.ordinal_position)
    INTO v_cols
  FROM information_schema.columns c
  WHERE c.table_schema = v_schema AND c.table_name = p_table;

  SELECT '  PRIMARY KEY (' || string_agg('`' || kcu.column_name || '`', ',' ORDER BY kcu.ordinal_position) || ')'
    INTO v_pk
  FROM information_schema.table_constraints tc
  JOIN information_schema.key_column_usage kcu
    ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
  WHERE tc.table_schema = v_schema AND tc.table_name = p_table AND tc.constraint_type = 'PRIMARY KEY';

  v_body := 'CREATE TABLE `' || p_table || '` (' || E'\n' || v_cols
            || CASE WHEN v_pk IS NOT NULL THEN ',' || E'\n' || v_pk ELSE '' END
            || E'\n)';

  "Table" := p_table;
  "Create Table" := v_body;
  RETURN NEXT;
END;
$$;
COMMENT ON FUNCTION mysql_catalog.show_create_table(text) IS 'Backs MySQL SHOW CREATE TABLE (see DialectTranslations.java''s rewrite). Reconstructs columns + PRIMARY KEY only -- no ENGINE=/CHARSET=/AUTO_INCREMENT=n table options, no secondary-index DDL. See this section''s header comment for the full fidelity statement.';

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA mysql_catalog TO PUBLIC;
