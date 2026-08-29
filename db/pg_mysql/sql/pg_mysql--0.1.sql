-- pg_mysql -- MySQL compatibility for Postgres (part of the Polygres
-- extension collection). Companion to db/pg_oracle, activated the same
-- way: `SET db_emulation = 'mysql'` appends mysql_catalog onto
-- search_path so unqualified MySQL-shaped function calls (LAST_INSERT_ID(),
-- GROUP_CONCAT(...), DATE_FORMAT(...), ...) resolve without a caller
-- (Polywire's mywire frontend, or a human at psql) spelling out
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
