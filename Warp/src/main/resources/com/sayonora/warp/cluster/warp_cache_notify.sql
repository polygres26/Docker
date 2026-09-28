-- The two plpgsql trigger functions behind Warp's out-of-band result/row-cache invalidation
-- (WARP_CACHE_INVALIDATION=listen|notify -- see CacheTriggerInstaller and
-- CacheInvalidationListener). ONE source for both the installer (which executes these) and the
-- `--print-cache-triggers` CLI (which prints them for an operator to apply by hand), so the two
-- can never drift apart. Each statement is delimited by a `-- ### <label>` marker line, the
-- same convention DdlTemplates uses for ddl/<engine>/*.sql.
--
-- Both functions emit exactly ONE pg_notify per statement on channel warp_cache_invalidate,
-- with a JSON payload:
--   {"v":1,"schema":"public","table":"orders","op":"UPDATE"}                       (table-level)
--   {"v":1,"schema":"public","table":"dynamo_item_orders","op":"UPDATE",
--    "keys":[["pk1","sk1"],["pk2",null]]}                                           (rows-level)
-- A NOTIFY payload over 8000 bytes ABORTS THE WRITER'S TRANSACTION -- so the rows-level
-- function measures its payload BEFORE calling pg_notify and degrades to "keys":null (which the
-- listener treats as a whole-table invalidation) rather than ever letting a large UPDATE fail
-- because Warp's cache happened to be watching the table.
--
-- SECURITY INVOKER (the default) on purpose: the function runs as whoever performed the write,
-- needs nothing beyond reading its own transition tables, and must never be a privilege step.

-- ### stmt_function
CREATE OR REPLACE FUNCTION warp_cache_notify_stmt() RETURNS trigger
LANGUAGE plpgsql AS $warp$
BEGIN
    PERFORM pg_notify('warp_cache_invalidate', json_build_object(
        'v', 1,
        'schema', TG_TABLE_SCHEMA,
        'table', TG_TABLE_NAME,
        'op', TG_OP)::text);
    RETURN NULL;
END
$warp$

-- ### rows_function
CREATE OR REPLACE FUNCTION warp_cache_notify_rows() RETURNS trigger
LANGUAGE plpgsql AS $warp$
DECLARE
    -- TG_ARGV[0] is the partition/primary-key column, TG_ARGV[1] the optional sort-key column
    -- (dynamowire's (pk_value, sk_value); mongowire's (id); otherwise the table's own PRIMARY
    -- KEY -- decided at install time by CacheTriggerInstaller, not here).
    pk_col text := TG_ARGV[0];
    sk_col text := CASE WHEN TG_NARGS > 1 THEN TG_ARGV[1] ELSE NULL END;
    key_expr text;
    src text;
    row_count bigint;
    keys jsonb;
    payload text;
BEGIN
    -- Postgres refuses transition tables on a multi-event trigger ("transition tables cannot be
    -- specified for triggers with more than one event"), so the installer creates one trigger
    -- per DML event, each pointing at this one function, and only the transition table(s) that
    -- event actually declares are referenced below. An UPDATE that changes a key column touches
    -- two cache entries (old key and new key), hence the UNION ALL.
    -- jsonb, not json: DISTINCT below needs an equality operator, and json has none ("could not
    -- identify an equality operator for type json" -- hit on the first real run).
    IF sk_col IS NULL THEN
        key_expr := format('jsonb_build_array(%I::text, NULL::text)', pk_col);
    ELSE
        key_expr := format('jsonb_build_array(%I::text, %I::text)', pk_col, sk_col);
    END IF;
    IF TG_OP = 'INSERT' THEN
        src := 'SELECT * FROM new_rows';
    ELSIF TG_OP = 'DELETE' THEN
        src := 'SELECT * FROM old_rows';
    ELSE
        src := 'SELECT * FROM old_rows UNION ALL SELECT * FROM new_rows';
    END IF;
    -- LIMIT 201, not a full count: a 10k-row UPDATE must cost this trigger 201 rows of work,
    -- not 10k -- anything past the 200-key cap degrades to a table-level notify anyway.
    EXECUTE format('SELECT count(*), jsonb_agg(k) FROM (SELECT DISTINCT %s AS k FROM (%s) u LIMIT 201) s',
                   key_expr, src)
        INTO row_count, keys;
    IF row_count > 200 THEN
        keys := NULL;
    END IF;
    payload := json_build_object(
        'v', 1,
        'schema', TG_TABLE_SCHEMA,
        'table', TG_TABLE_NAME,
        'op', TG_OP,
        'keys', keys)::text;
    -- 7000, not 8000: leaves headroom under pg_notify's hard 8000-byte limit for the
    -- schema/table names themselves, whatever their length.
    IF octet_length(payload) > 7000 THEN
        payload := json_build_object(
            'v', 1,
            'schema', TG_TABLE_SCHEMA,
            'table', TG_TABLE_NAME,
            'op', TG_OP,
            'keys', NULL)::text;
    END IF;
    PERFORM pg_notify('warp_cache_invalidate', payload);
    RETURN NULL;
END
$warp$
