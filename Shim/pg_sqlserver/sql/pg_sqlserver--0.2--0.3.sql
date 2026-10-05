-- pg_sqlserver 0.2 -> 0.3: sys.iif() resolves its arguments like T-SQL, via anycompatible.
--
-- 0.1 declared iif(boolean, anyelement, anyelement); 0.2 added a text overload so IIF(c, 'a', 'b') (two untyped
-- literals) worked. Both still failed for arguments of different types (IIF(c, 1.5, 2) -- anyelement needs one
-- type), and a numeric overload to fix that made IIF(c, 1, 2) ambiguous. anycompatible (PostgreSQL 13+) is built
-- for this: the arguments are resolved to a common type the way CASE does it. Checked on PostgreSQL 17:
--   IIF(c, 'a', 'b')            -> text            IIF(c, 1, 2)              -> integer
--   IIF(c, 1.5, 2)              -> numeric         IIF(c, 1::bigint, 2)      -> bigint
--   IIF(c, current_date, now()) -> timestamptz     IIF(c, NULL, NULL)        -> text (NULL)
--   IIF(c, 'a', 1) raises "invalid input syntax for type integer", as SQL Server's implicit conversion does.
--
-- Requires PostgreSQL 13 or later. The two older signatures are dropped; an object that depends on one of them (a
-- view or function you wrote that calls sys.iif) makes the DROP fail, so recreate it after the upgrade.

DROP FUNCTION sys.iif(boolean, text, text);
DROP FUNCTION sys.iif(boolean, anyelement, anyelement);

CREATE FUNCTION sys.iif(p_condition boolean, p_true anycompatible, p_false anycompatible) RETURNS anycompatible
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN p_condition THEN p_true ELSE p_false END;
$$;
COMMENT ON FUNCTION sys.iif(boolean, anycompatible, anycompatible) IS 'SQL Server IIF(condition, true_value, false_value); the two values are resolved to a common type like CASE (PostgreSQL 13+).';

GRANT EXECUTE ON FUNCTION sys.iif(boolean, anycompatible, anycompatible) TO PUBLIC;
