-- pg_sqlserver 0.1 -> 0.2: sys.iif() with untyped string literals.
--
-- 0.1 declared only iif(boolean, anyelement, anyelement). Postgres cannot infer a polymorphic type from two
-- untyped string literals, so the common T-SQL call IIF(cond, 'yes', 'no') failed with "could not determine
-- polymorphic type because input has type unknown" (real SQL Server has no such restriction).
--
-- The text overload below takes over exactly that case (text is the preferred type of the string category, so
-- unknown literals resolve to it). Calls with two arguments of the same type still resolve to the anyelement
-- version unchanged.
--
-- Known remaining limit, unchanged from 0.1: arguments of DIFFERENT non-string types, e.g. IIF(c, 1.5, 2),
-- still fail because anyelement needs one type. A numeric overload was tried and rejected: it makes the common
-- IIF(c, 1, 2) ambiguous ("function sys.iif(boolean, integer, integer) is not unique").

CREATE FUNCTION sys.iif(p_condition boolean, p_true text, p_false text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN p_condition THEN p_true ELSE p_false END;
$$;
COMMENT ON FUNCTION sys.iif(boolean, text, text) IS 'SQL Server IIF(condition, true_value, false_value) for string arguments, including untyped literals.';

GRANT EXECUTE ON FUNCTION sys.iif(boolean, text, text) TO PUBLIC;
