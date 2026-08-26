-- PolyWire: optional Oracle built-in function compatibility shims for Postgres.
--
-- PolyWire never modifies your backend's schema on its own -- install this file once, by hand,
-- only if your own application queries (or a real Oracle client's own internal behavior, like
-- SQL*Plus's connection-banner probe) call an Oracle built-in function Postgres doesn't have.
-- PolyWire's dialect translation rewrites the *shape* of a query (table/column references,
-- SYS.DUAL, etc.) but does not invent missing Oracle built-in *functions* on your behalf --
-- that's what this file is for.
--
-- This is the running reference for every Oracle built-in this project has hit and shimmed so
-- far. Add to it (don't create a new one-off file) the next time a real client or a real
-- customer query calls an Oracle-supplied function Postgres has no equivalent for -- same
-- pattern every time: confirm it's a genuine Oracle built-in via a real capture or Oracle's own
-- docs, implement the real semantics (not just enough to satisfy one specific call), and add it
-- here with a comment explaining what real thing sends it and why.
--
--   psql -h <host> -p <port> -U <user> -d <database> -f oracle_compat_functions.sql

-- Oracle DECODE(expr, search1, result1, search2, result2, ..., default) -- NULL-safe equality
-- comparison (unlike a plain CASE/=, which never matches NULL to NULL, DECODE treats NULL as
-- equal to NULL). Real Oracle clients and applications use this constantly; SQL*Plus's own
-- connection-banner probe (see XS_SYS_CONTEXT below) is the one call this codebase has actually
-- captured live, but the implementation here is the real, general N-argument form, not scoped to
-- that one call shape.
CREATE OR REPLACE FUNCTION decode(VARIADIC args text[])
RETURNS text AS $$
DECLARE
  i int;
BEGIN
  FOR i IN 1..(array_length(args, 1) - 1) BY 2 LOOP
    IF args[1] IS NOT DISTINCT FROM args[i + 1] THEN
      RETURN args[i + 2];
    END IF;
  END LOOP;
  IF array_length(args, 1) % 2 = 0 THEN
    RETURN args[array_length(args, 1)];
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Oracle's XS (Real Application Security) namespace accessor, XS_SYS_CONTEXT(namespace,
-- parameter). Confirmed live: a real SQL*Plus client calls
-- XS_SYS_CONTEXT('XS$SESSION', 'USERNAME') as part of its own internal startup probe
-- (`SELECT DECODE(USER, 'XS$NULL', XS_SYS_CONTEXT('XS$SESSION','USERNAME'), USER) FROM SYS.DUAL`,
-- run automatically right after login to populate its own "Connected to ... as USER" banner
-- line) -- PolyWire already recognizes and rewrites that exact query internally (see
-- DualTableRewriter.SQLPLUS_STARTUP_USER_PROBE), so a stock SQL*Plus session needs none of this
-- file to work. This function exists for application code that calls XS_SYS_CONTEXT directly,
-- for any other namespace/parameter pair. Only the one pair with real, known meaning here is
-- implemented (the connected user); any other pair returns NULL rather than raising, matching
-- how a real Oracle instance without XS Administrator actually configured behaves for an
-- unrecognized pair -- not a guess at every possible XS namespace value.
CREATE OR REPLACE FUNCTION xs_sys_context(namespace text, parameter text)
RETURNS text AS $$
BEGIN
  IF upper(namespace) = 'XS$SESSION' AND upper(parameter) = 'USERNAME' THEN
    RETURN current_user;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE;
