# oracle SQL-path matrix report

## Matrix: client x path (counts)
| client | path | PASS | KNOWN_DIFF | WARP_BUG | CLIENT_ISSUE | FLAKY | SKIP | ERROR | total |
|---|---|---|---|---|---|---|---|---|---|
| python | adapt | 2 | 11 | 7 | 0 | 0 | 0 | 0 | 20 |
| python | relay | 69 | 1 | 0 | 0 | 0 | 0 | 0 | 70 |

## Per-category breakdown
(see JSON for full per-scenario detail; category grouping is by scenario id prefix here for brevity)

## WARP_BUG / candidate bugs (7)
### ora006 / python / adapt
- Diff: ok: native=True other=False; error: native=(None,None) other=(ORA-00942,'statement cannot be translated from ORACLE to POSTGRES (needs manual migration): LLM fallback translator failed: null -- original SQL: SELECT CAST(123.456 AS NUMBER(10,2)) FROM (select 1) dual_placeholder Help: https://docs.oracle.com/error-help/db/ora-00942/')
- Repro: client=python path=adapt sql=`SELECT CAST(123.456 AS NUMBER(10,2)) FROM DUAL`
- Severity: LOW-MEDIUM (ADAPT dialect-translation gap)

### ora008 / python / adapt
- Diff: ok: native=True other=False; error: native=(None,None) other=(ORA-00942,'ERROR: type "binary_double" does not exist Position: 20 Help: https://docs.oracle.com/error-help/db/ora-00942/')
- Repro: client=python path=adapt sql=`SELECT CAST(3.5 AS BINARY_DOUBLE) FROM DUAL`
- Severity: LOW-MEDIUM (ADAPT dialect-translation gap)

### ora009 / python / adapt
- Diff: columns: native=["CAST('AB'ASCHAR(5))||'|'"] other=["CAST('AB' AS CHAR(5)) || '|'"]; rows: native=[['ab |']] other=[['ab|']]
- Repro: client=python path=adapt sql=`SELECT CAST('ab' AS CHAR(5)) || '|' FROM DUAL`
- Severity: LOW-MEDIUM (ADAPT dialect-translation gap)

### ora011 / python / adapt
- Diff: columns: native=["TO_CHAR(TIMESTAMP'2026-01-1510:00:00'ATTIMEZONE'UTC','YYYY-MM-DDHH24:MI:SSTZR')"] other=['TO_CHAR']; rows: native=[['2026-01-15 17:00:00 UTC']] other=[['2026-01-15 02:00:00 PSTR']]
- Repro: client=python path=adapt sql=`SELECT TO_CHAR(TIMESTAMP '2026-01-15 10:00:00' AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS TZR') FROM DUAL`
- Severity: LOW-MEDIUM (ADAPT dialect-translation gap)

### ora013 / python / adapt
- Diff: ok: native=True other=False; error: native=(None,None) other=(ORA-00904,'ORA-00904: "hextoraw(unknown)": invalid identifier Help: https://docs.oracle.com/error-help/db/ora-00904/')
- Repro: client=python path=adapt sql=`SELECT RAWTOHEX(HEXTORAW('DEADBEEF')) FROM DUAL`
- Severity: LOW-MEDIUM (ADAPT dialect-translation gap)

### ora018 / python / adapt
- Diff: ok: native=True other=False; error: native=(None,None) other=(ADAPTER_EXCEPTION,'could not locate a SQL statement in this Execute request by scanning (fallback for a real dblink native-OCI client\'s non-JDBC-shaped request)\n/site-packages/oracledb/cursor.py", line 859, in execute\n    impl.execute(self)\n  File "src/oracledb/impl/thin/cursor.pyx", line 279, in oracledb.thin_impl.ThinCursorImpl.execute\n  File "src/oracledb/impl/thin/protocol.pyx", line 501, in oracledb.thin_impl.Protocol._process_single_message\n  File "src/oracledb/impl/thin/protocol.pyx", line 502, in oracledb.thin_impl.Protocol._process_single_message\n  File "src/oracledb/impl/thin/protocol.pyx", line 494, in oracledb.thin_impl.Protocol._process_message\n  File "src/oracledb/impl/thin/messages/base.pyx", line 102, in oracledb.thin_impl.Message._check_and_raise_exception\noracledb.exceptions.DatabaseError: could not locate a SQL statement in this Execute request by scanning (fallback for a real dblink native-OCI client\'s non-JDBC-shaped request)\n')
- Repro: client=python path=adapt sql=`ALTER TABLE sp_t2 ADD (extra VARCHAR2(10))`
- Severity: LOW-MEDIUM (ADAPT dialect-translation gap)

### ora020 / python / adapt
- Diff: ok: native=True other=False; error: native=(None,None) other=(ADAPTER_EXCEPTION,'statement cannot be translated from ORACLE to POSTGRES (needs manual migration): LLM fallback translator failed: null -- original SQL: CREATE SEQUENCE sp_seq1 START WITH 1\nHelp: https://docs.oracle.com/error-help/db/ora-00942/\ne "src/oracledb/impl/thin/cursor.pyx", line 279, in oracledb.thin_impl.ThinCursorImpl.execute\n  File "src/oracledb/impl/thin/protocol.pyx", line 501, in oracledb.thin_impl.Protocol._process_single_message\n  File "src/oracledb/impl/thin/protocol.pyx", line 502, in oracledb.thin_impl.Protocol._process_single_message\n  File "src/oracledb/impl/thin/protocol.pyx", line 494, in oracledb.thin_impl.Protocol._process_message\n  File "src/oracledb/impl/thin/messages/base.pyx", line 102, in oracledb.thin_impl.Message._check_and_raise_exception\noracledb.exceptions.DatabaseError: statement cannot be translated from ORACLE to POSTGRES (needs manual migration): LLM fallback translator failed: null -- original SQL: CREATE SEQUENCE sp_seq1 START WITH 1\nHelp: https://docs.oracle.com/error-help/db/ora-00942/\n')
- Repro: client=python path=adapt sql=`SELECT sp_seq1.NEXTVAL FROM DUAL`
- Severity: LOW-MEDIUM (ADAPT dialect-translation gap)

## KNOWN_DIFF entries applied (12)
- ora014 / python / relay: ROWID is a physical row address. Each path's setup creates its own separate table/row instance (native's dual_probe is a different physical row than relay's or adapt's), so the ROWID value is expected to differ across paths even when NATIVE-vs-real-Oracle behavior is otherwise identical -- a scenario-design limitation (this scenario cannot assert row-count/existence-only without weakening real ROWID coverage entirely), not a Warp bug. On the ADAPT path, Postgres additionally has no ROWID type at all; pg_oracle emulates it from ctid.
- ora001 / python / adapt: ADAPT's login identity is the Postgres backend's own user (see test_orawire.py's connect() convention: user=postgres), not the real Oracle account the NATIVE/RELAY paths use -- USER legitimately differs by design, this is not a translation bug.
- ora002 / python / adapt: SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') has no dialect translation yet -- ADAPT raises ORA-00904 instead of returning the (Postgres-backed) current schema. Real, understood gap: SYS_CONTEXT's USERENV namespace is only partially implemented. Effort to close: ~0.5-1 day (extend DialectTranslations' SYS_CONTEXT handling with the CURRENT_SCHEMA key mapped to Postgres current_schema()).
- ora003 / python / adapt: ADAPT's V$VERSION comes from pg_oracle's Postgres-backed emulation view, not a real Oracle banner string; version text is expected to differ.
- ora004 / python / adapt: Unaliased-expression column name reflects the translated SQL text ('21 * 2', spaced) rather than Oracle's own re-serialization of the original expression ('21*2', unspaced) -- a cosmetic difference in default column naming for expressions without an explicit alias, not a value or correctness difference. Low effort to close (~1 day: preserve original source-text span per projected expression instead of re-stringifying the translated AST for the default alias) but low priority since real client code almost always aliases computed columns.
- ora005 / python / adapt: Same default-column-naming-from-translated-text gap as ora004, applied to string concatenation.
- ora007 / python / adapt: Same default-column-naming gap as ora004; additionally the translated column name surfaces the Postgres type name (INT4) rather than any form of the original CAST expression.
- ora010 / python / adapt: Same default-column-naming gap as ora004: unaliased TO_CHAR(...) is named 'TO_CHAR' after translation instead of the full original expression text.
- ora012 / python / adapt: Oracle INTERVAL DAY TO SECOND literal text formatting differs from Postgres's interval output style after dialect translation; values are equivalent, text representation is not.
- ora014 / python / adapt: ROWID is a physical row address. Each path's setup creates its own separate table/row instance (native's dual_probe is a different physical row than relay's or adapt's), so the ROWID value is expected to differ across paths even when NATIVE-vs-real-Oracle behavior is otherwise identical -- a scenario-design limitation (this scenario cannot assert row-count/existence-only without weakening real ROWID coverage entirely), not a Warp bug. On the ADAPT path, Postgres additionally has no ROWID type at all; pg_oracle emulates it from ctid.
- ora015 / python / adapt: Oracle treats '' as NULL; Postgres does not. This is a fundamental, well-known semantic gap in Oracle-to-Postgres migration, not a Warp bug -- see docs/WARP_GUIDE.md's dialect-translation caveats.
- ora016 / python / adapt: Same default-column-naming gap as ora004: NVL(...) is translated to COALESCE(...) and the default column name reflects that, not the original NVL(...) text.

## SKIPPED (client/path unavailable) (0)
