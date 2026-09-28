"""sp_python_client: python-oracledb (thin mode) adapter -- engine-agnostic entry point `run`.

Used for the Oracle engine plug-in; a MySQL/SQL Server plug-in would supply its own module
(mysql.connector / pyodbc / pymssql) with the same `run(scenario, target) -> CanonicalResult`
signature.
"""
from __future__ import annotations

import sys
sys.path.insert(0, __file__.rsplit("/", 2)[0])  # sqlpaths/ for sp_core
from sp_core import CanonicalResult, normalize_rows, normalize_error  # noqa: E402


def available():
    try:
        import oracledb  # noqa: F401
        return True, ""
    except Exception as e:  # noqa: BLE001
        return False, f"python-oracledb not importable: {e}"


def _connect(target):
    import oracledb
    dsn = f"{target.host}:{target.port}/{target.extra.get('service', 'FREEPDB1')}"
    kwargs = dict(user=target.user, password=target.password, dsn=dsn, disable_oob=True)
    if target.extra.get("tls"):
        kwargs["ssl_server_dn_match"] = False
        dsn = f"tcps://{target.host}:{target.port}/{target.extra.get('service', 'FREEPDB1')}"
        kwargs["dsn"] = dsn
    return oracledb.connect(**kwargs)


def run(scenario, target) -> CanonicalResult:
    conn = _connect(target)
    try:
        conn.autocommit = False
        cur = conn.cursor()
        try:
            for stmt in (scenario.setup or []):
                cur.execute(stmt)
            try:
                if scenario.id == "ora027":  # RETURNING INTO needs an out var
                    import oracledb
                    out_id = cur.var(oracledb.NUMBER)
                    cur.execute(scenario.sql, [scenario.binds[0], scenario.binds[1], out_id])
                    result = CanonicalResult(ok=True, columns=["ID"], rows=[[out_id.getvalue()[0]]])
                elif scenario.id == "ora045":
                    import oracledb
                    rc = cur.var(oracledb.CURSOR)
                    cur.execute(scenario.sql, [rc])
                    inner = rc.getvalue()
                    rows = inner.fetchall()
                    cols, nrows = normalize_rows([d[0] for d in inner.description], rows)
                    result = CanonicalResult(ok=True, columns=cols, rows=nrows, row_count=len(nrows))
                elif scenario.binds:
                    cur.execute(scenario.sql, scenario.binds)
                    if scenario.expect == "rows" and cur.description:
                        rows = cur.fetchall()
                        cols, nrows = normalize_rows([d[0] for d in cur.description], rows)
                        result = CanonicalResult(ok=True, columns=cols, rows=nrows, row_count=len(nrows))
                    else:
                        result = CanonicalResult(ok=True)
                else:
                    cur.execute(scenario.sql)
                    if scenario.expect == "rows" and cur.description:
                        rows = cur.fetchall()
                        cols, nrows = normalize_rows([d[0] for d in cur.description], rows)
                        result = CanonicalResult(ok=True, columns=cols, rows=nrows, row_count=len(nrows))
                    else:
                        result = CanonicalResult(ok=True)
                conn.commit()
            except Exception as e:  # noqa: BLE001 -- real DB-API error expected for expect=="error"
                conn.rollback()
                code = getattr(getattr(e, "args", [None])[0], "code", None) if e.args else None
                text = str(e)
                ncode, ntext = normalize_error(f"ORA-{code:05d}" if code else None, text)
                result = CanonicalResult(ok=False, error_code=ncode, error_text=ntext, raw=text)
        finally:
            for stmt in (scenario.teardown or []):
                try:
                    cur.execute(stmt)
                    conn.commit()
                except Exception:  # noqa: BLE001 -- best-effort cleanup
                    conn.rollback()
        return result
    finally:
        conn.close()
