"""sp_core: engine-agnostic core for the SQL-path test matrix framework.

Vocabulary
----------
Scenario   -- one engine-neutral test case: SQL text (+ optional binds/PL block), tags, and an
              "expect" hint (rows/error/etc). Engines provide their own scenario corpus; this
              module knows nothing about Oracle/MySQL/SQL Server SQL text itself.
Client     -- a driver/tool (sqlplus, sqlcl, jdbc, python, ...). An adapter module knows how to
              run one Scenario against one (host, port, credentials) target using that client and
              return a CanonicalResult.
Path       -- NATIVE / RELAY / ADAPT: which backend the client is really talking to.
CanonicalResult -- the normalized, comparable shape of a scenario's outcome: column names (upper-
              cased, since Oracle/JDBC/etc disagree on case in places that don't matter), rows (as
              strings, since binary/textual representations differ across drivers), row count,
              and/or a normalized error (SQLSTATE/ORA code + message with volatile bits stripped).
              Elapsed time is deliberately excluded.

Classification of a RELAY or ADAPT result against the same client's NATIVE result:
  PASS         -- canonical results identical.
  KNOWN_DIFF   -- different, but matches an entry in known/<engine>_<path>.yaml with a reason.
  WARP_BUG     -- different, undocumented. RELAY diffs are always candidate bugs (native-backend
                  mode is supposed to be a transparent proxy). ADAPT diffs are candidate bugs
                  unless they get a KNOWN_DIFF entry (real Postgres-semantic gaps).
  CLIENT_ISSUE -- the *client itself* misbehaved identically in NATIVE too (e.g. sqlplus formats
                  a NUMBER slightly differently from JDBC) -- detected by the client-vs-client
                  native sanity cross-check, not by RELAY comparison.
  FLAKY        -- re-run flips the classification without a code change; recorded, not silently
                  discarded.
"""
from __future__ import annotations

import dataclasses
import decimal
import json
import re
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class Scenario:
    id: str
    category: str
    description: str
    sql: str
    tags: list = dataclasses.field(default_factory=list)
    binds: Optional[list] = None          # list of bind values, positional, for parametrized SQL
    setup: Optional[list] = None          # SQL statements to run first (not compared)
    teardown: Optional[list] = None       # SQL statements to run after (not compared), best-effort
    expect: str = "rows"                  # "rows" | "error" | "ok" (DDL/DML with no rows)
    requires_plsql: bool = False
    oracle_only: bool = False
    expect_adapt_diff: Optional[str] = None   # short reason code if ADAPT is documented-different
    client_filter: Optional[list] = None  # if set, only run against these client names
    fetch_all: bool = True

    @staticmethod
    def from_dict(d: dict) -> "Scenario":
        known = {f.name for f in dataclasses.fields(Scenario)}
        return Scenario(**{k: v for k, v in d.items() if k in known})


def load_scenarios(path: str) -> list:
    with open(path) as f:
        data = json.load(f)
    return [Scenario.from_dict(d) for d in data]


# ---------------------------------------------------------------------------
# Canonical result
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class CanonicalResult:
    ok: bool
    columns: Optional[list] = None       # list[str], uppercased
    rows: Optional[list] = None          # list[list[str]] stringified, normalized
    row_count: Optional[int] = None
    error_code: Optional[str] = None     # e.g. "ORA-00942" or a DB-API/SQLSTATE code
    error_text: Optional[str] = None     # normalized (volatile bits stripped)
    raw: Optional[str] = None            # raw stdout/exception text, kept for repro/debugging only
    client_note: Optional[str] = None    # e.g. "sqlplus DESCRIBE format"

    def to_dict(self):
        return dataclasses.asdict(self)


_NUM_RE = re.compile(r"^-?\d+(\.\d+)?$")
_WS_RE = re.compile(r"\s+")
_ORA_CODE_RE = re.compile(r"ORA-(\d{5})")
_VOLATILE_RE = re.compile(
    r"(0x[0-9a-fA-F]+|\bat\s+line\s+\d+\b|\btime[: ]\S+|\bconnection id \d+\b)"
)


def normalize_scalar(v: Any) -> str:
    """Reduce one cell to a comparable string: numbers lose trailing-zero/format noise, None is
    a fixed sentinel, everything else is whitespace-collapsed text."""
    if v is None:
        return "<NULL>"
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float, decimal.Decimal)):
        d = decimal.Decimal(str(v))
        d = d.normalize()
        # avoid Decimal's E-notation for small exponents so "42" and "42.0" compare equal
        s = format(d, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s or "0"
    s = str(v).strip()
    if _NUM_RE.match(s):
        return normalize_scalar(decimal.Decimal(s))
    return _WS_RE.sub(" ", s)


def normalize_rows(columns, rows) -> tuple:
    cols = [str(c).upper() for c in (columns or [])]
    norm_rows = [[normalize_scalar(v) for v in row] for row in (rows or [])]
    return cols, norm_rows


def normalize_error(code: Optional[str], text: Optional[str]) -> tuple:
    text = text or ""
    if not code:
        m = _ORA_CODE_RE.search(text)
        if m:
            code = "ORA-" + m.group(1)
    norm_text = _VOLATILE_RE.sub("<X>", text)
    norm_text = _WS_RE.sub(" ", norm_text).strip()
    return code, norm_text


def compare(a: CanonicalResult, b: CanonicalResult) -> bool:
    """True if two canonical results are considered identical for matrix purposes."""
    if a.ok != b.ok:
        return False
    if not a.ok:
        return (a.error_code or "") == (b.error_code or "")
    if (a.columns or []) != (b.columns or []):
        return False
    if (a.rows or []) != (b.rows or []):
        return False
    if a.row_count is not None and b.row_count is not None and a.row_count != b.row_count:
        return False
    return True


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

PASS, KNOWN_DIFF, WARP_BUG, CLIENT_ISSUE, FLAKY, SKIP, ERROR = (
    "PASS", "KNOWN_DIFF", "WARP_BUG", "CLIENT_ISSUE", "FLAKY", "SKIP", "ERROR")


def classify(path: str, native: CanonicalResult, other: CanonicalResult,
             known_diffs: dict, scenario: Scenario) -> tuple:
    """Returns (verdict, reason)."""
    if compare(native, other):
        return PASS, None
    key = scenario.id
    entry = known_diffs.get(key)
    if entry and entry.get("path", path) in (path, "any"):
        return KNOWN_DIFF, entry.get("reason", "documented")
    if scenario.expect_adapt_diff and path == "adapt":
        return KNOWN_DIFF, scenario.expect_adapt_diff
    return WARP_BUG, None


def result_diff_text(native: CanonicalResult, other: CanonicalResult) -> str:
    parts = []
    if native.ok != other.ok:
        parts.append(f"ok: native={native.ok} other={other.ok}")
    if native.ok and other.ok:
        if (native.columns or []) != (other.columns or []):
            parts.append(f"columns: native={native.columns} other={other.columns}")
        if (native.rows or []) != (other.rows or []):
            parts.append(f"rows: native={native.rows} other={other.rows}")
    else:
        parts.append(f"error: native=({native.error_code},{native.error_text!r}) "
                      f"other=({other.error_code},{other.error_text!r})")
    return "; ".join(parts)
