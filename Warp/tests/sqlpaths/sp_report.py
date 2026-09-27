"""sp_report: turns run_matrix.py's results list into oracle_matrix.md/.html (engine-neutral --
the engine name is just a parameter, no Oracle-specific logic here)."""
from __future__ import annotations

import collections
import html
import json
import os


def write_reports(engine, results, out_dir):
    md_path = os.path.join(out_dir, f"{engine}_matrix.md")
    html_path = os.path.join(out_dir, f"{engine}_matrix.html")
    with open(md_path, "w") as f:
        f.write(render_markdown(engine, results))
    with open(html_path, "w") as f:
        f.write(render_html(engine, results))
    print(f"[sp_report] wrote {md_path}")
    print(f"[sp_report] wrote {html_path}")


def _counts_by(results, keys):
    c = collections.Counter()
    for r in results:
        c[tuple(r.get(k) for k in keys)] += 1
    return c


def render_markdown(engine, results) -> str:
    lines = [f"# {engine} SQL-path matrix report", ""]
    non_native = [r for r in results if r["path"] != "native"]
    by_cc = _counts_by(non_native, ["client", "path"])
    clients = sorted({r["client"] for r in non_native})
    paths = sorted({r["path"] for r in non_native})
    verdicts = ["PASS", "KNOWN_DIFF", "WARP_BUG", "CLIENT_ISSUE", "FLAKY", "SKIP", "ERROR"]

    lines.append("## Matrix: client x path (counts)")
    header = "| client | path | " + " | ".join(verdicts) + " | total |"
    lines.append(header)
    lines.append("|---" * (len(verdicts) + 3) + "|")
    for c in clients:
        for p in paths:
            rows = [r for r in non_native if r["client"] == c and r["path"] == p]
            if not rows:
                continue
            vc = collections.Counter(r["verdict"] for r in rows)
            lines.append("| " + " | ".join([c, p] + [str(vc.get(v, 0)) for v in verdicts] + [str(len(rows))]) + " |")
    lines.append("")

    lines.append("## Per-category breakdown")
    cats = sorted({r["scenario"][:3] for r in non_native}) if non_native else []
    by_cat = collections.defaultdict(list)
    for r in non_native:
        by_cat[r["scenario"]].append(r)
    lines.append("(see JSON for full per-scenario detail; category grouping is by scenario id "
                 "prefix here for brevity)")
    lines.append("")

    bugs = [r for r in non_native if r["verdict"] == "WARP_BUG"]
    lines.append(f"## WARP_BUG / candidate bugs ({len(bugs)})")
    if not bugs:
        lines.append("None found in this run.")
    for r in bugs:
        lines.append(f"### {r['scenario']} / {r['client']} / {r['path']}")
        lines.append(f"- Diff: {r.get('diff')}")
        lines.append(f"- Repro: client={r['client']} path={r['path']} sql=`{r.get('repro', {}).get('sql')}`")
        lines.append(f"- Severity: {_severity(r)}")
        lines.append("")

    known = [r for r in non_native if r["verdict"] == "KNOWN_DIFF"]
    lines.append(f"## KNOWN_DIFF entries applied ({len(known)})")
    for r in known:
        lines.append(f"- {r['scenario']} / {r['client']} / {r['path']}: {r.get('reason')}")
    lines.append("")

    skips = [r for r in results if r["verdict"] == "SKIP"]
    lines.append(f"## SKIPPED (client/path unavailable) ({len(skips)})")
    seen = set()
    for r in skips:
        key = (r["client"], r.get("reason"))
        if key in seen:
            continue
        seen.add(key)
        lines.append(f"- {r['client']}: {r.get('reason')}")
    lines.append("")
    return "\n".join(lines)


def _severity(r):
    if r["path"] == "relay":
        return "HIGH (RELAY should be a transparent proxy to real Oracle)"
    if r["scenario"].startswith(("ora050", "ora051", "ora052", "ora053", "ora054")):
        return "MEDIUM (error-shape mismatch in ADAPT)"
    return "LOW-MEDIUM (ADAPT dialect-translation gap)"


def render_html(engine, results) -> str:
    md = render_markdown(engine, results)
    non_native = [r for r in results if r["path"] != "native"]
    clients = sorted({r["client"] for r in non_native})
    paths = sorted({r["path"] for r in non_native})
    rows_html = []
    for r in non_native:
        rows_html.append(
            f"<tr class='v-{html.escape(r['verdict'])}' data-client='{html.escape(r['client'])}' "
            f"data-path='{html.escape(r['path'])}' data-verdict='{html.escape(r['verdict'])}'>"
            f"<td>{html.escape(r['scenario'])}</td><td>{html.escape(r['client'])}</td>"
            f"<td>{html.escape(r['path'])}</td><td>{html.escape(r['verdict'])}</td>"
            f"<td>{html.escape(str(r.get('reason') or r.get('diff') or ''))}</td></tr>")
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>{engine} SQL-path matrix</title>
<style>
body{{font-family:-apple-system,Helvetica,Arial,sans-serif;margin:24px;background:#fafafa;color:#222}}
table{{border-collapse:collapse;width:100%;margin-top:12px}}
td,th{{border:1px solid #ddd;padding:4px 8px;font-size:13px}}
th{{background:#eee;position:sticky;top:0}}
.v-PASS{{background:#e9f9ee}} .v-WARP_BUG{{background:#fde2e2}} .v-KNOWN_DIFF{{background:#fff6d6}}
.v-SKIP{{background:#eee}} .v-ERROR{{background:#f8d7da}}
select{{margin-right:12px}}
pre{{white-space:pre-wrap;background:#fff;border:1px solid #ddd;padding:12px}}
</style></head><body>
<h1>{engine} SQL-path matrix</h1>
<div>
Client: <select id="fc"><option value="">(all)</option>{"".join(f"<option>{c}</option>" for c in clients)}</select>
Path: <select id="fp"><option value="">(all)</option>{"".join(f"<option>{p}</option>" for p in paths)}</select>
Verdict: <select id="fv"><option value="">(all)</option>{"".join(f"<option>{v}</option>" for v in ["PASS","KNOWN_DIFF","WARP_BUG","CLIENT_ISSUE","FLAKY","SKIP","ERROR"])}</select>
</div>
<table id="t"><thead><tr><th>scenario</th><th>client</th><th>path</th><th>verdict</th><th>reason/diff</th></tr></thead>
<tbody>{"".join(rows_html)}</tbody></table>
<script>
function apply(){{
  const c=document.getElementById('fc').value, p=document.getElementById('fp').value, v=document.getElementById('fv').value;
  document.querySelectorAll('#t tbody tr').forEach(tr=>{{
    const show=(!c||tr.dataset.client===c)&&(!p||tr.dataset.path===p)&&(!v||tr.dataset.verdict===v);
    tr.style.display = show?'':'none';
  }});
}}
['fc','fp','fv'].forEach(id=>document.getElementById(id).addEventListener('change',apply));
</script>
<h2>Markdown report</h2>
<pre>{html.escape(md)}</pre>
</body></html>"""
