#!/usr/bin/env python3
"""Regenerates docker/assets/architecture.svg.

The diagram is data-driven: edit FRONTENDS / STAGES / BACKENDS below (or add a group) and re-run
    python3 docker/assets/gen_architecture.py
Palette mirrors Warp/web/src/styles/tokens.css (paper, ink, ember accent, dark rail).
"""
from html import escape
from pathlib import Path

# ---- data -------------------------------------------------------------------------------------
# (group, colour-key, [(frontend, port, "what the client sees")])
FRONTENDS = [
    ("Relational SQL", "ember", [
        ("pgwire", "15432", "PostgreSQL"), ("mywire", "13306", "MySQL"),
        ("mssqlwire", "14333", "SQL Server"), ("orawire", "11521", "Oracle")]),
    ("Documents, keys, search, graph, series", "green", [
        ("mongowire", "27017", "MongoDB"), ("dynamowire", "18000", "DynamoDB"),
        ("rediswire", "16379", "Redis"), ("oswire", "9200", "OpenSearch"),
        ("boltwire", "7687", "Neo4j"), ("influxwire", "8086", "InfluxDB"),
        ("aztable", "azure", "Azure Table"), ("firestorewire", "8080", "Firestore"),
        ("datastorewire", "8081", "Datastore"), ("bigtablewire", "8088", "Bigtable"),
        ("cqlwire", "19042", "Cassandra"), ("gremlinwire", "8182", "Gremlin")]),
    ("Queues and streams", "amber", [
        ("sqswire", "9324", "SQS"), ("awswire", "4566", "SNS, Kinesis"),
        ("azqueue", "azure", "Azure Queue"), ("pubsubwire", "8085", "Pub/Sub")]),
    ("Objects and secrets", "slate", [
        ("s3wire", "18020", "S3"), ("azblob", "azure", "Azure Blob"),
        ("gcswire", "4443", "Cloud Storage"), ("awswire", "4566", "Secrets, SSM, KMS, STS")]),
    ("Warp native", "ink", [
        ("gRPC", "7070", "typed clients"), ("MCP", "18010", "AI agents"),
        ("Admin UI", "19090", "operators")]),
]
CLIENTS = ["JDBC / ODBC drivers", "AWS, Azure and Google SDKs", "mongosh, redis-cli, cypher-shell",
           "Applications, ORMs, BI tools", "AI agents"]
STAGES = ["Federation", "Firewall", "Router", "QoS", "Dialect", "Rollup", "Cache", "Stats"]
SETS = [("Backend set: default", ["pg-1", "pg-2", "pg-3"]), ("Backend set: orders", ["pg-4", "pg-5"])]
NATIVE = ["Oracle", "MySQL", "SQL Server", "Neo4j"]
CONNECTORS = ["Snowflake", "BigQuery", "ClickHouse", "Kafka", "Cassandra", "S3"]
SIDE = [("Ferry", "migrates data and schema into Postgres"), ("Shim", "compatibility functions and views"),
        ("Control plane", "backend sets, users, licences")]

# ---- palette (light and dark) -----------------------------------------------------------------
LIGHT = dict(paper="#f4f3ef", surface="#ffffff", ink="#20211f", muted="#666962", line="#dcded7",
             ember="#e4512d", ember_s="#fff0e8", green="#19725a", green_s="#e5f3ed",
             amber="#9a6508", amber_s="#fbf1dc", slate="#4a4d45", slate_s="#ecece7",
             ink_s="#e6e6e0", rail="#242722", rail_text="#f4f2eb", rail_muted="#a7aaa1")
DARK = dict(paper="#171816", surface="#242722", ink="#f4f2eb", muted="#a7aaa1", line="#3b3e37",
            ember="#f0704f", ember_s="#3a2219", green="#3fbf98", green_s="#17302a",
            amber="#e0a93a", amber_s="#38290f", slate="#b9bcb2", slate_s="#2d302a",
            ink_s="#2d302a", rail="#0f100f", rail_text="#f4f2eb", rail_muted="#a7aaa1")
KEYS = ("ember", "green", "amber", "slate", "ink")

W = 1760
FONT = "Inter, 'Helvetica Neue', Arial, sans-serif"
MONO = "'JetBrains Mono', 'SF Mono', Menlo, monospace"


def t(x, y, s, cls="", anchor="start"):
    return f'<text x="{x}" y="{y}" class="{cls}" text-anchor="{anchor}">{escape(s)}</text>'


def build():
    out, ny = [], 0
    # ---- geometry
    cx, cw = 40, 250               # clients column
    fx, fw = 350, 700              # frontends column
    px, pw = 1110, 190             # pipeline column
    bx, bw = 1360, 360             # backends column
    top = 150

    # frontends: 2 columns of cards inside each group
    card_w, card_h, gap = 166, 46, 8
    y = top
    fe = []
    for name, key, items in FRONTENDS:
        rows = (len(items) + 3) // 4
        gh = 40 + rows * (card_h + gap)
        fe.append((name, key, items, y, gh))
        y += gh + 14
    height = max(y + 190, 1080)
    body = []

    # background
    body.append(f'<rect width="{W}" height="{height}" fill="var(--paper)"/>')
    # header
    body.append(t(40, 58, "Warp", "h1"))
    body.append(t(128, 58, "one Postgres estate, every database's protocol", "sub"))
    body.append(t(40, 86, "Applications keep their drivers and change only the host and port. Every request, whatever "
                          "the protocol, runs the same pipeline and lands in Postgres.", "note"))
    for lbl, x, w in (("CLIENTS", cx, cw), ("FRONTENDS, one per protocol", fx, fw),
                      ("SHARED PIPELINE", px, pw), ("BACKEND SETS AND BACKENDS", bx, bw)):
        body.append(t(x, 128, lbl, "eyebrow"))

    # clients
    cy = top
    ch = 62
    for i, c in enumerate(CLIENTS):
        yy = cy + i * (ch + 26)
        body.append(f'<rect x="{cx}" y="{yy}" width="{cw}" height="{ch}" rx="6" class="box"/>')
        body.append(t(cx + 16, yy + 37, c, "lbl"))
    client_mid = [cy + i * (ch + 26) + ch / 2 for i in range(len(CLIENTS))]

    # frontends
    for name, key, items, gy, gh in fe:
        body.append(f'<rect x="{fx}" y="{gy}" width="{fw}" height="{gh}" rx="8" fill="var(--{key}_s)" '
                    f'stroke="var(--{key})" stroke-width="1"/>')
        body.append(t(fx + 16, gy + 26, name.upper(), f"grp {key}"))
        for i, (fe_name, port, sees) in enumerate(items):
            r, c = divmod(i, 4)
            ix = fx + 16 + c * (card_w + gap)
            iy = gy + 40 + r * (card_h + gap)
            body.append(f'<rect x="{ix}" y="{iy}" width="{card_w - 4}" height="{card_h}" rx="5" class="card"/>')
            body.append(t(ix + 12, iy + 20, fe_name, "mono"))
            body.append(t(ix + card_w - 14, iy + 20, port, "port", "end"))
            body.append(t(ix + 12, iy + 37, sees, "small"))
    fe_bottom = y - 14
    fe_mid = (top + fe_bottom) / 2

    # arrows clients -> frontends
    body.append('<defs><marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
                'orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="var(--muted)"/></marker></defs>')
    for m in client_mid:
        body.append(f'<path d="M{cx + cw} {m} C {cx + cw + 30} {m}, {fx - 30} {fe_mid}, {fx - 2} {fe_mid}" '
                    f'class="edge"/>')
    body.append(f'<line x1="{fx - 40}" y1="{fe_mid}" x2="{fx - 2}" y2="{fe_mid}" class="edge" marker-end="url(#ar)"/>')
    body.append(t(cx, top + 5 * (ch + 26) - 8, "over each protocol's own wire format", "cap"))

    # pipeline
    ph = fe_bottom - top
    body.append(f'<rect x="{px}" y="{top}" width="{pw}" height="{ph}" rx="8" class="rail"/>')
    step = (ph - 60) / len(STAGES)
    for i, s in enumerate(STAGES):
        sy = top + 30 + i * step
        body.append(f'<rect x="{px + 20}" y="{sy}" width="{pw - 40}" height="{step - 12}" rx="5" class="stage"/>')
        body.append(t(px + pw / 2, sy + (step - 12) / 2 + 5, s, "stage-t", "middle"))
    body.append(t(px + pw / 2, top + ph - 12, "Ignite cache, cluster-wide", "rail-cap", "middle"))
    body.append(f'<line x1="{fx + fw + 4}" y1="{fe_mid}" x2="{px - 2}" y2="{fe_mid}" class="edge" marker-end="url(#ar)"/>')
    body.append(t(fx + fw + 6, fe_mid - 8, "SQL or", "cap"))
    body.append(t(fx + fw + 6, fe_mid + 14, "store ops", "cap"))

    # backends
    by = top
    body.append(f'<rect x="{bx}" y="{by}" width="{bw}" height="{ph * 0.56}" rx="8" fill="var(--surface)" '
                f'stroke="var(--ember)" stroke-width="1.5"/>')
    body.append(t(bx + 16, by + 26, "POSTGRES, THE SYSTEM OF RECORD", "grp ember"))
    yy = by + 42
    for sname, hosts in SETS:
        body.append(f'<rect x="{bx + 14}" y="{yy}" width="{bw - 28}" height="112" rx="6" class="card"/>')
        body.append(t(bx + 28, yy + 24, sname, "lbl"))
        for j, h in enumerate(hosts):
            hx = bx + 28 + j * 96
            body.append(f'<rect x="{hx}" y="{yy + 40}" width="84" height="56" rx="5" fill="var(--ember_s)" '
                        f'stroke="var(--ember)"/>')
            body.append(t(hx + 42, yy + 73, h, "mono", "middle"))
        yy += 126
    body.append(t(bx + 16, yy + 8, "Stores are sharded by hash across the backends of a set", "cap"))
    body.append(t(bx + 16, yy + 26, "and rebalanced when a backend is added.", "cap"))

    ny = by + ph * 0.56 + 22
    for title, items, key in (("NATIVE BACKENDS (proxy mode)", NATIVE, "slate"),
                              ("CONNECTORS (federated reads)", CONNECTORS, "slate")):
        body.append(f'<rect x="{bx}" y="{ny}" width="{bw}" height="{(ph * 0.44 - 44) / 2}" rx="8" '
                    f'fill="var(--slate_s)" stroke="var(--slate)" stroke-width="1"/>')
        body.append(t(bx + 16, ny + 26, title, "grp slate"))
        cxp = bx + 16
        rowy = ny + 40
        for it in items:
            wpx = 14 + 7.4 * len(it)
            if cxp + wpx > bx + bw - 10:
                cxp = bx + 16
                rowy += 34
            body.append(f'<rect x="{cxp}" y="{rowy}" width="{wpx}" height="26" rx="13" class="pill"/>')
            body.append(t(cxp + wpx / 2, rowy + 17, it, "pilltxt", "middle"))
            cxp += wpx + 8
        ny += (ph * 0.44 - 44) / 2 + 22
    body.append(f'<line x1="{px + pw + 4}" y1="{fe_mid - 40}" x2="{bx - 2}" y2="{fe_mid - 40}" class="edge" marker-end="url(#ar)"/>')
    body.append(f'<line x1="{px + pw + 4}" y1="{fe_mid + 120}" x2="{bx - 2}" y2="{fe_mid + 120}" class="edge" marker-end="url(#ar)"/>')

    # bottom band: ecosystem
    by2 = fe_bottom + 34
    body.append(t(40, by2, "AROUND THE GATEWAY", "eyebrow"))
    bw2 = (W - 80 - 2 * 20) / 3
    for i, (n, d) in enumerate(SIDE):
        xx = 40 + i * (bw2 + 20)
        body.append(f'<rect x="{xx}" y="{by2 + 14}" width="{bw2}" height="84" rx="8" class="box"/>')
        body.append(t(xx + 20, by2 + 46, n, "h3"))
        body.append(t(xx + 20, by2 + 74, d, "small"))
    total_h = by2 + 14 + 84 + 40
    body.append(t(40, total_h - 14, "Generated by docker/assets/gen_architecture.py. Ports shown are the frontend defaults "
                                   "inside the container; azure and gRPC/REST entries are configured per deployment.", "cap"))

    css = f"""
:root{{{';'.join(f'--{k}:{v}' for k, v in LIGHT.items())}}}
@media (prefers-color-scheme: dark){{:root{{{';'.join(f'--{k}:{v}' for k, v in DARK.items())}}}}}
text{{font-family:{FONT};fill:var(--ink)}}
.h1{{font-size:34px;font-weight:700;letter-spacing:-0.02em}}
.sub{{font-size:18px;fill:var(--ember);font-weight:600}}
.note{{font-size:14px;fill:var(--muted)}}
.eyebrow{{font-size:12px;letter-spacing:.12em;font-weight:700;fill:var(--muted)}}
.grp{{font-size:12px;letter-spacing:.1em;font-weight:700}}
.grp.ember{{fill:var(--ember)}}.grp.green{{fill:var(--green)}}.grp.amber{{fill:var(--amber)}}
.grp.slate{{fill:var(--slate)}}.grp.ink{{fill:var(--ink)}}
.mono{{font-family:{MONO};font-size:13px;font-weight:600}}
.port{{font-family:{MONO};font-size:11px;fill:var(--muted)}}
.small{{font-size:12px;fill:var(--muted)}}
.lbl{{font-size:14px;font-weight:600}}
.cap{{font-size:12px;fill:var(--muted)}}
.h3{{font-size:17px;font-weight:700}}
.box{{fill:var(--surface);stroke:var(--line)}}
.card{{fill:var(--surface);stroke:var(--line)}}
.rail{{fill:var(--rail)}}
.stage{{fill:none;stroke:var(--rail_muted);stroke-opacity:.5}}
.stage-t{{fill:var(--rail_text);font-size:14px;font-weight:600}}
.rail-cap{{fill:var(--rail_muted);font-size:11px}}
.edge{{stroke:var(--muted);stroke-width:1.5;fill:none}}
.pill{{fill:var(--surface);stroke:var(--slate)}}
.pilltxt{{font-size:12px;font-weight:600}}
"""
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {int(total_h)}" role="img" '
           f'aria-label="Warp architecture: clients connect over native protocols to frontends, which share one '
           f'pipeline and store data in Postgres backend sets">'
           f'<style>{css}</style>' + "".join(body) + "</svg>")
    return svg


if __name__ == "__main__":
    out = Path(__file__).with_name("architecture.svg")
    out.write_text(build())
    print(f"wrote {out} ({out.stat().st_size} bytes)")
