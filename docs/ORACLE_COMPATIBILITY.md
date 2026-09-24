# Oracle compatibility

Warp can sit in front of Oracle traffic two different ways, and which one you use changes what
works today. This page is a plain compatibility baseline — what we've actually tested, and what
you should expect — so you can pick the right mode for your situation.

## The two ways to connect

**Point-to-a-real-Oracle mode.** Warp sits in front of a real Oracle database and passes your
traffic straight through to it. Use this when you already have Oracle and want Warp's governance,
metrics, and access control in front of it without changing anything about how your apps talk to
Oracle.

**Point-to-another-database mode.** Warp accepts Oracle-style connections and rewrites the SQL on
the fly to run against a different backend (for example Postgres) — useful when you're migrating
off Oracle and want your existing Oracle-speaking apps and tools to keep working unmodified while
the actual data lives somewhere else.

## What we tested

We ran five common ways people talk to Oracle — a Python app, a Java (JDBC) app, the SQLcl
command-line tool, the classic SQL*Plus command-line tool, and Oracle's DATABASE LINK feature
(querying another database through your Oracle connection) — against both connection modes above.

### Point-to-a-real-Oracle mode

| Tool | Result |
|---|---|
| Python apps | ✅ Works normally |
| Java apps (JDBC) | ✅ Works normally |
| SQLcl | ✅ Works normally |
| SQL\*Plus | ✅ Works normally |
| DATABASE LINK queries | ✅ Works normally |

Everything we tested works normally in this mode — it behaves like a direct connection to your
Oracle database, including cross-database DATABASE LINK queries.

### Point-to-another-database mode

| Tool | Target: another Oracle database | Target: a Postgres database |
|---|---|---|
| Python apps | ✅ Works normally | ✅ Works normally |
| Java apps (JDBC) | ✅ Works normally | ✅ Works normally |
| SQLcl | ✅ Works normally | ✅ Works normally |
| SQL\*Plus | ⚠️ Connects, but hangs | ⚠️ Connects, but hangs |
| DATABASE LINK queries | ⚠️ Connects, but hangs | Not applicable — the target database has no DATABASE LINK feature to translate to |

In this mode, Python, Java (JDBC), and SQLcl all work reliably against either target. SQL\*Plus
does not yet work reliably in this mode against either target — we recommend using a Python or
Java-based tool instead until this is resolved. DATABASE LINK queries in this mode are not yet
supported.

## What this means for you

- **If you're pointing Warp at a real Oracle database**, use whichever tool you already use today
  — everything works.
- **If you're using Warp to run Oracle-speaking traffic against a different database**, Python,
  Java (JDBC), and SQLcl are all reliable choices today, against either an Oracle or a Postgres
  target. Avoid SQL\*Plus and DATABASE LINK queries in this mode for now.

This page will be updated as compatibility improves, and the same baseline approach will extend to
Warp's other supported protocols (MySQL, SQL Server, MongoDB) over time.
