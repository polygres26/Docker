# pg_oracle -- Oracle compatibility extension for Postgres (part of the Polygres collection).
# Standard PGXS build -- installs against whatever `pg_config` on PATH resolves to.

EXTENSION = pg_oracle
MODULE_big = pg_oracle
OBJS = src/pg_oracle.o src/dbms_output.o src/utl_file.o src/utl_http.o

DATA = sql/pg_oracle--0.1.sql

# UTL_HTTP needs a real outbound HTTP client -- SQL/plpgsql can't open a
# socket at all, and reimplementing one from raw sockets would be its own
# project, so this links libcurl rather than reinventing HTTP.
SHLIB_LINK += $(shell pkg-config --libs libcurl 2>/dev/null || echo -lcurl)
PG_CPPFLAGS += $(shell pkg-config --cflags libcurl 2>/dev/null)

# TODO: pg_regress test files (test/sql/*.sql + test/expected/*.out) --
# tracked as follow-up; verified manually against a local instance for now
# (see db/pg_oracle/README.md's "Verified" section for exactly what was run).

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
