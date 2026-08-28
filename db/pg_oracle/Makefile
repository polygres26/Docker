# pg_oracle -- Oracle compatibility extension for Postgres (part of the Polygres collection).
# Standard PGXS build -- installs against whatever `pg_config` on PATH resolves to.

EXTENSION = pg_oracle
MODULE_big = pg_oracle
OBJS = src/pg_oracle.o src/dbms_output.o

DATA = sql/pg_oracle--0.1.sql

# TODO: pg_regress test files (test/sql/*.sql + test/expected/*.out) --
# tracked as follow-up; verified manually against a local instance for now
# (see db/pg_oracle/README.md's "Verified" section for exactly what was run).

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
