# pg_sqlserver -- SQL Server compatibility extension for Postgres (part of
# the Polygres collection). Pure SQL/plpgsql, no C module of its own -- see
# README.md and pg_sqlserver.control for why (the one C symbol this
# extension needs, sys.emulation_active(), binds directly against
# pg_oracle's already-built shared library by path).
# Standard PGXS build, DATA-only (no MODULE_big/OBJS -- nothing to compile).

EXTENSION = pg_sqlserver
DATA = sql/pg_sqlserver--0.1.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
