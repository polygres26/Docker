# pg_mysql -- MySQL compatibility extension for Postgres (part of the
# Polygres collection). Pure SQL/plpgsql, no C module of its own -- see
# README.md and pg_mysql.control for why (the one C symbol this extension
# needs, mysql_catalog.emulation_active(), binds directly against
# pg_oracle's already-built shared library by path).
# Standard PGXS build, DATA-only (no MODULE_big/OBJS -- nothing to compile).

EXTENSION = pg_mysql
DATA = sql/pg_mysql--0.1.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
