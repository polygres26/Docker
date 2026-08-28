/*
 * pg_oracle.c -- the one piece of this extension that has to be C: the
 * `db_emulation` session GUC. Everything else (V$/GV$/DBA_* views, the
 * DBMS_OUTPUT package) is plain SQL/plpgsql in sql/pg_oracle--0.1.sql --
 * see that file's header comment for why the split is drawn here.
 *
 * What SET db_emulation = 'oracle' actually does: it's sugar for prepending
 * this extension's package/catalog schemas onto search_path, so unqualified
 * references the way real Oracle client code writes them --
 * DBMS_OUTPUT.PUT_LINE(...), V$SESSION, DBA_TABLES -- resolve without the
 * caller (Polywire's orawire frontend, or a human at psql) having to know
 * or spell out oracle_catalog.* by hand. Polywire issues exactly one
 * `SET db_emulation = 'oracle'` per orawire session and nothing else needs
 * to change on its side.
 *
 * Deliberately NOT a parser/planner hook: search_path is the smallest,
 * most-inspectable mechanism that gets unqualified-name resolution right,
 * and it composes with whatever search_path the user already had (Oracle
 * schemas go in front, nothing already there is removed).
 */
#include "postgres.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

typedef enum DbEmulationMode
{
	DB_EMULATION_POSTGRES = 0,
	DB_EMULATION_ORACLE = 1
} DbEmulationMode;

static const struct config_enum_entry db_emulation_options[] = {
	{"postgres", DB_EMULATION_POSTGRES, false},
	{"off", DB_EMULATION_POSTGRES, true},	/* accepted alias */
	{"oracle", DB_EMULATION_ORACLE, false},
	{NULL, 0, false}
};

static int db_emulation_mode = DB_EMULATION_POSTGRES;

/* The exact schema list SET db_emulation='oracle' prepends. Order matters:
 * oracle_catalog last among these three so a name defined in both
 * dbms_output/utl_file and oracle_catalog resolves to the package first --
 * doesn't currently happen, but keeps the precedent unsurprising as more
 * package schemas are added in later phases (top-20 DBMS_* rollout). */
#define ORACLE_EMULATION_SCHEMAS "dbms_output, utl_file, oracle_catalog"

static void
db_emulation_assign_hook(int newval, void *extra)
{
	const char *current_search_path;
	StringInfoData buf;

	if (newval == db_emulation_mode)
		return;					/* no-op SET, or the boot-time default assignment */

	current_search_path = GetConfigOption("search_path", false, false);

	initStringInfo(&buf);
	if (newval == DB_EMULATION_ORACLE)
	{
		/* Appended, not prepended: search_path's first existing entry is
		 * also where an unqualified CREATE TABLE/FUNCTION/etc. lands.
		 * Prepending oracle_catalog/dbms_output/utl_file would silently
		 * redirect a plain `CREATE TABLE foo(...)` into this extension's
		 * own schemas instead of the caller's -- found live: a CREATE
		 * TABLE issued right after SET db_emulation='oracle' landed in
		 * dbms_output instead of public. Appending keeps unqualified
		 * *lookups* working (DBMS_OUTPUT.PUT_LINE, V$SESSION still
		 * resolve) without touching where new objects are created. */
		if (current_search_path != NULL && current_search_path[0] != '\0')
			appendStringInfo(&buf, "%s, ", current_search_path);
		appendStringInfoString(&buf, ORACLE_EMULATION_SCHEMAS);
	}
	else
	{
		/* Switching back to 'postgres': strip exactly the suffix we added
		 * (a plain string match, not a schema-list parse -- if the user
		 * has since edited search_path by hand, leave it alone rather
		 * than guess wrong). */
		const char *suffix = ", " ORACLE_EMULATION_SCHEMAS;
		size_t		cur_len = current_search_path ? strlen(current_search_path) : 0;
		size_t		suffix_len = strlen(suffix);

		if (current_search_path != NULL && cur_len >= suffix_len &&
			strcmp(current_search_path + (cur_len - suffix_len), suffix) == 0)
			appendBinaryStringInfo(&buf, current_search_path, cur_len - suffix_len);
		else if (current_search_path != NULL)
			appendStringInfoString(&buf, current_search_path);
	}

	db_emulation_mode = newval;

	if (buf.len > 0)
		SetConfigOption("search_path", buf.data, PGC_USERSET, PGC_S_SESSION);

	pfree(buf.data);
}

void
_PG_init(void)
{
	DefineCustomEnumVariable(
		"db_emulation",
		"Which foreign database's client-visible surface this session should emulate "
		"(V$/GV$/DBA_* views, DBMS_*/UTL_* packages). Set by Polywire's protocol "
		"frontends on connect; safe to set by hand at psql too.",
		NULL,
		&db_emulation_mode,
		DB_EMULATION_POSTGRES,
		db_emulation_options,
		PGC_USERSET,
		0,
		NULL,
		db_emulation_assign_hook,
		NULL);

	MarkGUCPrefixReserved("db_emulation");
}

PG_FUNCTION_INFO_V1(pg_oracle_emulation_active);
Datum
pg_oracle_emulation_active(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(db_emulation_mode == DB_EMULATION_ORACLE);
}
