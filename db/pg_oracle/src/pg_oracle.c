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
 * caller (Warp's orawire frontend, or a human at psql) having to know
 * or spell out oracle_catalog.* by hand. Warp issues exactly one
 * `SET db_emulation = 'oracle'` per orawire session and nothing else needs
 * to change on its side.
 *
 * Deliberately NOT a parser/planner hook: search_path is the smallest,
 * most-inspectable mechanism that gets unqualified-name resolution right,
 * and it composes with whatever search_path the user already had (Oracle
 * schemas go in front, nothing already there is removed).
 *
 * The other thing SET db_emulation = 'oracle' does: ensure_user_schema()
 * below, an Oracle-native pattern -- in real Oracle, schema *is* username,
 * so a role's own objects are always found unqualified. Postgres's
 * default search_path already carries a "$user" placeholder for exactly
 * this (resolves to a same-named schema if one exists, silently skipped
 * otherwise) -- it's just that nothing creates that schema, so it's
 * always the "otherwise" in a fresh database. Auto-creating it once is
 * the whole feature: DBA_TABLES/USER_TABLES-style OWNER-based filtering
 * (which maps Oracle's OWNER onto Postgres schemaname, see
 * sql/pg_oracle--0.1.sql) then finds a user's own objects the way an
 * Oracle DBA script expects, with no additional search_path logic here.
 */
#include "postgres.h"
#include "fmgr.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"

PG_MODULE_MAGIC;

typedef enum DbEmulationMode
{
	DB_EMULATION_POSTGRES = 0,
	DB_EMULATION_ORACLE = 1,
	DB_EMULATION_MYSQL = 2,
	DB_EMULATION_SQLSERVER = 3
} DbEmulationMode;

static const struct config_enum_entry db_emulation_options[] = {
	{"postgres", DB_EMULATION_POSTGRES, false},
	{"off", DB_EMULATION_POSTGRES, true},	/* accepted alias */
	{"oracle", DB_EMULATION_ORACLE, false},
	{"mysql", DB_EMULATION_MYSQL, false},
	{"sqlserver", DB_EMULATION_SQLSERVER, false},
	{NULL, 0, false}
};

static int db_emulation_mode = DB_EMULATION_POSTGRES;

/* The exact schema list SET db_emulation='oracle' prepends. Order matters:
 * oracle_catalog last among these three so a name defined in both
 * dbms_output/utl_file and oracle_catalog resolves to the package first --
 * doesn't currently happen, but keeps the precedent unsurprising as more
 * package schemas are added in later phases (top-20 DBMS_* rollout). */
#define ORACLE_EMULATION_SCHEMAS "dbms_output, dbms_random, dbms_utility, dbms_assert, dbms_network_acl_admin, dbms_crypto, dbms_scheduler, dbms_aqadm, dbms_aq, dbms_stats, dbms_session, dbms_sql, dbms_sys_sql, utl_file, utl_http, oracle_catalog"

/* The equivalent for SET db_emulation='mysql', provided by the separate
 * pg_mysql extension (db/pg_mysql) -- db_emulation itself has to live in
 * exactly one C module (Postgres has no way for two independently loaded
 * .so's to both own a GUC named "db_emulation"), so pg_oracle is that one
 * shared owner and pg_mysql declares a real `requires = 'pg_oracle'`
 * dependency on it rather than hiding the coupling. pg_mysql's own SQL
 * script is the only thing that needs mysql_catalog to actually exist;
 * this C module doesn't reference it directly and works fine with
 * db_emulation='mysql' set even if pg_mysql isn't installed (the schema
 * just wouldn't resolve to anything, same as any other missing schema on
 * search_path). */
#define MYSQL_EMULATION_SCHEMAS "mysql_catalog"

/* The equivalent for SET db_emulation='sqlserver', provided by the separate
 * pg_sqlserver extension (db/pg_sqlserver) -- same "db_emulation lives in
 * exactly one C module" reasoning as MYSQL_EMULATION_SCHEMAS above. Just
 * "sys" (not "sys, sqlserver_catalog" or similar): SQL Server's own
 * convention already puts everything -- catalog views AND functions --
 * in a single schema literally named sys, so pg_sqlserver follows that
 * instead of inventing a second schema name nothing outside this project
 * would recognize. */
#define SQLSERVER_EMULATION_SCHEMAS "sys"

/* Returns the schema-list string SET db_emulation=<mode> should append
 * onto search_path, or NULL for 'postgres' (nothing to append -- also
 * what an as-yet-unrecognized future mode value would fall through to,
 * safely: no C module owns it yet, so there is nothing to add). */
static const char *
emulation_schemas_for(int mode)
{
	switch (mode)
	{
		case DB_EMULATION_ORACLE:
			return ORACLE_EMULATION_SCHEMAS;
		case DB_EMULATION_MYSQL:
			return MYSQL_EMULATION_SCHEMAS;
		case DB_EMULATION_SQLSERVER:
			return SQLSERVER_EMULATION_SCHEMAS;
		default:
			return NULL;
	}
}

/*
 * If search_path currently ends with (or is exactly) the given emulation
 * schema-list suffix, sets *base_len to the length of search_path with
 * that suffix (and its preceding ", ") stripped off and returns true.
 * Otherwise returns false and leaves *base_len untouched. Shared by both
 * the "is the target mode's suffix already there" check and the "which
 * OTHER mode's suffix, if any, needs to be stripped before appending a
 * new one" check below -- switching directly between two emulation modes
 * (oracle -> mysql with no intervening 'postgres') must not stack suffixes
 * or leave a stale sibling-mode suffix behind.
 */
static bool
strip_emulation_suffix(const char *search_path, size_t search_path_len,
						const char *schemas, size_t *base_len)
{
	size_t		schemas_len = strlen(schemas);

	if (search_path == NULL)
		return false;
	if (strcmp(search_path, schemas) == 0)
	{
		*base_len = 0;
		return true;
	}
	if (search_path_len >= schemas_len + 2)
	{
		const char *tail = search_path + (search_path_len - schemas_len);
		const char *sep = search_path + (search_path_len - schemas_len - 2);

		if (strcmp(tail, schemas) == 0 && strncmp(sep, ", ", 2) == 0)
		{
			*base_len = search_path_len - schemas_len - 2;
			return true;
		}
	}
	return false;
}

/*
 * Create a schema literally named after the current role, if one doesn't
 * already exist, so Postgres's own "$user" search_path entry starts
 * resolving. Isolated in its own subtransaction: a role without CREATE
 * privilege on the database is the common, expected case (most roles
 * aren't granted it), and that failure must degrade to ordinary shared-
 * 'public' behavior rather than aborting the SET db_emulation='oracle'
 * command that triggered it.
 */
static void
ensure_user_schema(void)
{
	char	   *username;
	HeapTuple	roletup;
	StringInfoData sql;
	MemoryContext oldcontext = CurrentMemoryContext;
	ResourceOwner oldowner = CurrentResourceOwner;

	if (!IsTransactionState())
		return;					/* e.g. applied at backend start before any transaction */

	roletup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(GetUserId()));
	if (!HeapTupleIsValid(roletup))
		return;					/* shouldn't happen for a live session's own role, but no crash either way */
	username = pstrdup(NameStr(((Form_pg_authid) GETSTRUCT(roletup))->rolname));
	ReleaseSysCache(roletup);

	if (get_namespace_oid(username, true) != InvalidOid)
		return;					/* already exists -- nothing to do, most callers after the first */

	BeginInternalSubTransaction("pg_oracle_ensure_user_schema");
	PG_TRY();
	{
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "pg_oracle: SPI_connect failed");

		initStringInfo(&sql);
		appendStringInfo(&sql, "CREATE SCHEMA %s AUTHORIZATION %s",
						 quote_identifier(username), quote_identifier(username));
		if (SPI_execute(sql.data, false, 0) != SPI_OK_UTILITY)
			elog(ERROR, "pg_oracle: unexpected result creating Oracle-style user schema");

		SPI_finish();
		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcontext);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		CurrentResourceOwner = oldowner;

		ereport(NOTICE,
				(errmsg("pg_oracle: could not auto-create Oracle-style schema \"%s\" (%s)",
						username, edata->message),
				 errdetail("Falling back to shared search_path resolution (e.g. public) -- "
						   "USER_*/DBA_*-owner-filtered views will not see this role's own "
						   "objects unless a schema named \"%s\" is created for it separately.",
						   username)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
}

static void
db_emulation_assign_hook(int newval, void *extra)
{
	const char *current_search_path;
	size_t		cur_len;
	const char *new_schemas = emulation_schemas_for(newval);
	size_t		base_len;
	bool		already_correct = false;
	StringInfoData buf;

	/*
	 * Deliberately NOT gated on "newval == db_emulation_mode" (that early
	 * return was the actual bug, found live via a real orawire+sqlcl
	 * connection -- see db/pg_oracle/README.md): search_path can be
	 * overwritten wholesale by code entirely outside this extension's
	 * control between two `SET db_emulation = '<mode>'` calls on what
	 * this GUC's own C-level state considers "the same session" --
	 * Warp's own LazyPooledConnection issues its own unconditional
	 * `SET search_path TO "<tenant>", public` the first time its Java
	 * wrapper object opens a (possibly pool-reused) physical connection,
	 * with no idea this extension's search_path append exists to
	 * preserve. If db_emulation_mode's enum value happens to already
	 * match on that same physical backend (a real, observed case: the
	 * backend process persists across what Warp's own code thinks
	 * are separate logical connections), the old "value didn't change,
	 * nothing to do" assumption was simply wrong -- search_path had
	 * already been reset out from under it by the time this hook ran
	 * again, and skipping the reconciliation left oracle_catalog/
	 * dbms_output/etc. missing for the rest of that session: every
	 * unqualified V$, DBA_*, or DBMS_* reference failed with ORA-00942/
	 * ORA-00904 even though `current_setting('db_emulation')` correctly
	 * still said 'oracle'.
	 *
	 * Fixed by reconciling against the ACTUAL current search_path text
	 * on every call, unconditionally: idempotent either way, so a
	 * genuinely repeated `SET db_emulation = '<mode>'` where nothing
	 * external touched search_path is just a cheap string-suffix check
	 * that changes nothing.
	 */
	current_search_path = GetConfigOption("search_path", false, false);
	cur_len = current_search_path ? strlen(current_search_path) : 0;
	base_len = cur_len;

	/*
	 * Strip off whichever known emulation suffix (if any) is currently
	 * active. Checking the NEW target's own suffix first lets the
	 * already-correct case short-circuit below; falling through to check
	 * every OTHER known mode's suffix handles switching directly between
	 * two emulation modes (oracle -> mysql with no intervening
	 * 'postgres') without stacking suffixes or leaving a stale
	 * sibling-mode suffix behind.
	 */
	if (new_schemas != NULL &&
		strip_emulation_suffix(current_search_path, cur_len, new_schemas, &base_len))
	{
		already_correct = true;
	}
	else if (strip_emulation_suffix(current_search_path, cur_len, ORACLE_EMULATION_SCHEMAS, &base_len))
	{
		/* base_len already set by the call above */
	}
	else if (strip_emulation_suffix(current_search_path, cur_len, MYSQL_EMULATION_SCHEMAS, &base_len))
	{
		/* base_len already set by the call above */
	}
	else if (strip_emulation_suffix(current_search_path, cur_len, SQLSERVER_EMULATION_SCHEMAS, &base_len))
	{
		/* base_len already set by the call above */
	}

	if (already_correct)
	{
		db_emulation_mode = newval;
		return;					/* already exactly the desired state -- nothing to reconcile */
	}

	initStringInfo(&buf);
	if (base_len > 0)
		appendBinaryStringInfo(&buf, current_search_path, base_len);

	if (new_schemas != NULL)
	{
		/* Appended, not prepended: search_path's first existing entry is
		 * also where an unqualified CREATE TABLE/FUNCTION/etc. lands.
		 * Prepending oracle_catalog/dbms_output/utl_file (or
		 * mysql_catalog) would silently redirect a plain `CREATE TABLE
		 * foo(...)` into this extension's own schemas instead of the
		 * caller's -- found live: a CREATE TABLE issued right after SET
		 * db_emulation='oracle' landed in dbms_output instead of public.
		 * Appending keeps unqualified *lookups* working (DBMS_OUTPUT.
		 * PUT_LINE, V$SESSION still resolve) without touching where new
		 * objects are created. */
		if (buf.len > 0)
			appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, new_schemas);

		if (newval == DB_EMULATION_ORACLE)
			ensure_user_schema();	/* Oracle-specific: schema-per-role, see this function's own header comment */
	}

	db_emulation_mode = newval;

	/* buf.data can legitimately be an empty string here (switching to
	 * 'postgres' when search_path was empty before any emulation mode
	 * ever applied) and still need to be applied -- SET search_path to ''
	 * is a real, valid (if unusual) state, distinct from "don't touch
	 * it", so this can't be guarded on buf.len > 0. */
	SetConfigOption("search_path", buf.data, PGC_USERSET, PGC_S_SESSION);

	pfree(buf.data);
}

void
_PG_init(void)
{
	DefineCustomEnumVariable(
		"db_emulation",
		"Which foreign database's client-visible surface this session should emulate "
		"(V$/GV$/DBA_* views, DBMS_*/UTL_* packages). Set by Warp's protocol "
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

/* Exported for db/pg_mysql's own SQL script (mysql_catalog.emulation_active())
 * -- pg_mysql has no C module of its own; db_emulation has to live in
 * exactly one, see MYSQL_EMULATION_SCHEMAS's own comment above. */
PG_FUNCTION_INFO_V1(pg_mysql_emulation_active);
Datum
pg_mysql_emulation_active(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(db_emulation_mode == DB_EMULATION_MYSQL);
}

/* Exported for db/pg_sqlserver's own SQL script (sys.emulation_active()) --
 * pg_sqlserver has no C module of its own either, same reason. */
PG_FUNCTION_INFO_V1(pg_sqlserver_emulation_active);
Datum
pg_sqlserver_emulation_active(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(db_emulation_mode == DB_EMULATION_SQLSERVER);
}
