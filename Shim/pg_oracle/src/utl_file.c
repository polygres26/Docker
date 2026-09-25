/*
 * utl_file.c -- Oracle's UTL_FILE package: FOPEN/PUT_LINE/PUT/NEW_LINE/
 * GET_LINE/FCLOSE/FCLOSE_ALL/IS_OPEN.
 *
 * Needs C for two reasons real file descriptors are session-lifetime
 * state (same category as DBMS_OUTPUT's buffer, see that file), and the
 * open()/read()/write() calls themselves aren't reachable from plain SQL
 * at all without either a C extension or an untrusted procedural
 * language (plperlu/plpythonu) -- and this needs to run as trusted-looking
 * SQL for an ordinary migrated application, not require installing an
 * untrusted PL.
 *
 * Security model -- deliberately NOT reinvented from scratch: rather than
 * building our own directory-object grant table the way Oracle does
 * (CREATE DIRECTORY, then GRANT READ/WRITE ON DIRECTORY ... TO ...), this
 * gates directory creation and file access on Postgres's own predefined
 * roles pg_read_server_files / pg_write_server_files (built in since
 * PG 11 for exactly this "let a non-superuser touch the filesystem
 * safely" purpose -- see CREATE ROLE's own docs). A DBA grants a role
 * membership in one of those the same way they'd grant an Oracle
 * directory-object privilege; this extension doesn't add a second,
 * parallel permission system on top. utl_file.directories itself only
 * maps a name to a path -- no privilege lives in that table.
 *
 * Path safety: a filename is rejected outright if it contains '/' or
 * '..' -- there is no legitimate UTL_FILE.FOPEN call that needs either,
 * and allowing them would let a caller escape the registered directory
 * entirely (the exact class of bug a directory-object model exists to
 * prevent).
 */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "catalog/pg_authid.h"

#include <stdio.h>
#include <string.h>

typedef struct UtlFileHandle
{
	FILE	   *fp;
	bool		writable;
	bool		in_use;
} UtlFileHandle;

#define UTL_FILE_MAX_HANDLES 64
static UtlFileHandle handles[UTL_FILE_MAX_HANDLES];

/* Oracle's directory-privilege split, reused rather than reinvented --
 * see this file's header comment. Superuser always passes, matching
 * every other Postgres filesystem-access check (COPY, pg_read_file). */
static bool
caller_has_file_role(bool require_write)
{
	if (superuser())
		return true;
	if (require_write)
		return has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES);
	return has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES) ||
		has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES);
}

static char *
current_role_name(void)
{
	HeapTuple	roletup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(GetUserId()));
	char	   *name;

	if (!HeapTupleIsValid(roletup))
		return "?";
	name = pstrdup(NameStr(((Form_pg_authid) GETSTRUCT(roletup))->rolname));
	ReleaseSysCache(roletup);
	return name;
}

static char *
lookup_directory_path(const char *directory)
{
	char	   *path = NULL;
	StringInfoData sql;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "pg_oracle: SPI_connect failed in UTL_FILE");

	initStringInfo(&sql);
	appendStringInfo(&sql,
					  "SELECT path FROM utl_file.directories WHERE directory_name = %s",
					  quote_literal_cstr(directory));
	if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		isnull;
		Datum		d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);

		if (!isnull)
			path = pstrdup(TextDatumGetCString(d));
	}
	SPI_finish();

	if (path == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("ORA-29280: invalid directory path"),
				 errdetail("No UTL_FILE directory object named \"%s\" (see utl_file.create_directory).",
						   directory)));
	return path;
}

static void
reject_unsafe_filename(const char *filename)
{
	if (filename == NULL || filename[0] == '\0' ||
		strchr(filename, '/') != NULL || strstr(filename, "..") != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ORA-29280: invalid filename"),
				 errdetail("Filenames may not contain '/' or '..' -- they must name a file directly inside the registered directory, not a path.")));
}

PG_FUNCTION_INFO_V1(utl_file_fopen);
Datum
utl_file_fopen(PG_FUNCTION_ARGS)
{
	char	   *directory = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *filename = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *open_mode = text_to_cstring(PG_GETARG_TEXT_PP(2));
	char	   *dirpath;
	char		fullpath[MAXPGPATH];
	const char *fmode;
	bool		writable;
	FILE	   *fp;
	int			i;

	if (strcmp(open_mode, "r") == 0)
	{
		writable = false;
		fmode = "r";
	}
	else if (strcmp(open_mode, "w") == 0)
	{
		writable = true;
		fmode = "w";
	}
	else if (strcmp(open_mode, "a") == 0)
	{
		writable = true;
		fmode = "a";
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ORA-29283: invalid open mode \"%s\"", open_mode),
				 errdetail("pg_oracle's UTL_FILE supports 'r', 'w', 'a' (text modes) in this version -- not 'rb'/'wb'/'ab'.")));

	if (!caller_has_file_role(writable))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("ORA-29283: invalid operation"),
				 errdetail("Role \"%s\" is not a member of pg_%s_server_files (or superuser).",
						   current_role_name(), writable ? "write" : "read")));

	reject_unsafe_filename(filename);
	dirpath = lookup_directory_path(directory);
	snprintf(fullpath, sizeof(fullpath), "%s/%s", dirpath, filename);

	for (i = 0; i < UTL_FILE_MAX_HANDLES; i++)
		if (!handles[i].in_use)
			break;
	if (i == UTL_FILE_MAX_HANDLES)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("ORA-29283: too many open UTL_FILE handles (max %d per session)",
						UTL_FILE_MAX_HANDLES)));

	fp = fopen(fullpath, fmode);
	if (fp == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("ORA-29283: invalid file operation"),
				 errdetail("Could not open \"%s\": %m", fullpath)));

	handles[i].fp = fp;
	handles[i].writable = writable;
	handles[i].in_use = true;

	PG_RETURN_INT32(i + 1);		/* 1-based, so 0/NULL reads as "no handle" */
}

static UtlFileHandle *
get_handle(int32 handle_id, bool require_writable)
{
	int			idx = handle_id - 1;

	if (idx < 0 || idx >= UTL_FILE_MAX_HANDLES || !handles[idx].in_use)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("ORA-06508: invalid file handle"),
				 errdetail("Handle %d is not open in this session (already closed, or never opened).",
						   handle_id)));
	if (require_writable && !handles[idx].writable)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("ORA-29283: file opened for read, not write")));
	return &handles[idx];
}

PG_FUNCTION_INFO_V1(utl_file_is_open);
Datum
utl_file_is_open(PG_FUNCTION_ARGS)
{
	int32		handle_id = PG_GETARG_INT32(0);
	int			idx = handle_id - 1;

	PG_RETURN_BOOL(idx >= 0 && idx < UTL_FILE_MAX_HANDLES && handles[idx].in_use);
}

PG_FUNCTION_INFO_V1(utl_file_put);
Datum
utl_file_put(PG_FUNCTION_ARGS)
{
	UtlFileHandle *h = get_handle(PG_GETARG_INT32(0), true);
	char	   *buffer = PG_ARGISNULL(1) ? "" : text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (fputs(buffer, h->fp) == EOF)
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("ORA-29285: file write error: %m")));
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(utl_file_put_line);
Datum
utl_file_put_line(PG_FUNCTION_ARGS)
{
	UtlFileHandle *h = get_handle(PG_GETARG_INT32(0), true);
	char	   *buffer = PG_ARGISNULL(1) ? "" : text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (fputs(buffer, h->fp) == EOF || fputc('\n', h->fp) == EOF)
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("ORA-29285: file write error: %m")));
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(utl_file_new_line);
Datum
utl_file_new_line(PG_FUNCTION_ARGS)
{
	UtlFileHandle *h = get_handle(PG_GETARG_INT32(0), true);
	int32		lines = PG_ARGISNULL(1) ? 1 : PG_GETARG_INT32(1);

	while (lines-- > 0)
		if (fputc('\n', h->fp) == EOF)
			ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg("ORA-29285: file write error: %m")));
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(utl_file_get_line);
Datum
utl_file_get_line(PG_FUNCTION_ARGS)
{
	UtlFileHandle *h = get_handle(PG_GETARG_INT32(0), false);
	char		linebuf[4096];

	if (fgets(linebuf, sizeof(linebuf), h->fp) == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("ORA-01403: no data found"),
				 errdetail("UTL_FILE.GET_LINE reached end of file -- catch this exactly like a "
						   "missing SELECT INTO row: EXCEPTION WHEN NO_DATA_FOUND THEN ...")));

	linebuf[strcspn(linebuf, "\n")] = '\0';		/* Oracle's GET_LINE excludes the newline */
	PG_RETURN_TEXT_P(cstring_to_text(linebuf));
}

PG_FUNCTION_INFO_V1(utl_file_fclose);
Datum
utl_file_fclose(PG_FUNCTION_ARGS)
{
	int32		handle_id = PG_GETARG_INT32(0);
	int			idx = handle_id - 1;

	if (idx >= 0 && idx < UTL_FILE_MAX_HANDLES && handles[idx].in_use)
	{
		fclose(handles[idx].fp);
		handles[idx].in_use = false;
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(utl_file_fclose_all);
Datum
utl_file_fclose_all(PG_FUNCTION_ARGS)
{
	int			i;

	for (i = 0; i < UTL_FILE_MAX_HANDLES; i++)
		if (handles[i].in_use)
		{
			fclose(handles[i].fp);
			handles[i].in_use = false;
		}
	PG_RETURN_VOID();
}
