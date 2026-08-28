/*
 * dbms_output.c -- Oracle's DBMS_OUTPUT package: PUT_LINE/PUT/NEW_LINE,
 * GET_LINE/GET_LINES, ENABLE/DISABLE. Picked as the first of the top-20
 * DBMS_* packages to implement in-house (see db/pg_oracle/README.md) rather
 * than pulling in orafce for it, because its whole job is holding a
 * session-lifetime FIFO of text lines -- state a SQL/plpgsql function can't
 * hold on its own, so it needs the same session-memory-context approach as
 * db_emulation, just doing something else with it. The rest of the top-20
 * list (DBMS_UTILITY, DBMS_RANDOM, DBMS_CRYPTO, UTL_*, ...) has no such
 * state requirement and belongs in plain SQL/plpgsql in
 * sql/pg_oracle--0.1.sql instead -- don't reflexively add more C here.
 *
 * Real Oracle behavior this preserves: the buffer is off by default: a
 * PUT_LINE before ENABLE() is silently dropped, matching Oracle exactly
 * (a very common "why don't I see my output" surprise worth keeping
 * faithful to, not fixing).
 */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "miscadmin.h"

typedef struct OutputLine
{
	char	   *text;
	struct OutputLine *next;
} OutputLine;

static bool buffer_enabled = false;
static int	buffer_max_bytes = 20000;	/* Oracle's pre-10g default cap */
static int	buffer_used_bytes = 0;
static OutputLine *buffer_head = NULL;
static OutputLine *buffer_tail = NULL;
static MemoryContext dbms_output_mcxt = NULL;

static void
ensure_mcxt(void)
{
	if (dbms_output_mcxt == NULL)
		dbms_output_mcxt = AllocSetContextCreate(TopMemoryContext,
												  "dbms_output buffer",
												  ALLOCSET_SMALL_SIZES);
}

static void
reset_buffer(void)
{
	buffer_head = buffer_tail = NULL;
	buffer_used_bytes = 0;
	if (dbms_output_mcxt != NULL)
		MemoryContextReset(dbms_output_mcxt);
}

PG_FUNCTION_INFO_V1(dbms_output_enable);
Datum
dbms_output_enable(PG_FUNCTION_ARGS)
{
	ensure_mcxt();
	buffer_enabled = true;
	buffer_max_bytes = PG_ARGISNULL(0) ? 0 /* Oracle: NULL = unlimited */
										: PG_GETARG_INT32(0);
	reset_buffer();
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_disable);
Datum
dbms_output_disable(PG_FUNCTION_ARGS)
{
	buffer_enabled = false;
	reset_buffer();
	PG_RETURN_VOID();
}

static void
append_line(const char *line_text)
{
	OutputLine *line;
	MemoryContext oldcxt;
	int			len;

	if (!buffer_enabled)
		return;					/* faithful to Oracle: silently dropped */

	len = line_text ? (int) strlen(line_text) : 0;
	if (buffer_max_bytes > 0 && buffer_used_bytes + len > buffer_max_bytes)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("ORU-10027: buffer overflow, limit of %d bytes",
						buffer_max_bytes)));

	ensure_mcxt();
	oldcxt = MemoryContextSwitchTo(dbms_output_mcxt);
	line = palloc(sizeof(OutputLine));
	line->text = line_text ? pstrdup(line_text) : NULL;
	line->next = NULL;
	MemoryContextSwitchTo(oldcxt);

	if (buffer_tail == NULL)
		buffer_head = buffer_tail = line;
	else
	{
		buffer_tail->next = line;
		buffer_tail = line;
	}
	buffer_used_bytes += len;
}

PG_FUNCTION_INFO_V1(dbms_output_put_line);
Datum
dbms_output_put_line(PG_FUNCTION_ARGS)
{
	char	   *line_text = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(0));

	append_line(line_text);
	PG_RETURN_VOID();
}

/* PUT() appends without a line break -- Oracle joins it with whatever the
 * next PUT_LINE/NEW_LINE contributes. Approximated here as: extend the
 * current tail line rather than starting a new one (matches the common
 * PUT(...); PUT(...); NEW_LINE(); calling pattern; doesn't try to replicate
 * every edge case of Oracle's internal partial-line state). */
PG_FUNCTION_INFO_V1(dbms_output_put);
Datum
dbms_output_put(PG_FUNCTION_ARGS)
{
	char	   *line_text = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (!buffer_enabled || line_text == NULL)
	{
		PG_RETURN_VOID();
	}

	if (buffer_tail != NULL)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(dbms_output_mcxt);
		char	   *joined = psprintf("%s%s", buffer_tail->text ? buffer_tail->text : "", line_text);

		buffer_used_bytes += (int) strlen(line_text);
		buffer_tail->text = joined;
		MemoryContextSwitchTo(oldcxt);
	}
	else
		append_line(line_text);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_new_line);
Datum
dbms_output_new_line(PG_FUNCTION_ARGS)
{
	if (buffer_tail == NULL)
		append_line("");
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(dbms_output_get_line);
Datum
dbms_output_get_line(PG_FUNCTION_ARGS)
{
	/* out params: line text, status (0 = a line was returned, 1 = buffer empty) */
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false, false};
	HeapTuple	tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "dbms_output_get_line: composite result type required");
	tupdesc = BlessTupleDesc(tupdesc);

	if (buffer_head == NULL)
	{
		nulls[0] = true;
		values[1] = Int32GetDatum(1);
	}
	else
	{
		OutputLine *line = buffer_head;

		values[0] = line->text ? PointerGetDatum(cstring_to_text(line->text)) : (Datum) 0;
		nulls[0] = (line->text == NULL);
		values[1] = Int32GetDatum(0);

		buffer_head = line->next;
		if (buffer_head == NULL)
			buffer_tail = NULL;
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
