/*
 * utl_http.c -- Oracle's UTL_HTTP package, gated by DBMS_NETWORK_ACL_ADMIN
 * exactly the way real Oracle requires (UTL_HTTP.REQUEST raises ORA-24247
 * if the caller has no 'connect' privilege on an ACL assigned to the
 * target host/port -- there is no way to make an HTTP call from PL/SQL
 * without an ACL grant, by Oracle's own design since 11g). This file is
 * the one piece that has to be C: making an outbound socket connection
 * isn't reachable from SQL/plpgsql at all.
 *
 * Deliberate division of responsibility, same principle as db_emulation
 * (C) vs. the views (SQL) and utl_file (C) vs. its directory grants
 * (SQL): the ACL *policy* -- host/port specificity resolution, grant/deny
 * list-order semantics -- lives entirely in
 * sql/pg_oracle--0.1.sql's dbms_network_acl_admin functions, in
 * inspectable, independently testable plpgsql. This file only does the
 * two things SQL genuinely cannot: parse a URL well enough to get a
 * host/port to check, and make the actual libcurl call once permitted.
 * It calls back into that SQL policy via SPI before ever opening a
 * socket -- the check is not duplicated or approximated here.
 */
#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "catalog/pg_authid.h"

#include <curl/curl.h>
#include <string.h>
#include <stdlib.h>

typedef struct HttpBuffer
{
	char	   *data;
	size_t		len;
	size_t		cap;
} HttpBuffer;

/* Simplification, documented in README.md: real UTL_HTTP exposes a
 * request/response handle pair (BEGIN_REQUEST/GET_RESPONSE/READ_TEXT) for
 * streaming and per-header access; this tracks only the most recent
 * call's status in a session-lifetime variable, enough for the extremely
 * common "call REQUEST, then check the status" pattern. */
static int	last_http_status = 0;

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

static size_t
curl_write_cb(char *ptr, size_t size, size_t nmemb, void *userdata)
{
	HttpBuffer *buf = (HttpBuffer *) userdata;
	size_t		add = size * nmemb;

	if (buf->len + add + 1 > buf->cap)
	{
		size_t		newcap = buf->cap == 0 ? 8192 : buf->cap * 2;

		while (newcap < buf->len + add + 1)
			newcap *= 2;
		buf->data = realloc(buf->data, newcap);
		if (buf->data == NULL)
			return 0;			/* signals an error to curl */
		buf->cap = newcap;
	}
	memcpy(buf->data + buf->len, ptr, add);
	buf->len += add;
	buf->data[buf->len] = '\0';
	return add;
}

/*
 * Extract host and port from a URL well enough for an ACL lookup --
 * doesn't need to be a full URL-correctness validator (libcurl itself
 * will reject a genuinely malformed URL when the request actually runs),
 * just accurate about what host/port a request will actually reach.
 */
static void
extract_host_port(const char *url, char **host_out, int *port_out)
{
	CURLU	   *h = curl_url();
	char	   *host = NULL,
			   *scheme = NULL,
			   *portstr = NULL;
	CURLUcode	rc;

	if (h == NULL || curl_url_set(h, CURLUPART_URL, url, 0) != CURLUE_OK)
	{
		if (h)
			curl_url_cleanup(h);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ORA-29273: HTTP request failed"),
				 errdetail("\"%s\" is not a valid URL.", url)));
	}

	curl_url_get(h, CURLUPART_HOST, &host, 0);
	curl_url_get(h, CURLUPART_SCHEME, &scheme, 0);
	rc = curl_url_get(h, CURLUPART_PORT, &portstr, 0);

	if (host == NULL)
	{
		curl_url_cleanup(h);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("ORA-29273: HTTP request failed"),
				 errdetail("Could not determine a host from \"%s\".", url)));
	}

	*host_out = pstrdup(host);
	if (rc == CURLUE_OK && portstr != NULL)
		*port_out = atoi(portstr);
	else if (scheme != NULL && strcasecmp(scheme, "https") == 0)
		*port_out = 443;
	else
		*port_out = 80;			/* the only two schemes UTL_HTTP realistically sees */

	curl_free(host);
	if (scheme)
		curl_free(scheme);
	if (portstr)
		curl_free(portstr);
	curl_url_cleanup(h);
}

/*
 * The one and only gate: raises ORA-24247 unless
 * dbms_network_acl_admin.check_privilege_for_host() says this role has
 * 'connect' on an ACL covering this host/port. NULL and 0 are both
 * "not permitted" -- deny-by-default, same as real Oracle when no ACL
 * covers a host at all.
 */
static void
check_network_acl(const char *host, int port)
{
	StringInfoData sql;
	int			ret;
	bool		permitted = false;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "pg_oracle: SPI_connect failed in UTL_HTTP");

	initStringInfo(&sql);
	appendStringInfo(&sql,
					  "SELECT dbms_network_acl_admin.check_privilege_for_host(%s, %d, %s, 'connect')",
					  quote_literal_cstr(host), port, quote_literal_cstr(current_role_name()));
	ret = SPI_execute(sql.data, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		isnull;
		Datum		d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);

		permitted = !isnull && DatumGetInt32(d) == 1;
	}
	SPI_finish();

	if (!permitted)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("ORA-24247: network access denied by access control list (ACL)"),
				 errdetail("No ACL grants role \"%s\" CONNECT to %s:%d. "
						   "See dbms_network_acl_admin.create_acl()/assign_acl().",
						   current_role_name(), host, port)));
}

PG_FUNCTION_INFO_V1(utl_http_request);
Datum
utl_http_request(PG_FUNCTION_ARGS)
{
	char	   *url = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *method = PG_ARGISNULL(1) ? "GET" : text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *host;
	int			port;
	CURL	   *curl;
	CURLcode	res;
	HttpBuffer	body = {0};
	long		http_status = 0;
	text	   *result;

	extract_host_port(url, &host, &port);
	check_network_acl(host, port);

	curl = curl_easy_init();
	if (curl == NULL)
		ereport(ERROR, (errmsg("ORA-29273: HTTP request failed"), errdetail("libcurl initialization failed.")));

	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
	curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(curl, CURLOPT_USERAGENT, "pg_oracle-utl_http/0.1 (Polygres)");
	if (strcasecmp(method, "POST") == 0)
		curl_easy_setopt(curl, CURLOPT_POST, 1L);
	else if (strcasecmp(method, "HEAD") == 0)
		curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
	else if (strcasecmp(method, "GET") != 0)
		curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method);

	res = curl_easy_perform(curl);
	if (res == CURLE_OK)
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_status);
	curl_easy_cleanup(curl);

	if (res != CURLE_OK)
	{
		const char *errstr = curl_easy_strerror(res);

		if (body.data)
			free(body.data);
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("ORA-29273: HTTP request failed"),
				 errdetail("%s", errstr)));
	}

	last_http_status = (int) http_status;
	result = cstring_to_text(body.data ? body.data : "");
	if (body.data)
		free(body.data);

	PG_RETURN_TEXT_P(result);
}

PG_FUNCTION_INFO_V1(utl_http_last_status);
Datum
utl_http_last_status(PG_FUNCTION_ARGS)
{
	if (last_http_status == 0)
		PG_RETURN_NULL();
	PG_RETURN_INT32(last_http_status);
}
