package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** What may leave the primary. Every "false" here is a statement that, sent to a replica, would
 * either fail, take a lock the primary never sees, or silently return a session-local/stale answer. */
class StatementClassifierTest {

    private static void safe(String sql) {
        assertTrue(StatementClassifier.isReplicaSafeRead(sql), "expected replica-safe: " + sql);
    }

    private static void unsafe(String sql) {
        assertFalse(StatementClassifier.isReplicaSafeRead(sql), "expected primary-only: " + sql);
    }

    @Test
    void plainSelectsAreSafe() {
        safe("select * from orders where id = 7");
        safe("SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY a LIMIT 10;");
        safe("  select 1");
        safe("with recent as (select * from orders where ts > now() - interval '1 day') select count(*) from recent");
        safe("select count(*) from updated_orders");
        safe("select created_at, updated_at, set_name from t");
    }

    @Test
    void lockingReadsStayOnThePrimary() {
        unsafe("select * from t where id = 1 for update");
        unsafe("SELECT * FROM t FOR NO KEY UPDATE");
        unsafe("select * from t for share");
        unsafe("select * from t for key share");
        unsafe("select * from t lock in share mode");
        unsafe("select * from t with (updlock) where id = 1");
        unsafe("select * from t where id = 1 for update skip locked");
    }

    @Test
    void selectIntoAndWritableCtesStayOnThePrimary() {
        unsafe("select * into backup from t");
        unsafe("select * into outfile '/tmp/x' from t");
        unsafe("with d as (delete from t returning *) select * from d");
        unsafe("with i as (insert into t values (1) returning id) select * from i");
        unsafe("with u as (update t set a = 1 returning *) select * from u");
    }

    @Test
    void sideEffectAndSessionLocalFunctionsStayOnThePrimary() {
        unsafe("select nextval('seq')");
        unsafe("select setval('seq', 10)");
        unsafe("select currval('seq')");
        unsafe("select lastval()");
        unsafe("select set_config('a.b', 'c', false)");
        unsafe("select pg_advisory_lock(1)");
        unsafe("select pg_try_advisory_xact_lock(1)");
        unsafe("select txid_current()");
        unsafe("select last_insert_id()");
        unsafe("select get_lock('x', 1)");
        unsafe("select my_seq.nextval from dual");
        unsafe("select next value for dbo.s");
        unsafe("select scope_identity()");
        unsafe("select dblink('c', 'select 1')");
        unsafe("select dbms_random.value from dual");
    }

    @Test
    void nonSelectsAndUtilityStatementsStayOnThePrimary() {
        unsafe("insert into t values (1)");
        unsafe("update t set a = 1");
        unsafe("delete from t");
        unsafe("merge into t using s on (t.id = s.id) when matched then update set a = 1");
        unsafe("create table x (a int)");
        unsafe("show search_path");
        unsafe("explain analyze select * from t");
        unsafe("set search_path = foo");
        unsafe("call p()");
        unsafe("begin");
        unsafe("");
        unsafe(null);
        unsafe("   ");
    }

    @Test
    void multipleStatementsInOneStringStayOnThePrimary() {
        unsafe("select 1; select 2");
        unsafe("select 1; delete from t");
    }

    @Test
    void keywordsInsideLiteralsAndCommentsDoNotMatter() {
        safe("select 'for update' as note from t");
        safe("select * from t where name = 'insert into x'");
        safe("select * /* delete from t */ from t");
        safe("select * from t -- for update\n where id = 1");
        safe("select \"update\" from t");
        safe("select $$ nextval $$ as s");
        safe("select 'it''s a nextval(x)' from t");
    }

    @Test
    void hidingAKeywordBehindAnUnterminatedQuoteDoesNotWork() {
        unsafe("select * from t where a = 'x for update");
        unsafe("select * from t /* for update");
    }

    @Test
    void caseInsensitive() {
        unsafe("SeLeCt * FrOm t FoR uPdAtE");
        unsafe("SELECT NEXTVAL('s')");
    }
}
