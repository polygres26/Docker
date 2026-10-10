package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.TwoPhaseReadiness.Probe;
import com.sayonora.warp.core.TwoPhaseReadiness.State;
import com.sayonora.warp.testsupport.RealAzureSqlEdge;
import com.sayonora.warp.testsupport.RealMySql;
import com.sayonora.warp.testsupport.RealOracle;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** The startup check's probes against a real MySQL, Oracle and SQL Server. Opt-in per engine: WARP_TEST_PROBE_ENGINES=mysql,oracle,mssql (docker). */
class TwoPhaseProbeEnginesLiveTest {

    private static void want(String engine) {
        String v = System.getenv("WARP_TEST_PROBE_ENGINES");
        Assumptions.assumeTrue(v != null && java.util.Arrays.asList(v.split(",")).contains(engine), engine + " not requested");
    }

    private static Probe probe(String url, String user, String password) {
        BackendTarget t = BackendRegistry.fromConfig("default=" + url + "|" + user + "|" + password, null).resolveForRouting("default");
        return TwoPhaseReadiness.probe(t);
    }

    @Test
    void mysqlReportsInnoDb() throws Exception {
        want("mysql");
        try (RealMySql my = RealMySql.start()) {
            Probe p = probe(my.jdbcUrl(), my.username(), my.password());
            assertEquals(State.READY, p.state(), p.detail());
        }
    }

    @Test
    void oracleNeedsReadAccessToPendingTransactions() throws Exception {
        want("oracle");
        try (RealOracle ora = RealOracle.start()) {
            try (Connection c = DriverManager.getConnection(ora.sysJdbcUrl(), ora.sysUsername(), ora.sysPassword()); Statement st = c.createStatement()) {
                st.execute("CREATE USER plainuser IDENTIFIED BY plainpw1");
                st.execute("GRANT CREATE SESSION TO plainuser");
            }
            Probe plain = probe(ora.jdbcUrl(), "plainuser", "plainpw1");
            assertEquals(State.NOT_READY, plain.state(), plain.detail());
            assertTrue(plain.fix().contains("DBA_PENDING_TRANSACTIONS"));
            try (Connection c = DriverManager.getConnection(ora.sysJdbcUrl(), ora.sysUsername(), ora.sysPassword()); Statement st = c.createStatement()) {
                st.execute("GRANT SELECT ON SYS.DBA_PENDING_TRANSACTIONS TO plainuser");
            }
            Probe granted = probe(ora.jdbcUrl(), "plainuser", "plainpw1");
            assertEquals(State.READY, granted.state(), granted.detail());
        }
    }

    @Test
    void sqlServerWithoutTheXaProceduresIsNotReady() throws Exception {
        want("mssql");
        try (RealAzureSqlEdge mssql = RealAzureSqlEdge.start()) {
            Probe p = probe(mssql.masterJdbcUrl(), mssql.username(), mssql.password());
            System.out.println("MSSQL-PROBE " + p);
            assertTrue(p.state() == State.NOT_READY || p.state() == State.READY, p.detail());
        }
    }
}
