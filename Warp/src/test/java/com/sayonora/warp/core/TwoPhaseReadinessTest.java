package com.sayonora.warp.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.TwoPhaseReadiness.Probe;
import com.sayonora.warp.core.TwoPhaseReadiness.State;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class TwoPhaseReadinessTest {

    @Test
    void postgresIsNotReadyWhenPreparedTransactionsAreOff() {
        Probe off = TwoPhaseReadiness.interpret(SourceDialect.POSTGRES, "0");
        assertEquals(State.NOT_READY, off.state());
        assertTrue(off.fix().contains("max_prepared_transactions"));
        assertEquals(State.READY, TwoPhaseReadiness.interpret(SourceDialect.POSTGRES, "50").state());
    }

    @Test
    void sqlServerNeedsTheXaProcedures() {
        assertEquals(State.NOT_READY, TwoPhaseReadiness.interpret(SourceDialect.SQL_SERVER, "0").state());
        assertEquals(State.READY, TwoPhaseReadiness.interpret(SourceDialect.SQL_SERVER, "1").state());
    }

    @Test
    void anUnreadableValueIsUnknownNeverReady() {
        assertEquals(State.UNKNOWN, TwoPhaseReadiness.interpret(SourceDialect.POSTGRES, "on").state());
        assertEquals(State.UNKNOWN, TwoPhaseReadiness.interpret(SourceDialect.MYSQL, null).state());
    }

    @Test
    void onlyTablesSpanningSeveralBackendsAreChecked() {
        BackendRegistry registry = BackendRegistry.fromConfig("default=jdbc:postgresql://h/d|u|p;s1=jdbc:postgresql://h/a|u|p;s2=jdbc:postgresql://h/b|u|p", null);
        var spread = new RouterStage.TableShardRule("orders", Pattern.compile("\\borders\\b"), "id", ShardingStrategy.hash(List.of("s1", "s2")));
        var single = new RouterStage.TableShardRule("solo", Pattern.compile("\\bsolo\\b"), "id", ShardingStrategy.hash(List.of("default")));
        var probed = new java.util.ArrayList<String>();
        var reports = new TwoPhaseReadiness(registry, () -> List.of(spread, single), t -> {
            probed.add(t.name());
            return new Probe(t.name().equals("s2") ? State.NOT_READY : State.READY, "x", "fix it");
        }).check();
        assertEquals(List.of("s1", "s2"), probed);
        assertEquals(State.READY, reports.get(0).state());
        assertEquals(State.NOT_READY, reports.get(1).state());
        assertEquals(List.of("orders"), reports.get(1).tables());
    }

    @Test
    void aProbeThatThrowsIsUnknown() {
        BackendRegistry registry = BackendRegistry.fromConfig("default=jdbc:postgresql://h/d|u|p;s1=jdbc:postgresql://h/a|u|p;s2=jdbc:postgresql://h/b|u|p", null);
        var rule = new RouterStage.TableShardRule("orders", Pattern.compile("\\borders\\b"), "id", ShardingStrategy.hash(List.of("s1", "s2")));
        var reports = new TwoPhaseReadiness(registry, () -> List.of(rule), t -> {
            throw new IllegalStateException("boom");
        }).check();
        assertEquals(State.UNKNOWN, reports.get(0).state());
    }
}
