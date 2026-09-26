package com.sayonora.wire.gremlinwire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The script parser, the traversal engine and the heap store on the standard "modern" toy graph. */
class GremlinInterpreterTest {

    private MemGraph store;

    @BeforeEach
    void modern() {
        store = new MemGraph();
        run("g.addV('person').property(T.id,1L).property('name','marko').property('age',29).next()");
        run("g.addV('person').property(T.id,2L).property('name','vadas').property('age',27).next()");
        run("g.addV('software').property(T.id,3L).property('name','lop').property('lang','java').next()");
        run("g.addV('person').property(T.id,4L).property('name','josh').property('age',32).next()");
        run("g.addV('software').property(T.id,5L).property('name','ripple').property('lang','java').next()");
        run("g.addV('person').property(T.id,6L).property('name','peter').property('age',35).next()");
        run("g.V(1L).addE('knows').to(__.V(2L)).property(T.id,7L).property('weight',0.5d).iterate()");
        run("g.V(1L).addE('knows').to(__.V(4L)).property(T.id,8L).property('weight',1.0d).iterate()");
        run("g.V(1L).addE('created').to(__.V(3L)).property(T.id,9L).property('weight',0.4d).iterate()");
        run("g.V(4L).addE('created').to(__.V(5L)).property(T.id,10L).property('weight',1.0d).iterate()");
        run("g.V(4L).addE('created').to(__.V(3L)).property(T.id,11L).property('weight',0.4d).iterate()");
        run("g.V(6L).addE('created').to(__.V(3L)).property(T.id,12L).property('weight',0.2d).iterate()");
    }

    private List<Object> run(String script) {
        Script.Session ss = new Script.Session(store, false, 0);
        Script.Env env = Script.newRoot(ss, null);
        Object r = Script.eval(script, env);
        Iterator<Object> it = Processor.resultItems(r, ss, env);
        List<Object> out = new ArrayList<>();
        while (it.hasNext()) {
            out.add(it.next());
        }
        return out;
    }

    private Object one(String script) {
        List<Object> l = run(script);
        assertEquals(1, l.size(), script + " -> " + l);
        return l.get(0);
    }

    @Test
    void countsAndFilters() {
        assertEquals(6L, one("g.V().count()"));
        assertEquals(6L, one("g.E().count()"));
        assertEquals(4L, one("g.V().hasLabel('person').count()"));
        assertEquals(List.of("josh", "peter"), run("g.V().has('age',gt(30)).values('name')"));
        assertEquals(List.of("marko"), run("g.V().has('name',regex('ma')).values('name')"));
        assertEquals(1L, one("g.V().has('age',between(27,29)).count()"));
        assertEquals(3L, one("g.V().has('age',inside(26,33)).count()"));
    }

    @Test
    void navigationKeepsTinkerGraphOrder() {
        // created before knows: TinkerGraph iterates a HashMap of edge labels
        assertEquals(List.of("lop", "vadas", "josh"), run("g.V(1).out().values('name')"));
        assertEquals(List.of("marko", "josh", "peter"), run("g.V(3).in().values('name')"));
    }

    @Test
    void groupingProjectionAndPaths() {
        Map<?, ?> m = (Map<?, ?>) one("g.V().groupCount().by(label)");
        assertEquals(4L, m.get("person"));
        assertEquals(2L, m.get("software"));
        assertEquals(List.of("marko", "lop"), ((G.Path) run("g.V(1).out('created').path().by('name')").get(0)).objects);
        Map<?, ?> p = (Map<?, ?>) one("g.V(1).project('n','c').by('name').by(out().count())");
        assertEquals("marko", p.get("n"));
        assertEquals(3L, p.get("c"));
    }

    @Test
    void repeatUntilEmitTimes() {
        // josh's created edges iterate ripple (e10) before lop (e11), like TinkerGraph
        assertEquals(List.of("ripple", "lop"), run("g.V(1).repeat(out()).times(2).values('name')"));
        assertEquals(List.of("lop", "ripple", "lop"), run("g.V(1).repeat(out()).until(has('lang')).values('name')"));
        assertEquals(5L, one("g.V(1).repeat(out()).emit().count()"));
    }

    @Test
    void mutationsAndCardinality() {
        run("g.V(1).property('nick','m').property(list,'nick','mk')");
        assertEquals(List.of("m", "mk"), run("g.V(1).values('nick')"));
        run("g.V(1).property('nick','only')");
        assertEquals(List.of("only"), run("g.V(1).values('nick')"));
        run("g.V(3).drop()");
        assertEquals(5L, one("g.V().count()"));
        assertEquals(3L, one("g.E().count()"));
    }

    @Test
    void scriptLanguage() {
        assertEquals(2, one("1+1"));
        assertEquals(3.5d, ((Number) one("7/2")).doubleValue());
        assertEquals(List.of(2, 4, 6), run("[1,2,3].collect{it*2}"));
        assertEquals(6, one("def f(a,b){a*b}; f(2,3)"));
        assertEquals("x2y", one("\"x${1+1}y\""));
        assertEquals(10, one("x=0;for(i in 1..4){x=x+i};x"));
        assertEquals(29, one("g.V(1).next().value('age')"));
        assertEquals(List.of(28), run("g.V(1).values('age').map{it.get()-1}"));
    }

    @Test
    void errorsHaveGremlinServerStatusCodes() {
        assertEquals(597, assertThrows(G.GremlinError.class, () -> run("g.V().foo()")).code);
        assertEquals(597, assertThrows(G.GremlinError.class, () -> run("g.V().hasLabel()")).code);
        assertEquals(597, assertThrows(G.GremlinError.class, () -> run("1/0")).code);
        assertEquals(597, assertThrows(G.GremlinError.class, () -> run("nosuchvariable")).code);
        G.GremlinError e = assertThrows(G.GremlinError.class, () -> run("g.V().hasLabel('none').next()"));
        assertTrue(e.getMessage().contains("NoSuchElement"));
    }

    @Test
    void readOnlyRefusesWrites() {
        Script.Session ss = new Script.Session(store, true, 0);
        Script.Env env = Script.newRoot(ss, null);
        G.GremlinError e = assertThrows(G.GremlinError.class,
                () -> Processor.resultItems(Script.eval("g.addV('x')", env), ss, env).next());
        assertTrue(e.getMessage().contains("Read-only"));
        assertEquals(6L, one("g.V().count()"));
    }

    @Test
    void edgeIdsAreStrictLongsLikeTinkerGraph() {
        assertEquals(0, run("g.E(7)").size());
        assertEquals(1, run("g.E(7L)").size());
        assertEquals(1, run("g.V(1)").size());
    }

    @Test
    void connectivesAndSideEffects() {
        assertEquals(List.of("marko"), run("g.V().hasLabel('person').where(out('created').and().out('knows')).values('name')"));
        Object cap = one("g.V().hasLabel('person').aggregate('p').cap('p')");
        assertTrue(cap instanceof G.BulkSet b && b.size() == 4);
    }
}
