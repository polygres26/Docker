package com.sayonora.warp.boltwire;

import com.sayonora.warp.boltwire.Cy.*;
import com.sayonora.warp.boltwire.Values.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Expression evaluation over one row (a variable-name to value map). */
final class Eval {

    final Exec x;
    final Executor executor;
    /** While a group is being finalised: aggregate call node to its result. */
    java.util.HashMap<String, Object> aggResults;

    Eval(Exec x, Executor executor) {
        this.x = x;
        this.executor = executor;
    }

    Object eval(Expr e, Map<String, Object> row) {
        if (aggResults != null && (e instanceof CountStar || (e instanceof Call ac && Funcs.isAggregate(ac.name())))) {
            String key = Analyzer.norm(e);
            if (aggResults.containsKey(key)) {
                return aggResults.get(key);
            }
        }
        return switch (e) {
            case Lit l -> l.value();
            case Param p -> param(p);
            case Var v -> {
                if (!row.containsKey(v.name())) {
                    throw CypherException.syntax("Variable `" + v.name() + "` not defined");
                }
                yield row.get(v.name());
            }
            case Prop p -> property(eval(p.target(), row), p.key());
            case Index i -> index(eval(i.target(), row), eval(i.index(), row));
            case Slice s -> slice(eval(s.target(), row), s.from() == null ? NONE : eval(s.from(), row),
                    s.to() == null ? NONE : eval(s.to(), row));
            case ListLit l -> {
                List<Object> out = new ArrayList<>(l.items().size());
                for (Expr it : l.items()) {
                    out.add(eval(it, row));
                }
                yield out;
            }
            case MapLit m -> {
                Map<String, Object> out = new LinkedHashMap<>();
                for (int i = 0; i < m.keys().size(); i++) {
                    out.put(m.keys().get(i), eval(m.values().get(i), row));
                }
                yield out;
            }
            case Unary u -> unary(u, eval(u.operand(), row));
            case Binary b -> binary(b.op(), eval(b.left(), row), eval(b.right(), row));
            case Not n -> {
                Object v = eval(n.operand(), row);
                if (v == null) {
                    yield null;
                }
                if (!(v instanceof Boolean b)) {
                    throw typeMismatch("Boolean", v);
                }
                yield !b;
            }
            case And a -> and(a, row);
            case Or o -> or(o, row);
            case Xor xo -> {
                Object l = bool(eval(xo.left(), row)), r = bool(eval(xo.right(), row));
                yield l == null || r == null ? null : (Boolean) l ^ (Boolean) r;
            }
            case Cmp c -> cmp(c.op(), eval(c.left(), row), eval(c.right(), row));
            case IsNull n -> {
                Object v = eval(n.operand(), row);
                yield n.negated() ? v != null : v == null;
            }
            case HasLabels h -> hasLabels(eval(h.target(), row), h.labels());
            case InList i -> inList(eval(i.element(), row), eval(i.list(), row));
            case StrOp s -> strOp(s.op(), eval(s.left(), row), eval(s.right(), row));
            case RegexMatch r -> regex(eval(r.left(), row), eval(r.right(), row));
            case Call c -> {
                if (Funcs.isAggregate(c.name())) {
                    throw CypherException.syntax("Invalid use of aggregating function " + c.name() + "(...)");
                }
                List<Object> args = new ArrayList<>(c.args().size());
                for (Expr a : c.args()) {
                    args.add(eval(a, row));
                }
                yield Funcs.call(this, c.name(), args, row);
            }
            case CountStar cs -> throw CypherException.syntax("Invalid use of aggregating function count(...)");
            case CaseExpr c -> caseExpr(c, row);
            case ListComp lc -> listComp(lc, row);
            case PatternComp pc -> executor.patternComprehension(this, pc, row);
            case Quantifier q -> quantifier(q, row);
            case Reduce r -> {
                Object l = eval(r.list(), row);
                if (l == null) {
                    yield null;
                }
                Object acc = eval(r.init(), row);
                Map<String, Object> child = new HashMap<>(row);
                for (Object it : asList(l, "reduce")) {
                    child.put(r.acc(), acc);
                    child.put(r.var(), it);
                    acc = eval(r.body(), child);
                }
                yield acc;
            }
            case MapProj mp -> mapProj(mp, row);
            case PatternPred pp -> executor.patternExists(this, pp.pattern(), row);
            case ExistsSub es -> executor.existsSub(this, es.patterns(), es.where(), es.query(), row);
            case CountSub cs -> executor.countSub(this, cs.patterns(), cs.where(), cs.query(), row);
            case CollectSub cs -> executor.collectSub(this, cs.query(), row);
            case PathFn pf -> executor.shortestPathExpr(this, pf.pattern(), row);
        };
    }

    static final Object NONE = new Object();

    Object param(Param p) {
        if (!x.params.containsKey(p.name())) {
            throw new CypherException(CypherException.PARAM_MISSING, "Expected parameter(s): " + p.name());
        }
        return x.params.get(p.name());
    }

    // ---------------------------------------------------------------------------------------- properties

    Object property(Object t, String key) {
        if (t == null) {
            return null;
        }
        if (t instanceof NodeV n) {
            Exec.checkLive(n);
            return n.props.get(key);
        }
        if (t instanceof RelV r) {
            Exec.checkLive(r);
            return r.props.get(key);
        }
        if (t instanceof Map<?, ?> m) {
            return m.get(key);
        }
        Object tv = Temporal.component(t, key);
        if (tv != Temporal.NOT_TEMPORAL) {
            return tv;
        }
        throw CypherException.type("Type mismatch: expected a map but was " + Values.typeName(t));
    }

    Object index(Object t, Object i) {
        if (t == null) {
            return null;
        }
        if (t instanceof List<?> l) {
            if (i == null) {
                return null;
            }
            if (!(i instanceof Long idx)) {
                throw CypherException.type("Type mismatch: expected Integer but was " + Values.typeName(i));
            }
            long k = idx < 0 ? l.size() + idx : idx;
            return k < 0 || k >= l.size() ? null : l.get((int) k);
        }
        if (t instanceof Map<?, ?> || t instanceof NodeV || t instanceof RelV) {
            if (i == null) {
                return null;
            }
            if (!(i instanceof String k)) {
                throw CypherException.type("Type mismatch: expected String but was " + Values.typeName(i));
            }
            return property(t, k);
        }
        throw CypherException.type("Type mismatch: expected Map, Node, Relationship or List but was " + Values.typeName(t));
    }

    Object slice(Object t, Object from, Object to) {
        if (t == null) {
            return null;
        }
        if (!(t instanceof List<?> l)) {
            throw CypherException.type("Type mismatch: expected List but was " + Values.typeName(t));
        }
        if ((from != NONE && from == null) || (to != NONE && to == null)) {
            return null;
        }
        long n = l.size();
        long f = from == NONE ? 0 : toLong(from);
        long e = to == NONE ? n : toLong(to);
        if (f < 0) {
            f = Math.max(0, n + f);
        }
        if (e < 0) {
            e = Math.max(0, n + e);
        }
        f = Math.min(f, n);
        e = Math.min(e, n);
        if (f >= e) {
            return new ArrayList<>();
        }
        return new ArrayList<>(l.subList((int) f, (int) e));
    }

    static long toLong(Object o) {
        if (o instanceof Long l) {
            return l;
        }
        throw CypherException.type("Type mismatch: expected Integer but was " + Values.typeName(o));
    }

    static List<?> asList(Object o, String what) {
        if (o instanceof List<?> l) {
            return l;
        }
        throw CypherException.type("Type mismatch: expected List but was " + Values.typeName(o));
    }

    // ---------------------------------------------------------------------------------------- operators

    static CypherException typeMismatch(String expected, Object was) {
        return CypherException.type("Type mismatch: expected " + expected + " but was " + Values.typeName(was));
    }

    private static Object bool(Object v) {
        if (v == null || v instanceof Boolean) {
            return v;
        }
        throw typeMismatch("Boolean", v);
    }

    private Object and(And a, Map<String, Object> row) {
        Object l = bool(eval(a.left(), row));
        if (Boolean.FALSE.equals(l)) {
            return false;
        }
        Object r = bool(eval(a.right(), row));
        if (Boolean.FALSE.equals(r)) {
            return false;
        }
        return l == null || r == null ? null : Boolean.TRUE;
    }

    private Object or(Or o, Map<String, Object> row) {
        Object l = bool(eval(o.left(), row));
        if (Boolean.TRUE.equals(l)) {
            return true;
        }
        Object r = bool(eval(o.right(), row));
        if (Boolean.TRUE.equals(r)) {
            return true;
        }
        return l == null || r == null ? null : Boolean.FALSE;
    }

    private Object cmp(String op, Object a, Object b) {
        switch (op) {
            case "=" -> {
                return Values.equal(a, b);
            }
            case "<>" -> {
                Boolean e = Values.equal(a, b);
                return e == null ? null : !e;
            }
            default -> {
                if (Values.isNumber(a) && Values.isNumber(b)
                        && ((a instanceof Double da && da.isNaN()) || (b instanceof Double db && db.isNaN()))) {
                    return false;
                }
                Integer c = Values.compare(a, b);
                if (c == null) {
                    return null;
                }
                return switch (op) {
                    case "<" -> c < 0;
                    case ">" -> c > 0;
                    case "<=" -> c <= 0;
                    default -> c >= 0;
                };
            }
        }
    }

    private Object unary(Unary u, Object v) {
        if (v == null) {
            return null;
        }
        if (u.op() == '+') {
            if (!Values.isNumber(v)) {
                throw typeMismatch("Integer or Float", v);
            }
            return v;
        }
        if (v instanceof Long l) {
            if (l == Long.MIN_VALUE) {
                throw CypherException.arithmetic("long overflow");
            }
            return -l;
        }
        if (v instanceof Double d) {
            return -d;
        }
        if (v instanceof DurationV d) {
            return Temporal.negate(d);
        }
        throw typeMismatch("Integer, Float or Duration", v);
    }

    Object binary(char op, Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        switch (op) {
            case '+' -> {
                if (a instanceof Long x && b instanceof Long y) {
                    try {
                        return Math.addExact(x, y);
                    } catch (ArithmeticException ex) {
                        throw CypherException.arithmetic("long overflow");
                    }
                }
                if (Values.isNumber(a) && Values.isNumber(b)) {
                    return ((Number) a).doubleValue() + ((Number) b).doubleValue();
                }
                if (a instanceof String s && (b instanceof String || Values.isNumber(b))) {
                    return s + Funcs.str(b);
                }
                if (b instanceof String s && Values.isNumber(a)) {
                    return Funcs.str(a) + s;
                }
                if (a instanceof List<?> l1 && b instanceof List<?> l2) {
                    List<Object> out = new ArrayList<>(l1);
                    out.addAll(l2);
                    return out;
                }
                if (a instanceof List<?> l1) {
                    List<Object> out = new ArrayList<>(l1);
                    out.add(b);
                    return out;
                }
                if (b instanceof List<?> l2) {
                    List<Object> out = new ArrayList<>();
                    out.add(a);
                    out.addAll(l2);
                    return out;
                }
                Object t = Temporal.add(a, b, false);
                if (t != Temporal.NOT_TEMPORAL) {
                    return t;
                }
                throw CypherException.type("Type mismatch: expected Float, Integer, String, Duration, Date, Time, LocalTime, "
                        + "LocalDateTime, DateTime or List<T> but was " + Values.typeName(a) + " and " + Values.typeName(b));
            }
            case '-' -> {
                if (a instanceof Long x && b instanceof Long y) {
                    try {
                        return Math.subtractExact(x, y);
                    } catch (ArithmeticException ex) {
                        throw CypherException.arithmetic("long overflow");
                    }
                }
                if (Values.isNumber(a) && Values.isNumber(b)) {
                    return ((Number) a).doubleValue() - ((Number) b).doubleValue();
                }
                Object t = Temporal.add(a, b, true);
                if (t != Temporal.NOT_TEMPORAL) {
                    return t;
                }
                throw typeMismatch("Integer, Float, Duration, Date, Time, LocalTime, LocalDateTime or DateTime", a instanceof Long || a instanceof Double ? b : a);
            }
            case '*' -> {
                if (a instanceof Long x && b instanceof Long y) {
                    try {
                        return Math.multiplyExact(x, y);
                    } catch (ArithmeticException ex) {
                        throw CypherException.arithmetic("long overflow");
                    }
                }
                if (Values.isNumber(a) && Values.isNumber(b)) {
                    return ((Number) a).doubleValue() * ((Number) b).doubleValue();
                }
                Object t = Temporal.multiply(a, b, false);
                if (t != Temporal.NOT_TEMPORAL) {
                    return t;
                }
                throw typeMismatch("Integer, Float or Duration", Values.isNumber(a) ? b : a);
            }
            case '/' -> {
                if (a instanceof Long x && b instanceof Long y) {
                    if (y == 0) {
                        throw CypherException.arithmetic("/ by zero");
                    }
                    if (x == Long.MIN_VALUE && y == -1) {
                        throw CypherException.arithmetic("long overflow");
                    }
                    return x / y;
                }
                if (Values.isNumber(a) && Values.isNumber(b)) {
                    return ((Number) a).doubleValue() / ((Number) b).doubleValue();
                }
                Object t = Temporal.multiply(a, b, true);
                if (t != Temporal.NOT_TEMPORAL) {
                    return t;
                }
                throw typeMismatch("Integer, Float or Duration", Values.isNumber(a) ? b : a);
            }
            case '%' -> {
                if (a instanceof Long x && b instanceof Long y) {
                    if (y == 0) {
                        throw CypherException.arithmetic("/ by zero");
                    }
                    return x % y;
                }
                if (Values.isNumber(a) && Values.isNumber(b)) {
                    return ((Number) a).doubleValue() % ((Number) b).doubleValue();
                }
                throw typeMismatch("Integer or Float", Values.isNumber(a) ? b : a);
            }
            case '^' -> {
                if (Values.isNumber(a) && Values.isNumber(b)) {
                    return Math.pow(((Number) a).doubleValue(), ((Number) b).doubleValue());
                }
                throw typeMismatch("Integer or Float", Values.isNumber(a) ? b : a);
            }
            default -> throw new IllegalStateException("operator " + op);
        }
    }

    private Object hasLabels(Object t, List<String> labels) {
        if (t == null) {
            return null;
        }
        if (t instanceof NodeV n) {
            Exec.checkLive(n);
            return n.labels.containsAll(labels);
        }
        if (t instanceof RelV r) {
            Exec.checkLive(r);
            return labels.size() == 1 && r.type.equals(labels.get(0));
        }
        throw typeMismatch("Node or Relationship", t);
    }

    private Object inList(Object el, Object list) {
        if (list == null) {
            return null;
        }
        if (!(list instanceof List<?> l)) {
            throw typeMismatch("List", list);
        }
        boolean unknown = false;
        for (Object o : l) {
            Boolean eq = Values.equal(el, o);
            if (eq == null) {
                unknown = true;
            } else if (eq) {
                return true;
            }
        }
        if (l.isEmpty()) {
            return false;
        }
        return unknown ? null : Boolean.FALSE;
    }

    private Object strOp(String op, Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        if (!(a instanceof String s) || !(b instanceof String t)) {
            return null;
        }
        return switch (op) {
            case "STARTS" -> s.startsWith(t);
            case "ENDS" -> s.endsWith(t);
            default -> s.contains(t);
        };
    }

    private Object regex(Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        if (!(a instanceof String s) || !(b instanceof String p)) {
            return null;
        }
        try {
            return Pattern.compile(p, Pattern.DOTALL).matcher(s).matches();
        } catch (PatternSyntaxException e) {
            throw new CypherException(CypherException.SEMANTIC, "Invalid regular expression: " + e.getDescription());
        }
    }

    private Object caseExpr(CaseExpr c, Map<String, Object> row) {
        if (c.subject() != null) {
            Object subj = eval(c.subject(), row);
            for (int i = 0; i < c.whens().size(); i++) {
                Object w = eval(c.whens().get(i), row);
                if (Boolean.TRUE.equals(Values.equal(subj, w))) {
                    return eval(c.thens().get(i), row);
                }
            }
        } else {
            for (int i = 0; i < c.whens().size(); i++) {
                Object w = eval(c.whens().get(i), row);
                if (w != null && !(w instanceof Boolean)) {
                    throw typeMismatch("Boolean", w);
                }
                if (Boolean.TRUE.equals(w)) {
                    return eval(c.thens().get(i), row);
                }
            }
        }
        return c.otherwise() == null ? null : eval(c.otherwise(), row);
    }

    private Object listComp(ListComp lc, Map<String, Object> row) {
        Object l = eval(lc.list(), row);
        if (l == null) {
            return null;
        }
        List<Object> out = new ArrayList<>();
        Map<String, Object> child = new HashMap<>(row);
        for (Object it : asList(l, "list comprehension")) {
            child.put(lc.var(), it);
            if (lc.where() != null) {
                Object w = eval(lc.where(), child);
                if (w != null && !(w instanceof Boolean)) {
                    throw typeMismatch("Boolean", w);
                }
                if (!Boolean.TRUE.equals(w)) {
                    continue;
                }
            }
            out.add(lc.map() == null ? it : eval(lc.map(), child));
        }
        return out;
    }

    private Object quantifier(Quantifier q, Map<String, Object> row) {
        Object l = eval(q.list(), row);
        if (l == null) {
            return null;
        }
        long trues = 0, falses = 0, nulls = 0;
        Map<String, Object> child = new HashMap<>(row);
        for (Object it : asList(l, q.kind())) {
            child.put(q.var(), it);
            Object w = q.where() == null ? Boolean.TRUE : eval(q.where(), child);
            if (w == null) {
                nulls++;
            } else if (!(w instanceof Boolean b)) {
                throw typeMismatch("Boolean", w);
            } else if (b) {
                trues++;
            } else {
                falses++;
            }
        }
        return switch (q.kind()) {
            case "ALL" -> falses > 0 ? Boolean.FALSE : nulls > 0 ? null : Boolean.TRUE;
            case "ANY" -> trues > 0 ? Boolean.TRUE : nulls > 0 ? null : Boolean.FALSE;
            case "NONE" -> trues > 0 ? Boolean.FALSE : nulls > 0 ? null : Boolean.TRUE;
            default -> trues > 1 ? Boolean.FALSE : nulls > 0 ? null : (trues == 1 ? Boolean.TRUE : Boolean.FALSE);
        };
    }

    private Object mapProj(MapProj mp, Map<String, Object> row) {
        Object t = eval(mp.target(), row);
        if (t == null) {
            return null;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (int i = 0; i < mp.kinds().size(); i++) {
            switch (mp.kinds().get(i)) {
                case "PROP" -> out.put(mp.keys().get(i), property(t, mp.keys().get(i)));
                case "ALL" -> {
                    if (t instanceof NodeV n) {
                        out.putAll(n.props);
                    } else if (t instanceof RelV r) {
                        out.putAll(r.props);
                    } else if (t instanceof Map<?, ?> m) {
                        for (Map.Entry<?, ?> e : m.entrySet()) {
                            out.put((String) e.getKey(), e.getValue());
                        }
                    } else {
                        throw typeMismatch("Map, Node or Relationship", t);
                    }
                }
                default -> out.put(mp.keys().get(i), eval(mp.values().get(i), row));
            }
        }
        return out;
    }

    // ---------------------------------------------------------------------------------------- helpers for callers

    boolean isTrue(Expr e, Map<String, Object> row) {
        Object v = eval(e, row);
        if (v == null) {
            return false;
        }
        if (!(v instanceof Boolean b)) {
            throw typeMismatch("Boolean", v);
        }
        return b;
    }

    static boolean isTemporal(Object v) {
        return v instanceof LocalDate || v instanceof LocalTime || v instanceof OffsetTime || v instanceof LocalDateTime
                || v instanceof ZonedDateTime;
    }
}
