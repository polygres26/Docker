package com.sayonora.wire.gremlinwire;

import com.sayonora.wire.gremlinwire.Engine.Ctx;
import com.sayonora.wire.gremlinwire.Engine.Step;
import com.sayonora.wire.gremlinwire.Engine.Tr;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/** The math() step: an infix expression over the current value ({@code _}) and labelled path values, evaluated as doubles. */
final class MathStep {

    private MathStep() {
    }

    static Iterator<Tr> apply(Step s, Iterator<Tr> in, Ctx ctx) {
        String expr = (String) s.args[0];
        return Engine.map(in, t -> {
            Map<String, Double> vars = new HashMap<>();
            Parser p = new Parser(expr, name -> {
                if (vars.containsKey(name)) {
                    return vars.get(name);
                }
                Object v;
                if (name.equals("_")) {
                    v = t.v;
                } else {
                    v = null;
                    for (Engine.Node n = t.path; n != null; n = n.parent) {
                        if (n.labels.contains(name)) {
                            v = n.obj;
                            break;
                        }
                    }
                    if (v == null && ctx.side.containsKey(name)) {
                        v = ctx.side.get(name);
                    }
                    if (v == null) {
                        throw G.GremlinError.script("The math() step could not find a value for the variable '" + name + "'");
                    }
                }
                if (!s.by.isEmpty()) {
                    Object[] by = s.by.get(vars.size() % s.by.size());
                    v = Engine.byStrict(by, t.child(v), ctx);
                }
                if (!(v instanceof Number n)) {
                    throw G.GremlinError.script("The variable '" + name + "' of math() is not a number: " + v);
                }
                vars.put(name, n.doubleValue());
                return n.doubleValue();
            });
            return t.child(p.parse());
        });
    }

    private static final class Parser {
        final String s;
        final Function<String, Double> var;
        int i;

        Parser(String s, Function<String, Double> var) {
            this.s = s;
            this.var = var;
        }

        double parse() {
            double v = expr();
            ws();
            if (i < s.length()) {
                throw G.GremlinError.script("Unexpected character in math expression: " + s.charAt(i));
            }
            return v;
        }

        void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }

        boolean eat(char c) {
            ws();
            if (i < s.length() && s.charAt(i) == c) {
                i++;
                return true;
            }
            return false;
        }

        double expr() {
            double v = term();
            while (true) {
                if (eat('+')) {
                    v += term();
                } else if (eat('-')) {
                    v -= term();
                } else {
                    return v;
                }
            }
        }

        double term() {
            double v = power();
            while (true) {
                if (eat('*')) {
                    v *= power();
                } else if (eat('/')) {
                    v /= power();
                } else if (eat('%')) {
                    v %= power();
                } else {
                    return v;
                }
            }
        }

        double power() {
            double b = unary();
            if (eat('^')) {
                return Math.pow(b, power());
            }
            return b;
        }

        double unary() {
            if (eat('-')) {
                return -unary();
            }
            if (eat('+')) {
                return unary();
            }
            return atom();
        }

        double atom() {
            ws();
            if (eat('(')) {
                double v = expr();
                if (!eat(')')) {
                    throw G.GremlinError.script("Missing ) in math expression");
                }
                return v;
            }
            int st = i;
            if (i < s.length() && (Character.isDigit(s.charAt(i)) || s.charAt(i) == '.')) {
                while (i < s.length() && (Character.isDigit(s.charAt(i)) || s.charAt(i) == '.' || s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
                    i++;
                }
                return Double.parseDouble(s.substring(st, i));
            }
            while (i < s.length() && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_')) {
                i++;
            }
            if (st == i) {
                throw G.GremlinError.script("Unexpected token in math expression at " + i);
            }
            String name = s.substring(st, i);
            if (eat('(')) {
                double a = expr();
                if (!eat(')')) {
                    throw G.GremlinError.script("Missing ) in math expression");
                }
                return switch (name) {
                    case "abs" -> Math.abs(a);
                    case "ceil" -> Math.ceil(a);
                    case "floor" -> Math.floor(a);
                    case "sqrt" -> Math.sqrt(a);
                    case "cbrt" -> Math.cbrt(a);
                    case "sin" -> Math.sin(a);
                    case "cos" -> Math.cos(a);
                    case "tan" -> Math.tan(a);
                    case "asin" -> Math.asin(a);
                    case "acos" -> Math.acos(a);
                    case "atan" -> Math.atan(a);
                    case "sinh" -> Math.sinh(a);
                    case "cosh" -> Math.cosh(a);
                    case "tanh" -> Math.tanh(a);
                    case "log" -> Math.log(a);
                    case "log10" -> Math.log10(a);
                    case "log2" -> Math.log(a) / Math.log(2);
                    case "exp" -> Math.exp(a);
                    case "signum" -> Math.signum(a);
                    default -> throw G.GremlinError.script("Unknown math function " + name);
                };
            }
            return var.apply(name);
        }
    }
}
