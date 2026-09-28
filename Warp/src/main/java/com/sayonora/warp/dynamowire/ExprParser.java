package com.sayonora.warp.dynamowire;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Tokenizer + recursive-descent parser for DynamoDB's condition/filter, update, projection and
 * key-condition expression languages. Error messages follow DynamoDB's own
 * ("Invalid ConditionExpression: Syntax error; token: ..., near: ..." and friends).
 */
public final class ExprParser {

    private enum Tk { IDENT, NAMEREF, VALREF, NUM, LPAREN, RPAREN, COMMA, DOT, LBRACK, RBRACK, EQ, NE, LT, LE, GT, GE, PLUS, MINUS, BAD, EOF }

    private record Token(Tk kind, String text, int start, int end) {}

    private static final int MAX_EXPRESSION_BYTES = 4096;
    private static final Set<String> BOOLEAN_FUNCTIONS =
            Set.of("attribute_exists", "attribute_not_exists", "attribute_type", "begins_with", "contains");
    private static final Set<String> ATTRIBUTE_TYPES =
            Set.of("S", "SS", "N", "NS", "B", "BS", "BOOL", "NULL", "L", "M");

    private final String src;
    private final String exprName;
    private final ExpressionContext ctx;
    private final List<Token> tokens = new ArrayList<>();
    private int pos;

    private ExprParser(String src, String exprName, ExpressionContext ctx) {
        this.src = src;
        this.exprName = exprName;
        this.ctx = ctx;
        ctx.currentExpression = exprName;
        ctx.markExpressionUsed();
        if (src.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_EXPRESSION_BYTES) {
            throw DynamoException.validation("Invalid " + exprName
                    + ": Expression size has exceeded the maximum allowed size; expression size: "
                    + src.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        }
        if (src.isBlank()) {
            throw DynamoException.validation("Invalid " + exprName + ": The expression can not be empty;");
        }
        lex();
    }

    // ------------------------------------------------------------------------------- entry points

    public static Expr.Cond parseCondition(String expr, String exprName, ExpressionContext ctx) {
        ExprParser p = new ExprParser(expr, exprName, ctx);
        Expr.Cond c = p.or();
        p.expectEof();
        return c;
    }

    public static Expr.UpdatePlan parseUpdate(String expr, ExpressionContext ctx) {
        ExprParser p = new ExprParser(expr, "UpdateExpression", ctx);
        return p.update();
    }

    public static List<Expr.Path> parseProjection(String expr, ExpressionContext ctx) {
        ExprParser p = new ExprParser(expr, "ProjectionExpression", ctx);
        List<Expr.Path> paths = new ArrayList<>();
        do {
            paths.add(p.path());
        } while (p.accept(Tk.COMMA));
        p.expectEof();
        for (int i = 0; i < paths.size(); i++) {
            for (int j = i + 1; j < paths.size(); j++) {
                Expr.Path a = paths.get(i), b = paths.get(j);
                if (a.overlaps(b)) {
                    throw DynamoException.validation("Invalid ProjectionExpression: Two document paths overlap with each other; "
                            + "must remove or rewrite one of these paths; path one: " + a.display() + ", path two: " + b.display());
                }
                if (a.conflicts(b)) {
                    throw DynamoException.validation("Invalid ProjectionExpression: Two document paths conflict with each other; "
                            + "must remove or rewrite one of these paths; path one: " + a.display() + ", path two: " + b.display());
                }
            }
        }
        return paths;
    }

    // ------------------------------------------------------------------------------- lexer

    private void lex() {
        int i = 0, n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            int start = i;
            if (Character.isLetter(c) && c < 128 || c == '_') {
                while (i < n && (isIdentChar(src.charAt(i)))) i++;
                tokens.add(new Token(Tk.IDENT, src.substring(start, i), start, i));
            } else if (c == '#' || c == ':') {
                i++;
                while (i < n && isIdentChar(src.charAt(i))) i++;
                if (i == start + 1) {
                    tokens.add(new Token(Tk.BAD, src.substring(start, i), start, i));
                } else {
                    tokens.add(new Token(c == '#' ? Tk.NAMEREF : Tk.VALREF, src.substring(start, i), start, i));
                }
            } else if (Character.isDigit(c)) {
                while (i < n && Character.isDigit(src.charAt(i))) i++;
                tokens.add(new Token(Tk.NUM, src.substring(start, i), start, i));
            } else {
                i++;
                switch (c) {
                    case '(' -> tokens.add(new Token(Tk.LPAREN, "(", start, i));
                    case ')' -> tokens.add(new Token(Tk.RPAREN, ")", start, i));
                    case ',' -> tokens.add(new Token(Tk.COMMA, ",", start, i));
                    case '.' -> tokens.add(new Token(Tk.DOT, ".", start, i));
                    case '[' -> tokens.add(new Token(Tk.LBRACK, "[", start, i));
                    case ']' -> tokens.add(new Token(Tk.RBRACK, "]", start, i));
                    case '+' -> tokens.add(new Token(Tk.PLUS, "+", start, i));
                    case '-' -> tokens.add(new Token(Tk.MINUS, "-", start, i));
                    case '=' -> tokens.add(new Token(Tk.EQ, "=", start, i));
                    case '<' -> {
                        if (i < n && src.charAt(i) == '>') { i++; tokens.add(new Token(Tk.NE, "<>", start, i)); }
                        else if (i < n && src.charAt(i) == '=') { i++; tokens.add(new Token(Tk.LE, "<=", start, i)); }
                        else tokens.add(new Token(Tk.LT, "<", start, i));
                    }
                    case '>' -> {
                        if (i < n && src.charAt(i) == '=') { i++; tokens.add(new Token(Tk.GE, ">=", start, i)); }
                        else tokens.add(new Token(Tk.GT, ">", start, i));
                    }
                    default -> tokens.add(new Token(Tk.BAD, String.valueOf(c), start, i));
                }
            }
        }
        tokens.add(new Token(Tk.EOF, "<EOF>", n, n));
    }

    private static boolean isIdentChar(char c) {
        return c < 128 && (Character.isLetterOrDigit(c) || c == '_');
    }

    // ------------------------------------------------------------------------------- token helpers

    private Token peek() {
        return tokens.get(pos);
    }

    private Token peekAt(int offset) {
        return tokens.get(Math.min(pos + offset, tokens.size() - 1));
    }

    private Token next() {
        Token t = tokens.get(pos);
        if (t.kind != Tk.EOF) pos++;
        return t;
    }

    private boolean accept(Tk k) {
        if (peek().kind == k) {
            pos++;
            return true;
        }
        return false;
    }

    private boolean isKeyword(String kw) {
        Token t = peek();
        return t.kind == Tk.IDENT && t.text.equalsIgnoreCase(kw);
    }

    private boolean acceptKeyword(String kw) {
        if (isKeyword(kw)) {
            pos++;
            return true;
        }
        return false;
    }

    private void expect(Tk k) {
        if (!accept(k)) throw syntax(peek());
    }

    private void expectEof() {
        if (peek().kind != Tk.EOF) throw syntax(peek());
    }

    private DynamoException syntax(Token bad) {
        int idx = tokens.indexOf(bad);
        int nearStart = idx > 0 ? tokens.get(idx - 1).start : bad.start;
        int nearEnd = idx + 1 < tokens.size() ? tokens.get(idx + 1).end : src.length();
        if (bad.kind == Tk.EOF) {
            nearEnd = src.length();
        }
        String near = src.substring(Math.min(nearStart, src.length()), Math.min(Math.max(nearEnd, nearStart), src.length()));
        return DynamoException.validation("Invalid " + exprName + ": Syntax error; token: \"" + bad.text + "\", near: \"" + near + "\"");
    }

    private DynamoException invalid(String message) {
        return DynamoException.validation("Invalid " + exprName + ": " + message);
    }

    // ------------------------------------------------------------------------------- paths & operands

    private Expr.Path path() {
        List<Object> segs = new ArrayList<>();
        segs.add(pathName());
        while (true) {
            if (accept(Tk.DOT)) {
                segs.add(pathName());
            } else if (peek().kind == Tk.LBRACK) {
                pos++;
                Token n = peek();
                if (n.kind != Tk.NUM) throw syntax(n);
                pos++;
                expect(Tk.RBRACK);
                try {
                    segs.add(Integer.parseInt(n.text));
                } catch (NumberFormatException e) {
                    throw syntax(n);
                }
            } else {
                break;
            }
        }
        return new Expr.Path(segs);
    }

    private String pathName() {
        Token t = peek();
        if (t.kind == Tk.NAMEREF) {
            pos++;
            return ctx.resolveName(t.text);
        }
        if (t.kind == Tk.IDENT) {
            pos++;
            if (ReservedWords.isReserved(t.text)) {
                throw invalid("Attribute name is a reserved keyword; reserved keyword: " + t.text);
            }
            return t.text;
        }
        throw syntax(t);
    }

    /** operand in a condition: path | :value | size(path) */
    private Expr.Operand condOperand() {
        Token t = peek();
        if (t.kind == Tk.VALREF) {
            pos++;
            return new Expr.ValueOp(ctx.resolveValue(t.text));
        }
        if (t.kind == Tk.IDENT && peekAt(1).kind == Tk.LPAREN) {
            if (!t.text.equals("size")) {
                throw invalid("Invalid function name; function: " + t.text);
            }
            pos += 2;
            Expr.Operand inner = condOperand();
            expect(Tk.RPAREN);
            if (!(inner instanceof Expr.PathOp p)) {
                throw invalid("Operator or function requires a document path; operator or function: size");
            }
            return new Expr.SizeOp(p.path());
        }
        return new Expr.PathOp(path());
    }

    // ------------------------------------------------------------------------------- conditions

    private Expr.Cond or() {
        Expr.Cond left = and();
        if (!isKeyword("OR")) return left;
        List<Expr.Cond> parts = new ArrayList<>();
        parts.add(left);
        while (acceptKeyword("OR")) parts.add(and());
        return new Expr.Or(parts);
    }

    private Expr.Cond and() {
        Expr.Cond left = not();
        if (!isKeyword("AND")) return left;
        List<Expr.Cond> parts = new ArrayList<>();
        parts.add(left);
        while (acceptKeyword("AND")) parts.add(not());
        return new Expr.And(parts);
    }

    private Expr.Cond not() {
        if (acceptKeyword("NOT")) return new Expr.Not(not());
        return primary();
    }

    private Expr.Cond primary() {
        Token t = peek();
        if (t.kind == Tk.LPAREN) {
            pos++;
            int innerStart = pos;
            Expr.Cond inner = or();
            if (tokens.get(innerStart).kind == Tk.LPAREN && matchingClose(innerStart) == pos - 1) {
                throw invalid("The expression has redundant parentheses;");
            }
            expect(Tk.RPAREN);
            return inner;
        }
        if (t.kind == Tk.IDENT && peekAt(1).kind == Tk.LPAREN && BOOLEAN_FUNCTIONS.contains(t.text)) {
            return function();
        }
        Expr.Operand left = condOperand();
        Token op = peek();
        switch (op.kind) {
            case EQ, NE, LT, LE, GT, GE -> {
                pos++;
                Expr.Operand right = condOperand();
                checkComparable(op.text, left, right);
                return new Expr.Cmp(op.text, left, right);
            }
            default -> { }
        }
        if (acceptKeyword("BETWEEN")) {
            Expr.Operand lo = condOperand();
            if (!acceptKeyword("AND")) throw syntax(peek());
            Expr.Operand hi = condOperand();
            checkDistinct("BETWEEN", left, List.of(lo, hi));
            if (lo instanceof Expr.ValueOp l && hi instanceof Expr.ValueOp h) {
                if (l.value().type != h.value().type) {
                    throw invalid("The BETWEEN operator requires same data type for lower and upper bounds; lower bound operand: AttributeValue: "
                            + l.value().debug() + ", upper bound operand: AttributeValue: " + h.value().debug());
                }
                Integer c = l.value().compareOrNull(h.value());
                if (c == null) {
                    throw invalid("Incorrect operand type for operator or function; operator or function: BETWEEN, operand type: "
                            + l.value().type);
                }
                if (c > 0) {
                    throw invalid("The BETWEEN operator requires upper bound to be greater than or equal to lower bound; lower bound operand: AttributeValue: "
                            + l.value().debug() + ", upper bound operand: AttributeValue: " + h.value().debug());
                }
            }
            return new Expr.Between(left, lo, hi);
        }
        if (acceptKeyword("IN")) {
            expect(Tk.LPAREN);
            List<Expr.Operand> cands = new ArrayList<>();
            do {
                cands.add(condOperand());
            } while (accept(Tk.COMMA));
            expect(Tk.RPAREN);
            checkDistinct("IN", left, cands);
            if (cands.size() > 100) {
                throw invalid("The IN operator is only allowed to have 100 operands; the expression has " + cands.size());
            }
            return new Expr.In(left, cands);
        }
        throw syntax(peek());
    }

    /** Index of the ')' matching the '(' at {@code open}. */
    private int matchingClose(int open) {
        int depth = 0;
        for (int i = open; i < tokens.size(); i++) {
            if (tokens.get(i).kind == Tk.LPAREN) depth++;
            else if (tokens.get(i).kind == Tk.RPAREN && --depth == 0) return i;
        }
        return -1;
    }

    private void checkDistinct(String op, Expr.Operand first, List<Expr.Operand> rest) {
        if (!(first instanceof Expr.PathOp p)) return;
        for (Expr.Operand o : rest) {
            if (o instanceof Expr.PathOp q && q.path().equals(p.path())) {
                throw invalid("The first operand must be distinct from the remaining operands for this operator or function; operator: "
                        + op + ", first operand: " + p.path().display());
            }
        }
    }

    private void checkComparable(String op, Expr.Operand l, Expr.Operand r) {
        checkDistinct(op, l, List.of(r));
        if (op.equals("=") || op.equals("<>")) return;
        for (Expr.Operand o : List.of(l, r)) {
            if (o instanceof Expr.ValueOp v) {
                switch (v.value().type) {
                    case S, N, B -> { }
                    default -> throw invalid("Incorrect operand type for operator or function; operator or function: "
                            + op + ", operand type: " + v.value().type);
                }
            }
        }
    }

    private Expr.Cond function() {
        Token name = next();
        expect(Tk.LPAREN);
        List<Expr.Operand> args = new ArrayList<>();
        if (peek().kind != Tk.RPAREN) {
            do {
                args.add(condOperand());
            } while (accept(Tk.COMMA));
        }
        expect(Tk.RPAREN);
        int expected = switch (name.text) {
            case "attribute_exists", "attribute_not_exists" -> 1;
            default -> 2;
        };
        if (args.size() != expected) {
            throw invalid("Incorrect number of operands for operator or function; operator or function: "
                    + name.text + ", number of operands: " + args.size());
        }
        switch (name.text) {
            case "attribute_exists", "attribute_not_exists" -> {
                if (!(args.get(0) instanceof Expr.PathOp)) {
                    throw invalid("Operator or function requires a document path; operator or function: " + name.text);
                }
            }
            case "begins_with" -> {
                for (Expr.Operand a : args) {
                    if (a instanceof Expr.ValueOp v && v.value().type != AttributeValue.Type.S && v.value().type != AttributeValue.Type.B) {
                        throw invalid("Incorrect operand type for operator or function; operator or function: begins_with, operand type: "
                                + v.value().type);
                    }
                }
            }
            case "attribute_type" -> {
                if (!(args.get(0) instanceof Expr.PathOp)) {
                    throw invalid("Operator or function requires a document path; operator or function: attribute_type");
                }
                if (!(args.get(1) instanceof Expr.ValueOp v) || v.value().type != AttributeValue.Type.S) {
                    throw invalid("Incorrect operand type for operator or function; operator or function: attribute_type, operand type: "
                            + (args.get(1) instanceof Expr.ValueOp vv ? vv.value().type : "PATH"));
                }
                String t = ((Expr.ValueOp) args.get(1)).value().scalar;
                if (!ATTRIBUTE_TYPES.contains(t)) {
                    throw invalid("Invalid attribute type name found; type: " + t + ", valid types: {N,BS,L,B,NULL,M,S,SS,NS,BOOL}");
                }
            }
            case "contains" -> {
                if (!(args.get(0) instanceof Expr.PathOp)) {
                    throw invalid("Operator or function requires a document path; operator or function: contains");
                }
            }
            default -> { }
        }
        return new Expr.Fn(name.text, args);
    }

    // ------------------------------------------------------------------------------- update

    private Expr.UpdatePlan update() {
        List<Expr.UpdateAction> actions = new ArrayList<>();
        boolean set = false, remove = false, add = false, del = false;
        if (peek().kind == Tk.EOF) throw syntax(peek());
        while (peek().kind != Tk.EOF) {
            Token kw = peek();
            String k = kw.kind == Tk.IDENT ? kw.text.toUpperCase() : "";
            switch (k) {
                case "SET" -> {
                    if (set) throw invalid("The \"SET\" section can only be used once in an update expression;");
                    set = true;
                    pos++;
                    do {
                        Expr.Path p = path();
                        expect(Tk.EQ);
                        actions.add(new Expr.UpdateAction(Expr.ActionKind.SET, p, setValue()));
                    } while (accept(Tk.COMMA));
                }
                case "REMOVE" -> {
                    if (remove) throw invalid("The \"REMOVE\" section can only be used once in an update expression;");
                    remove = true;
                    pos++;
                    do {
                        actions.add(new Expr.UpdateAction(Expr.ActionKind.REMOVE, path(), null));
                    } while (accept(Tk.COMMA));
                }
                case "ADD" -> {
                    if (add) throw invalid("The \"ADD\" section can only be used once in an update expression;");
                    add = true;
                    pos++;
                    do {
                        Expr.Path p = path();
                        actions.add(new Expr.UpdateAction(Expr.ActionKind.ADD, p, valueOperandOnly()));
                    } while (accept(Tk.COMMA));
                }
                case "DELETE" -> {
                    if (del) throw invalid("The \"DELETE\" section can only be used once in an update expression;");
                    del = true;
                    pos++;
                    do {
                        Expr.Path p = path();
                        actions.add(new Expr.UpdateAction(Expr.ActionKind.DELETE, p, valueOperandOnly()));
                    } while (accept(Tk.COMMA));
                }
                default -> throw syntax(kw);
            }
        }
        // overlapping / conflicting paths
        for (int i = 0; i < actions.size(); i++) {
            for (int j = i + 1; j < actions.size(); j++) {
                Expr.Path a = actions.get(i).path(), b = actions.get(j).path();
                if (a.overlaps(b)) {
                    throw invalid("Two document paths overlap with each other; must remove or rewrite one of these paths; path one: "
                            + a.display() + ", path two: " + b.display());
                }
                if (a.conflicts(b)) {
                    throw invalid("Two document paths conflict with each other; must remove or rewrite one of these paths; path one: "
                            + a.display() + ", path two: " + b.display());
                }
            }
        }
        return new Expr.UpdatePlan(actions);
    }

    private Expr.Operand valueOperandOnly() {
        Token t = peek();
        if (t.kind != Tk.VALREF) throw syntax(t);
        pos++;
        return new Expr.ValueOp(ctx.resolveValue(t.text));
    }

    private Expr.Operand setValue() {
        Expr.Operand left = setOperand();
        Token t = peek();
        if (t.kind == Tk.PLUS || t.kind == Tk.MINUS) {
            pos++;
            Expr.Operand right = setOperand();
            return new Expr.Arith(t.kind == Tk.PLUS ? '+' : '-', left, right);
        }
        return left;
    }

    private Expr.Operand setOperand() {
        Token t = peek();
        if (t.kind == Tk.VALREF) {
            pos++;
            return new Expr.ValueOp(ctx.resolveValue(t.text));
        }
        if (t.kind == Tk.IDENT && peekAt(1).kind == Tk.LPAREN) {
            switch (t.text) {
                case "if_not_exists" -> {
                    pos += 2;
                    Expr.Path p = path();
                    expect(Tk.COMMA);
                    Expr.Operand fb = setOperand();
                    expect(Tk.RPAREN);
                    return new Expr.IfNotExists(p, fb);
                }
                case "list_append" -> {
                    pos += 2;
                    Expr.Operand a = setOperand();
                    expect(Tk.COMMA);
                    Expr.Operand b = setOperand();
                    expect(Tk.RPAREN);
                    return new Expr.ListAppend(a, b);
                }
                default -> throw invalid("Invalid function name; function: " + t.text);
            }
        }
        return new Expr.PathOp(path());
    }
}
