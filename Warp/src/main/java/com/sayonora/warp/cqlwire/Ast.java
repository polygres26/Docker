package com.sayonora.warp.cqlwire;

import java.util.List;
import java.util.Map;

/** Parsed CQL: statements and terms. Records are immutable so parsed statements can be cached and shared by sessions. */
final class Ast {

    private Ast() {
    }

    // ------------------------------------------------------------------ terms

    sealed interface Term permits Lit, Bind, Func, ListLit, BraceLit, TupleLit, Arith, Cast, ColRef, TypeHint {
    }

    /** A literal: Long/BigInteger (integer), BigDecimal (float), Double (NaN/Infinity), String, Boolean, byte[] (blob), UUID, null (NULL), Duration. */
    record Lit(Object value, boolean isString) implements Term {
    }

    /** {@code ?} (index assigned in order) or {@code :name}. */
    record Bind(int index, String name) implements Term {
    }

    record Func(String ks, String name, List<Term> args, boolean star) implements Term {
    }

    record ListLit(List<Term> items) implements Term {
    }

    /** {@code { ... }}: a set (items), a map (pairs) or a UDT literal (fields), told apart by the expected type. */
    record BraceLit(List<Term> items, List<Term[]> pairs, Map<String, Term> fields) implements Term {
    }

    record TupleLit(List<Term> items) implements Term {
    }

    record Arith(char op, Term l, Term r) implements Term {
    }

    record Cast(Term t, String type) implements Term {
    }

    record TypeHint(String type, Term t) implements Term {
    }

    /** A column (or column[index], column.field) in a selector, term or condition. */
    record ColRef(String name, Term index, String field) implements Term {
    }

    // ------------------------------------------------------------------ relations

    /** lhs: columns (one, or several for a tuple relation) or a token(...) call. */
    record Relation(List<String> cols, boolean token, Term index, String op, Term rhs, List<Term> inList, boolean multi) {
        Relation(List<String> cols, boolean token, Term index, String op, Term rhs, List<Term> inList) {
            this(cols, token, index, op, rhs, inList, false);
        }
    }

    record Assign(String col, Term index, String field, Term value, char arith, boolean prepend) {
    }

    record Cond(String col, Term index, String field, String op, Term rhs, List<Term> inList) {
    }

    // ------------------------------------------------------------------ statements

    sealed interface Stmt permits Use, CreateKeyspace, AlterKeyspace, DropKeyspace, CreateTable, AlterTable, DropTable, Truncate,
            CreateIndex, DropIndex, CreateType, AlterType, DropType, Insert, Update, Delete, Batch, Select, Nop, Other, Describe {
    }

    record Nop() implements Stmt {
    }

    /** A statement Warp recognises but does not implement (roles, permissions, functions, materialized views, ...). */
    record Other(String kind) implements Stmt {
    }

    /** DESCRIBE what [ks.]name: what = cluster, schema, keyspaces, keyspace, tables, table, types, type, index, functions, aggregates, view, element. */
    record Describe(String what, String ks, String name, boolean full, boolean only) implements Stmt {
    }

    record Use(String ks) implements Stmt {
    }

    record CreateKeyspace(String name, boolean ine, Map<String, Object> options) implements Stmt {
    }

    record AlterKeyspace(String name, Map<String, Object> options) implements Stmt {
    }

    record DropKeyspace(String name, boolean ifExists) implements Stmt {
    }

    record ColDef(String name, String type, boolean isStatic, boolean pk) {
    }

    record CreateTable(String ks, String name, boolean ine, List<ColDef> cols, List<String> partition, List<String> clustering,
            List<Boolean> clusteringDesc, Map<String, Object> options) implements Stmt {
    }

    record AlterTable(String ks, String name, List<ColDef> add, List<String> drop, Map<String, String> rename, Map<String, Object> options,
            Map<String, String> alterType, boolean ifExists, boolean addIfNotExists, boolean dropIfExists) implements Stmt {
    }

    record DropTable(String ks, String name, boolean ifExists) implements Stmt {
    }

    record Truncate(String ks, String name) implements Stmt {
    }

    record CreateIndex(String name, boolean ine, String ks, String table, String target, String kind, String custom,
            Map<String, Object> options) implements Stmt {
    }

    record DropIndex(String ks, String name, boolean ifExists) implements Stmt {
    }

    record CreateType(String ks, String name, boolean ine, List<String> fields, List<String> types) implements Stmt {
    }

    record AlterType(String ks, String name, String addField, String addType, Map<String, String> rename) implements Stmt {
    }

    record DropType(String ks, String name, boolean ifExists) implements Stmt {
    }

    record Insert(String ks, String table, List<String> cols, List<Term> values, String json, boolean ine, Term ttl, Term ts,
            boolean defaultUnset) implements Stmt {
    }

    record Update(String ks, String table, Term ttl, Term ts, List<Assign> assigns, List<Relation> where, boolean ifExists,
            List<Cond> conds) implements Stmt {
    }

    /** A DELETE target: col, col[key] (index != null) or col.field. */
    record DelTarget(String col, Term index, String field) {
    }

    record Delete(String ks, String table, List<DelTarget> targets, Term ts, List<Relation> where, boolean ifExists,
            List<Cond> conds) implements Stmt {
    }

    record Batch(String kind, Term ts, List<Stmt> stmts) implements Stmt {
    }

    record Selector(Term expr, String alias, boolean star) {
    }

    record Order(String col, boolean desc) {
    }

    record Select(String ks, String table, boolean distinct, boolean json, List<Selector> selectors, List<Relation> where,
            List<String> groupBy, List<Order> order, Term limit, Term perPartitionLimit, boolean allowFiltering) implements Stmt {
    }
}
