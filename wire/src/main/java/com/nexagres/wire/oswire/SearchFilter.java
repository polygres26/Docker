package com.nexagres.wire.oswire;

import java.util.List;

/**
 * The filter half of the internal Search IR every search-style wire protocol (today: OpenSearch;
 * see {@link SearchRequest}'s javadoc for the Qdrant plan) compiles its query DSL into, before
 * {@code PostgresSearchPlanner} ever sees it. Deliberately small: only the clause shapes the V1
 * OpenSearch adapter actually needs (bool/term/range) -- not an attempt at a general SQL AST.
 * Adding a clause type here is the one place a new adapter or a richer V2 query shape has to
 * touch; the planner already switches exhaustively over this sealed hierarchy.
 */
public sealed interface SearchFilter {

    /** Exact-value equality on one field -- OpenSearch's {@code term} query. */
    record Term(String field, Object value) implements SearchFilter {
    }

    /**
     * A numeric or lexicographic range on one field -- OpenSearch's {@code range} query. Any
     * bound left {@code null} is simply omitted from the generated SQL (an open-ended range).
     */
    record Range(String field, Object gte, Object lte, Object gt, Object lt) implements SearchFilter {
    }

    /** Full-text match on one field via Postgres's own text search (see the planner). */
    record Match(String field, String text) implements SearchFilter {
    }

    /**
     * Boolean composition -- OpenSearch's {@code bool} query. {@code must}/{@code filter} both
     * mean AND (OpenSearch itself only distinguishes them for relevance scoring, which this V1
     * planner doesn't yet differentiate -- see {@code PostgresSearchPlanner}'s javadoc);
     * {@code should} means OR, and {@code mustNot} means AND NOT. Any empty list is simply
     * skipped when building SQL.
     */
    record Bool(List<SearchFilter> must, List<SearchFilter> filter, List<SearchFilter> should,
            List<SearchFilter> mustNot) implements SearchFilter {
    }

    /** No filtering at all -- OpenSearch's {@code match_all}, and the default when a request has
     * no {@code query} clause. */
    record MatchAll() implements SearchFilter {
    }
}
