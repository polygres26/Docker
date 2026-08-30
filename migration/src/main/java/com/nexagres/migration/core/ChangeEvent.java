package com.nexagres.migration.core;

import java.util.List;

/**
 * One write derived from a source's bulk read or change feed -- always expressed as the exact
 * parameterized SQL to run against the target, since each connector already knows its own
 * target's physical schema (mongowire's <code>id</code>/<code>doc jsonb</code> table, a future
 * DynamoDB connector's <code>pk_value</code>/<code>sk_value</code>/<code>item</code> table, a
 * future relational connector's real translated columns) far better than any single generic
 * "document" shape could capture across all of them. A {@link Sink}'s job is only ever "execute
 * this," the same generic contract Polywire's own gRPC {@code QueryService} already exposes --
 * there is nothing to gain from inventing a fake unified data model on top of genuinely
 * heterogeneous sources this early.
 */
public record ChangeEvent(String sql, List<String> params) {
}
