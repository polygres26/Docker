package com.sayonora.migration.connectors.neo4j;

import com.google.gson.Gson;
import com.sayonora.migration.core.ChangeEvent;
import com.sayonora.migration.core.Partition;
import com.sayonora.migration.core.Sink;
import com.sayonora.migration.core.Source;
import com.sayonora.migration.core.StateStore;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Neo4j (Bolt protocol) connector: a real, one-time, full-graph migration -- deliberately NOT a
 * snapshot-plus-CDC design like every other connector in this project. Real Neo4j Community
 * Edition has no exposed change-tracking mechanism at all (Neo4j's own CDC feature is Enterprise-
 * only), and boltwire (this project's own Bolt emulation, used for this connector's own tests --
 * see this class's own test) doesn't implement any change-tracking procedure either. This is a
 * genuine, disclosed limitation, not glossed over: {@link #prepareChangeFeed} is a no-op and
 * {@link #streamChanges} returns immediately (logging why) rather than blocking forever the way
 * every other connector's live change feed does -- {@link com.sayonora.migration.coordinator.Coordinator#run}
 * still calls both, so this connector's {@code run()} simply finishes once the one-time graph copy
 * completes, which is the CORRECT behavior for a source with no live tail to follow.
 *
 * <p><b>Explicit schema declaration, a real design necessity, not a shortcut</b>: Neo4j has no
 * universal schema catalog the way SQL's {@code information_schema} is (labels/relationship types
 * are optional metadata, not a declared schema), and boltwire's own Cypher subset doesn't support
 * schema-discovery procedures like {@code CALL db.labels()} (confirmed by reading wire's own code
 * this session: boltwire supports RETURN/CREATE/MATCH only, no {@code CALL}). This connector
 * therefore requires the labels and relationship patterns to migrate to be declared explicitly
 * (see {@link RelationshipSpec}), rather than attempting unreliable auto-discovery.
 *
 * <p><b>Node identity across the migration, a real problem every other connector didn't have</b>:
 * every other connector's target has a real, source-provided primary key (a document {@code _id},
 * a DynamoDB key, a relational table's PK) known BEFORE the insert. A migrated node's target row
 * id is Postgres's own server-assigned {@code bigserial} (matching boltwire's own physical schema
 * exactly -- see this class's own javadoc reference to {@code PgGraphStore}), unknowable in
 * advance. Rather than extend the gRPC write path with a generated-key-feedback mechanism no other
 * connector needs, this connector maintains its OWN small correlation table ({@code
 * migration_neo4j_id_map: source_element_id text PRIMARY KEY, target_node_id bigint}), populated
 * via a single data-modifying CTE per node insert (one round trip, not two) -- and resolves each
 * relationship's endpoint ids via a SQL subquery against that map, entirely within the existing
 * fire-and-forget {@link Sink#apply} contract. This keeps the migrated nodes' own {@code
 * properties} column byte-identical to what boltwire itself would have written -- no synthetic
 * "source id" property polluting the migrated data.
 *
 * <p><b>Known, scoped limitation</b>: no parallelism -- migrates one label/relationship type at a
 * time, sequentially, and every declared node label must be FULLY migrated before any relationship
 * migration starts (a relationship's endpoint lookup in {@code migration_neo4j_id_map} would
 * otherwise race against that node not being inserted yet). Real graph partitioning for parallel
 * reads is a genuinely hard problem in general, not a quick addition; deferred.
 */
public final class Neo4jSource implements Source {

    private static final Logger log = LoggerFactory.getLogger(Neo4jSource.class);
    private static final Gson GSON = new Gson();
    private static final String PARTITION_DONE = "\"DONE\"";
    private static final int BATCH_SIZE = 500;

    /** One relationship pattern to migrate: {@code (from:FromLabel)-[:TYPE]->(to:ToLabel)}. Both
     * endpoint labels must already be included in this source's own {@code nodeLabels}. */
    public record RelationshipSpec(String fromLabel, String type, String toLabel) {
    }

    private final Driver sourceDriver;
    private final List<String> nodeLabels;
    private final List<RelationshipSpec> relationshipSpecs;
    private final String checkpointKey;

    public Neo4jSource(Driver sourceDriver, List<String> nodeLabels, List<RelationshipSpec> relationshipSpecs) {
        this.sourceDriver = sourceDriver;
        this.nodeLabels = nodeLabels;
        this.relationshipSpecs = relationshipSpecs;
        this.checkpointKey = "neo4j:" + String.join(",", nodeLabels);
    }

    @Override
    public void ensureTargetSchema(Sink sink) throws Exception {
        applyTolerantOfConcurrentCreateRace(sink, "CREATE TABLE IF NOT EXISTS warp_graph_nodes ("
                + "id bigserial PRIMARY KEY, labels text[] NOT NULL DEFAULT '{}', properties jsonb NOT NULL DEFAULT '{}')");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE INDEX IF NOT EXISTS warp_graph_nodes_labels_idx "
                + "ON warp_graph_nodes USING GIN (labels)");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE INDEX IF NOT EXISTS warp_graph_nodes_props_idx "
                + "ON warp_graph_nodes USING GIN (properties)");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE TABLE IF NOT EXISTS warp_graph_edges ("
                + "id bigserial PRIMARY KEY, type text NOT NULL, from_id bigint NOT NULL REFERENCES warp_graph_nodes(id), "
                + "to_id bigint NOT NULL REFERENCES warp_graph_nodes(id), properties jsonb NOT NULL DEFAULT '{}')");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE INDEX IF NOT EXISTS warp_graph_edges_from_idx "
                + "ON warp_graph_edges (from_id)");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE INDEX IF NOT EXISTS warp_graph_edges_to_idx "
                + "ON warp_graph_edges (to_id)");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE INDEX IF NOT EXISTS warp_graph_edges_type_idx "
                + "ON warp_graph_edges (type)");
        // This connector's own bookkeeping, not part of boltwire's own physical schema -- a
        // migrated node's real properties column stays byte-identical to what boltwire itself
        // would write; the correlation lives here instead. See this class's own javadoc.
        applyTolerantOfConcurrentCreateRace(sink, "CREATE TABLE IF NOT EXISTS migration_neo4j_id_map ("
                + "source_element_id text PRIMARY KEY, target_node_id bigint NOT NULL)");
    }

    private static void applyTolerantOfConcurrentCreateRace(Sink sink, String ddl) throws Exception {
        try {
            sink.apply(new ChangeEvent(ddl, List.of()));
        } catch (SQLException e) {
            if (!"23505".equals(e.getSQLState())) {
                throw e;
            }
            log.info("ensureTargetSchema: lost a benign concurrent CREATE race to another worker "
                    + "(23505 on the object catalog) -- the object exists either way, continuing");
        }
    }

    /** Always a single partition -- see this class's own javadoc on why real parallelism isn't
     * attempted for v1 (a relationship's endpoint lookup requires ALL node labels fully migrated
     * first, which a naive per-label parallel partition would race against). */
    @Override
    public List<Partition> listPartitions() {
        return List.of(new Partition(checkpointKey, null));
    }

    @Override
    public void readPartition(Partition partition, Sink sink, StateStore checkpoints) throws Exception {
        if (PARTITION_DONE.equals(checkpoints.load(checkpointKey))) {
            log.info("neo4j source[{}]: already fully migrated in a prior run -- skipping", checkpointKey);
            return;
        }
        long totalNodes = 0;
        for (String label : nodeLabels) {
            totalNodes += migrateNodes(sink, label);
        }
        long totalRels = 0;
        for (RelationshipSpec spec : relationshipSpecs) {
            totalRels += migrateRelationships(sink, spec);
        }
        checkpoints.save(checkpointKey, PARTITION_DONE);
        log.info("neo4j source[{}]: migration complete -- {} node(s), {} relationship(s)", checkpointKey, totalNodes, totalRels);
    }

    private long migrateNodes(Sink sink, String label) throws Exception {
        long count = 0;
        List<ChangeEvent> batch = new ArrayList<>(BATCH_SIZE);
        try (Session session = sourceDriver.session()) {
            Result result = session.run("MATCH (n:" + label + ") RETURN n");
            while (result.hasNext()) {
                Record rec = result.next();
                batch.add(nodeInsertEvent(rec.get("n").asNode()));
                count++;
                if (batch.size() >= BATCH_SIZE) {
                    sink.applyBatch(batch);
                    batch.clear();
                }
            }
        }
        if (!batch.isEmpty()) {
            sink.applyBatch(batch);
        }
        log.info("neo4j source[{}]: migrated {} node(s) labeled {}", checkpointKey, count, label);
        return count;
    }

    /** Migrates relationship TOPOLOGY (from/to node identity, correctly correlated per matched
     * pattern instance) and TYPE (already known from {@code spec}, never needs to come back from
     * the query) -- deliberately does NOT attempt to migrate per-relationship PROPERTIES in v1.
     * Real Neo4j's Cypher fully supports {@code RETURN r, a, b} to get a relationship's own
     * property map back; boltwire, this connector's own real test target, does not -- confirmed
     * live: {@code RETURN r} fails with a real "which isn't a matched node" error, since
     * boltwire's RETURN clause only supports bound NODE variables at all, not relationships. Since
     * this project's own testing discipline is "verify against real infrastructure, don't ship an
     * untestable code path," relationship properties are a real, separately scoped follow-up
     * rather than code that would only ever be exercised against real Neo4j, never this project's
     * own emulation target. */
    private long migrateRelationships(Sink sink, RelationshipSpec spec) throws Exception {
        long count = 0;
        List<ChangeEvent> batch = new ArrayList<>(BATCH_SIZE);
        try (Session session = sourceDriver.session()) {
            String query = "MATCH (a:" + spec.fromLabel() + ")-[r:" + spec.type() + "]->(b:" + spec.toLabel() + ") RETURN a, b";
            Result result = session.run(query);
            while (result.hasNext()) {
                Record rec = result.next();
                batch.add(edgeInsertEvent(spec.type(), rec.get("a").asNode(), rec.get("b").asNode()));
                count++;
                if (batch.size() >= BATCH_SIZE) {
                    sink.applyBatch(batch);
                    batch.clear();
                }
            }
        }
        if (!batch.isEmpty()) {
            sink.applyBatch(batch);
        }
        log.info("neo4j source[{}]: migrated {} relationship(s) of type {}", checkpointKey, count, spec.type());
        return count;
    }

    /** One round trip, not two: the data-modifying CTE inserts the real node row (matching
     * boltwire's own physical schema exactly) and, in the SAME statement, records the source-to-
     * target id correlation this connector's own {@code migration_neo4j_id_map} needs for
     * relationship resolution later. */
    private ChangeEvent nodeInsertEvent(Node n) {
        String labelsLiteral = "{" + String.join(",", n.labels()) + "}"; // Postgres text[] literal syntax
        String propsJson = GSON.toJson(n.asMap());
        String sql = "WITH inserted AS ("
                + "INSERT INTO warp_graph_nodes (labels, properties) VALUES (?::text[], ?::jsonb) RETURNING id) "
                + "INSERT INTO migration_neo4j_id_map (source_element_id, target_node_id) "
                + "SELECT ?, id FROM inserted ON CONFLICT (source_element_id) DO NOTHING";
        return new ChangeEvent(sql, List.of(labelsLiteral, propsJson, n.elementId()));
    }

    private ChangeEvent edgeInsertEvent(String relationshipType, Node from, Node to) {
        // properties defaults to '{}' (the target column's own DDL default) -- see this class's
        // migrateRelationships' own javadoc for why per-relationship properties aren't migrated.
        // Explicit ::text casts on every bind param, not optional here: unlike a plain INSERT ...
        // VALUES (where the target column list gives every "?" an unambiguous type), a bare
        // SELECT expression list has no such context, and Postgres's own type inference guessed
        // wrong (a real "operator does not exist: text = bigint" error, confirmed live) without
        // these.
        String sql = "INSERT INTO warp_graph_edges (type, from_id, to_id) SELECT ?::text, "
                + "(SELECT target_node_id FROM migration_neo4j_id_map WHERE source_element_id = ?::text), "
                + "(SELECT target_node_id FROM migration_neo4j_id_map WHERE source_element_id = ?::text)";
        return new ChangeEvent(sql, List.of(relationshipType, from.elementId(), to.elementId()));
    }

    /** No-op -- see this class's own javadoc: there is no resumable change-feed position for a
     * source with no change-tracking capability at all. */
    @Override
    public void prepareChangeFeed(Sink sink, StateStore checkpoints) {
    }

    /** Returns immediately, logging why, rather than blocking forever like every other
     * connector's live change feed -- see this class's own javadoc. This is CORRECT behavior for
     * a source with no live tail to follow, not a bug: {@link com.sayonora.migration.coordinator.Coordinator#run}
     * simply finishes once this returns. */
    @Override
    public void streamChanges(Sink sink, StateStore checkpoints) {
        log.info("neo4j source[{}]: no live change-feed capability exists for this source "
                + "(Neo4j CDC is Enterprise-only; boltwire doesn't implement it either) -- "
                + "the one-time graph migration above is already complete, nothing further to do", checkpointKey);
    }

    @Override
    public void close() {
        // No persistent resources of this connector's own to release -- sourceDriver is owned
        // and closed by the caller, same convention as every other connector's source client.
    }
}
