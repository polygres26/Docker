package com.nexagres.migration.connectors.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.nexagres.migration.verify.RowChecksum;
import com.nexagres.migration.verify.VerificationResult;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.bson.Document;

/**
 * Post-backfill (or spot-check, any time) verification for {@link MongoSource}'s target: compares
 * the source collection against the target's real physical schema ({@code "db"."collection"},
 * {@code id text}/{@code doc jsonb} -- see {@link MongoSource}'s own javadoc) via count + an
 * order-independent checksum ({@link RowChecksum}), so a shard-routing misconfiguration or a
 * silently-failed batch (see this session's own migration-plan discussion: "a shard-routing
 * misconfiguration... is a real, silent failure mode per the agent's finding") shows up as a real,
 * detected mismatch instead of an assumed-correct migration.
 *
 * <p>Reads the ENTIRE source collection and the entire target table client-side to compute the
 * checksum -- correct, but real memory/time cost scales with collection size; a real follow-up
 * (chunked/streaming verification with partial checksums merged, mirroring the same partitioning
 * {@link MongoSource#listPartitions} already does for the snapshot itself) is a genuine, scoped
 * next step once collections large enough to make this matter show up, not pretended away here.
 */
public final class MongoVerifier {

    private MongoVerifier() {
    }

    public static VerificationResult verify(MongoClient sourceClient, String sourceDb, String sourceCollection,
            Connection targetConnection, String targetDb, String targetCollection) throws SQLException {
        long[] sourceCountAndChecksum = checksumSource(sourceClient, sourceDb, sourceCollection);
        long[] targetCountAndChecksum = checksumTarget(targetConnection, targetDb, targetCollection);
        return new VerificationResult(sourceCountAndChecksum[0], targetCountAndChecksum[0],
                sourceCountAndChecksum[1], targetCountAndChecksum[1]);
    }

    /** {@code [count, checksum]} -- same id/doc JSON serialization {@link MongoSource} itself
     * writes with ({@link MongoBsonJson}), or the checksum would never match even on perfectly
     * replicated data. */
    private static long[] checksumSource(MongoClient client, String db, String collection) {
        MongoCollection<Document> source = client.getDatabase(db).getCollection(collection);
        long count = 0;
        long checksum = 0;
        try (MongoCursor<Document> cursor = source.find().iterator()) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                String idJson = MongoBsonJson.valueToJson(doc.get("_id"));
                String docJson = MongoBsonJson.toJson(doc);
                checksum = RowChecksum.combineJson(checksum, idJson, docJson);
                count++;
            }
        }
        return new long[] { count, checksum };
    }

    private static long[] checksumTarget(Connection conn, String db, String collection) throws SQLException {
        String table = "\"" + db + "\".\"" + collection + "\"";
        long count = 0;
        long checksum = 0;
        try (PreparedStatement ps = conn.prepareStatement("SELECT id, doc FROM " + table);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                checksum = RowChecksum.combineJson(checksum, rs.getString("id"), rs.getString("doc"));
                count++;
            }
        }
        return new long[] { count, checksum };
    }
}
