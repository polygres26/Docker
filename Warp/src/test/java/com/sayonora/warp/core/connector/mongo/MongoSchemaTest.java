package com.sayonora.warp.core.connector.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClient;
import com.sayonora.warp.core.connector.ConnectorOperands;
import com.sayonora.warp.testsupport.RealMongo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.ScannableTable;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Real MongoDB container, same shape as ThinkingSense's {@code MongoSchemaFactoryTest}: real
 * documents with a known count and heterogeneous field types, read back through the real
 * connector built from a real {@code mongodb://} pseudo-URL. */
class MongoSchemaTest {

    private static final String DATABASE = "warp_connector_test";
    private static final int ROW_COUNT = 5;
    private static RealMongo mongo;

    @BeforeAll
    static void start() throws Exception {
        mongo = RealMongo.start();
        try (MongoClient c = mongo.client()) {
            List<Document> docs = new ArrayList<>();
            for (int i = 0; i < ROW_COUNT; i++) {
                docs.add(new Document("id", i).append("name", "widget-" + i).append("amount", i * 1.5));
            }
            c.getDatabase(DATABASE).getCollection("widgets").insertMany(docs);
        }
    }

    @AfterAll
    static void stop() {
        if (mongo != null) mongo.close();
    }

    private static Map<String, Object> operand(String query) {
        return ConnectorOperands.parse(mongo.connectionString() + "/" + DATABASE + query, null, null);
    }

    @Test
    void mountsARealQueryableCollectionWithStringifiedValues() {
        try (MongoSchema schema = MongoSchemaFactory.createSchema(operand("?table.widgets=id,name,amount"))) {
            var table = schema.tables().get("widgets");
            assertNotNull(table);
            assertInstanceOf(ScannableTable.class, table);
            List<Object[]> rows = new ArrayList<>();
            ((ScannableTable) table).scan(null).forEach(rows::add);
            assertEquals(ROW_COUNT, rows.size());
            // int, string, double -- all stringified consistently (VARCHAR-everywhere posture).
            assertTrue(rows.stream().anyMatch(r -> "2".equals(r[0]) && "widget-2".equals(r[1]) && "3.0".equals(r[2])));
        }
    }

    @Test
    void sourceMapsSqlNameToADifferentRealCollection() {
        try (MongoSchema schema = MongoSchemaFactory.createSchema(
                operand("?table.w=name&source.w=widgets&serverSelectionTimeoutMS=5000"))) {
            int[] count = {0};
            ((ScannableTable) schema.tables().get("w")).scan(null).forEach(r -> count[0]++);
            assertEquals(ROW_COUNT, count[0]);
        }
    }

    @Test
    void rejectsBackendWithNoDeclaredCollections() {
        assertThrows(IllegalArgumentException.class, () -> MongoSchemaFactory.createSchema(operand("")));
    }
}
