package com.sayonora.warp.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sayonora.warp.core.BackendRegistry;
import com.sayonora.warp.core.BackendTarget;
import com.sayonora.warp.server.ServerOptions.McpBackendMode;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Type derivation from backend definitions, and the scope -> in-scope backend resolution. */
class BackendTypesTest {

    private static BackendTarget t(String url) {
        return new BackendTarget("x", url, "u", "p");
    }

    @Test
    void typeIsDerivedFromTheDefinitionNeverConfigured() {
        assertEquals("postgres", BackendTypes.typeOf(t("jdbc:postgresql://h:5432/db")));
        assertEquals("mysql", BackendTypes.typeOf(t("jdbc:mysql://h/db")));
        assertEquals("mariadb", BackendTypes.typeOf(t("jdbc:mariadb://h/db")));
        assertEquals("oracle", BackendTypes.typeOf(t("jdbc:oracle:thin:@h:1521/x")));
        assertEquals("sqlserver", BackendTypes.typeOf(t("jdbc:sqlserver://h;databaseName=d")));
        assertEquals("snowflake", BackendTypes.typeOf(t("jdbc:snowflake://a.snowflakecomputing.com")));
        assertEquals("mongodb", BackendTypes.typeOf(t("mongodb://h:27017/db")));
        assertEquals("dynamodb", BackendTypes.typeOf(t("dynamodb://us-east-1")));
        assertEquals("s3", BackendTypes.typeOf(t("s3://bucket")));
        assertEquals("kafka", BackendTypes.typeOf(t("kafka://h:9092/topic")));
        assertEquals("cassandra", BackendTypes.typeOf(t("cassandra://h:9042/ks.t")));
        assertEquals("splunk", BackendTypes.typeOf(t("splunk://h:8089")));
        assertEquals("jdbc", BackendTypes.typeOf(t("jdbc:weirddb://h")));
    }

    @Test
    void familyDecidesTheToolVocabulary() {
        assertEquals(BackendKind.RELATIONAL, BackendTypes.kindOf(t("jdbc:postgresql://h/db")));
        assertEquals(BackendKind.RELATIONAL, BackendTypes.kindOf(t("jdbc:trino://h/c")));
        assertEquals(BackendKind.MONGODB, BackendTypes.kindOf(t("mongodb+srv://h/db")));
        assertEquals(BackendKind.DYNAMODB, BackendTypes.kindOf(t("dynamodb://us-east-1")));
        assertEquals(BackendKind.S3, BackendTypes.kindOf(t("s3://b")));
        assertEquals(BackendKind.KAFKA, BackendTypes.kindOf(t("kafka://h:1/t")));
    }

    @Test
    void legacyKindListParsing() {
        assertEquals(Set.copyOf(List.of(BackendKind.values())), Set.copyOf(BackendKind.parseList(null)));
        assertEquals(Set.of(BackendKind.RELATIONAL, BackendKind.MONGODB), BackendKind.parseList("sql, mongo"));
        assertTrue(BackendKind.parseList("s3").contains(BackendKind.S3));
    }

    private static BackendRegistry registry() {
        return BackendRegistry.fromConfig(
                "default=jdbc:postgresql://h/d|u|p;pg2=jdbc:postgresql://h2/d|u|p;m=mongodb://h:1/db|u|p",
                null, null, "team:plain=pg2,m", null, java.util.Map.of());
    }

    @Test
    void scopeResolvesToTheBackendsItContains() {
        McpBackendCatalog cat = new McpBackendCatalog(registry(), new EmulatedStores(null), Set.of(),
                McpBackendMode.POSTGRES);
        Set<BackendKind> all = Set.of(BackendKind.values());
        assertEquals(List.of("default", "m", "pg2"),
                cat.inScope(McpScope.all(), all).stream().map(McpBackend::name).toList());
        List<McpBackend> one = cat.inScope(McpScope.database("m"), all);
        assertEquals(1, one.size());
        assertEquals("mongodb", one.get(0).type());
        assertEquals(List.of("m", "pg2"), cat.inScope(McpScope.group("team"), all).stream().map(McpBackend::name).toList());
        assertTrue(cat.inScope(McpScope.database("nope"), all).isEmpty());
        // the legacy family filter restricts by tool family
        assertEquals(List.of("m"), cat.inScope(McpScope.all(), Set.of(BackendKind.MONGODB)).stream()
                .map(McpBackend::name).toList());
    }

    @Test
    void descriptionsAreRegistryConfigAndNeverFailAReload() {
        BackendRegistry r = registry();
        assertNull(r.descriptionOf("pg2"));
        r.applyDescriptions("{\"pg2\":\"Orders DB\"}", "{\"team\":\"Team set\"}");
        assertEquals("Orders DB", r.descriptionOf("pg2"));
        assertEquals("Team set", r.groupDescriptionOf("team"));
        r.applyDescriptions("not json", null);
        assertNull(r.descriptionOf("pg2"));
        assertEquals(List.of("team"), List.of(r.groupInfoFor("pg2").name()));
    }

    @Test
    void splunkAndOtherConnectorsDescribeWithoutContactingTheServer() throws Exception {
        BackendTarget splunk = new BackendTarget("logs", "splunk://splunk.example:8089?table=web_errors&search=index%3Dmain%20error",
                "", "Splunk token");
        McpBackend b = new McpBackend("logs", BackendTypes.typeOf(splunk), BackendTypes.kindOf(splunk), false, null, splunk);
        BackendToolProvider p = new ConnectorDescribeProvider(BackendKind.SPLUNK);
        assertTrue(p.tools().isEmpty());
        com.google.gson.JsonObject contents = p.describe(new BackendToolProvider.Ctx() {
            @Override
            public McpBackend backend() {
                return b;
            }

            @Override
            public com.sayonora.warp.core.AdHocQueryRunner.Result sql(String sql) {
                throw new UnsupportedOperationException();
            }
        });
        assertTrue(contents.getAsJsonObject("declaredTables").has("web_errors"));
        assertTrue(contents.get("note").getAsString().contains("Splunk"));
        assertTrue(p.call("x", new com.google.gson.JsonObject(), null).isError());
    }
}
