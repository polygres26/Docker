package com.sayonora.wire.cosmoswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Expected values are written by hand from the documented Cosmos DB NoSQL query language semantics (learn.microsoft.com, "Azure Cosmos DB
 * for NoSQL query language": SELECT, FROM, WHERE, ORDER BY, GROUP BY, OFFSET LIMIT, subqueries, operators, system functions), using the
 * documentation's "Families" sample data. NOT compared with a real service; see cosmos_known.md.
 */
class CosmosSqlTest {

    static final String ANDERSEN = "{\"id\":\"AndersenFamily\",\"lastName\":\"Andersen\",\"parents\":[{\"firstName\":\"Thomas\"},{\"firstName\":\"Mary Kay\"}],"
            + "\"children\":[{\"firstName\":\"Henriette Thaulow\",\"gender\":\"female\",\"grade\":5,\"pets\":[{\"givenName\":\"Fluffy\"}]}],"
            + "\"address\":{\"state\":\"WA\",\"county\":\"King\",\"city\":\"Seattle\"},\"creationDate\":1431620472,\"isRegistered\":true}";
    static final String WAKEFIELD = "{\"id\":\"WakefieldFamily\",\"parents\":[{\"familyName\":\"Wakefield\",\"givenName\":\"Robin\"},"
            + "{\"familyName\":\"Miller\",\"givenName\":\"Ben\"}],\"children\":[{\"familyName\":\"Merriam\",\"givenName\":\"Jesse\",\"gender\":\"female\","
            + "\"grade\":1,\"pets\":[{\"givenName\":\"Goofy\"},{\"givenName\":\"Shadow\"}]},{\"familyName\":\"Miller\",\"givenName\":\"Lisa\","
            + "\"gender\":\"female\",\"grade\":8}],\"address\":{\"state\":\"NY\",\"county\":\"Manhattan\",\"city\":\"NY\"},\"creationDate\":1431620462,"
            + "\"isRegistered\":false}";

    static List<JsonElement> docs(String... json) {
        List<JsonElement> l = new ArrayList<>();
        for (String j : json) {
            l.add(JsonParser.parseString(j));
        }
        return l;
    }

    static String arr(List<JsonElement> rows) {
        JsonArray a = new JsonArray();
        rows.forEach(a::add);
        return a.toString();
    }

    static String run(String sql, List<JsonElement> docs) {
        return arr(CosmosQuery.compile(sql, null, 0).runAll(docs));
    }

    static String runP(String sql, String params, List<JsonElement> docs) {
        return arr(CosmosQuery.compile(sql, JsonParser.parseString(params).getAsJsonArray(), 0).runAll(docs));
    }

    static final List<JsonElement> FAM = docs(ANDERSEN, WAKEFIELD);

    @Test
    void selectStarWhereId() {
        assertEquals("[" + ANDERSEN + "]", run("SELECT * FROM Families f WHERE f.id = \"AndersenFamily\"", FAM));
    }

    @Test
    void projectionKeysAndImplicitNames() {
        assertEquals("[{\"city\":\"Seattle\",\"state\":\"WA\"}]", run("SELECT f.address.city, f.address.state FROM Families f WHERE f.id = 'AndersenFamily'", FAM));
        assertEquals("[{\"$1\":2}]", run("SELECT 1+1 FROM r", docs("{}")));
        assertEquals("[{\"Family\":{\"Name\":\"WakefieldFamily\",\"City\":\"NY\"}}]",
                run("SELECT {\"Name\":f.id, \"City\":f.address.city} AS Family FROM Families f WHERE f.address.city = f.address.state", FAM));
    }

    @Test
    void selectValueDropsUndefined() {
        assertEquals("[\"Andersen\"]", run("SELECT VALUE f.lastName FROM Families f", FAM));
        assertEquals("[{\"state\":\"WA\",\"county\":\"King\",\"city\":\"Seattle\"},{\"state\":\"NY\",\"county\":\"Manhattan\",\"city\":\"NY\"}]",
                run("SELECT VALUE f.address FROM Families f", FAM));
    }

    @Test
    void joinIteratesArrays() {
        assertEquals("[{\"n\":\"Jesse\"},{\"n\":\"Lisa\"}]", run("SELECT c.givenName AS n FROM Families f JOIN c IN f.children", docs(WAKEFIELD)));
        assertEquals("[{\"givenName\":\"Jesse\",\"petName\":\"Goofy\"},{\"givenName\":\"Jesse\",\"petName\":\"Shadow\"}]",
                run("SELECT c.givenName, p.givenName AS petName FROM Families f JOIN c IN f.children JOIN p IN c.pets", docs(WAKEFIELD)));
    }

    @Test
    void whereOperatorsAndUndefined() {
        // a comparison with an undefined operand is undefined and WHERE keeps only true
        assertEquals("[\"AndersenFamily\"]", run("SELECT VALUE f.id FROM Families f WHERE f.lastName = 'Andersen'", FAM));
        assertEquals("[]", run("SELECT VALUE f.id FROM Families f WHERE f.lastName != 'Andersen'", FAM));
        assertEquals("[\"AndersenFamily\",\"WakefieldFamily\"]", run("SELECT VALUE f.id FROM Families f WHERE f.address.state IN ('NY','WA')", FAM));
        assertEquals("[\"WakefieldFamily\"]", run("SELECT VALUE f.id FROM Families f WHERE f.creationDate BETWEEN 1431620400 AND 1431620470", FAM));
        assertEquals("[\"AndersenFamily\"]", run("SELECT VALUE f.id FROM Families f WHERE NOT IS_DEFINED(f.parents[0].givenName)", FAM));
        // cross-type equality is false, ordering comparison across types undefined
        assertEquals("[false]", run("SELECT VALUE (1 = \"1\") FROM r", docs("{}")));
        assertEquals("[{}]", run("SELECT (1 < \"a\") AS x FROM r", docs("{}")));
        assertEquals("[{\"x\":true}]", run("SELECT (1 != \"a\") AS x FROM r", docs("{}")));
    }

    @Test
    void threeValuedLogic() {
        assertEquals("[{\"a\":false,\"b\":true}]", run("SELECT (false AND undefined) AS a, (true OR undefined) AS b FROM r", docs("{}")));
        assertEquals("[{}]", run("SELECT (true AND undefined) AS a FROM r", docs("{}")));
        assertEquals("[{\"a\":\"x\"}]", run("SELECT (undefined ?? 'x') AS a FROM r", docs("{}")));
        assertEquals("[{\"a\":null}]", run("SELECT (null ?? 'x') AS a FROM r", docs("{}")));
        assertEquals("[{\"a\":1}]", run("SELECT (true ? 1 : 2) AS a FROM r", docs("{}")));
    }

    @Test
    void orderByTypeOrder() {
        // undefined < null < boolean < number < string < array < object
        List<JsonElement> d = docs("{\"id\":1,\"v\":{\"a\":1}}", "{\"id\":2,\"v\":\"s\"}", "{\"id\":3,\"v\":5}", "{\"id\":4,\"v\":true}", "{\"id\":5,\"v\":null}",
                "{\"id\":6}", "{\"id\":7,\"v\":[1]}");
        assertEquals("[6,5,4,3,2,7,1]", run("SELECT VALUE c.id FROM c ORDER BY c.v", d));
        assertEquals("[1,7,2,3,4,5,6]", run("SELECT VALUE c.id FROM c ORDER BY c.v DESC", d));
    }

    @Test
    void orderByCompositeTopOffsetDistinct() {
        List<JsonElement> d = docs("{\"id\":\"a\",\"g\":1,\"n\":3}", "{\"id\":\"b\",\"g\":2,\"n\":1}", "{\"id\":\"c\",\"g\":1,\"n\":2}", "{\"id\":\"d\",\"g\":2,\"n\":1}");
        assertEquals("[\"c\",\"a\",\"b\",\"d\"]", run("SELECT VALUE c.id FROM c ORDER BY c.g ASC, c.n ASC, c.id ASC", d));
        assertEquals("[\"d\",\"b\"]", run("SELECT TOP 2 VALUE c.id FROM c ORDER BY c.g DESC, c.n ASC, c.id DESC", d));
        assertEquals("[\"b\",\"c\"]", run("SELECT VALUE c.id FROM c ORDER BY c.id OFFSET 1 LIMIT 2", d));
        assertEquals("[1,2]", run("SELECT DISTINCT VALUE c.g FROM c ORDER BY c.g", d));
    }

    @Test
    void aggregatesAndGroupBy() {
        assertEquals("[2]", run("SELECT VALUE COUNT(1) FROM Families f", FAM));
        assertEquals("[0]", run("SELECT VALUE COUNT(1) FROM Families f WHERE f.id = 'zzz'", FAM));
        assertEquals("[1431620472]", run("SELECT VALUE MAX(f.creationDate) FROM Families f", FAM));
        assertEquals("[2863240934]", run("SELECT VALUE SUM(f.creationDate) FROM Families f", FAM));
        assertEquals("[1431620467]", run("SELECT VALUE AVG(f.creationDate) FROM Families f", FAM));
        assertEquals("[]", run("SELECT VALUE SUM(f.nothing) FROM Families f", FAM));
        assertEquals("[{\"n\":1}]", run("SELECT COUNT(f.lastName) AS n FROM Families f", FAM));
        List<JsonElement> d = docs("{\"g\":\"x\",\"n\":1}", "{\"g\":\"y\",\"n\":5}", "{\"g\":\"x\",\"n\":2}");
        assertEquals("[{\"g\":\"x\",\"c\":2,\"s\":3},{\"g\":\"y\",\"c\":1,\"s\":5}]", run("SELECT c.g, COUNT(1) AS c, SUM(c.n) AS s FROM c GROUP BY c.g", d));
    }

    @Test
    void subqueries() {
        assertEquals("[\"AndersenFamily\"]", run("SELECT VALUE f.id FROM Families f WHERE EXISTS(SELECT VALUE p FROM p IN f.parents WHERE p.firstName = 'Thomas')", FAM));
        assertEquals("[[\"Fluffy\"],[\"Goofy\",\"Shadow\"]]",
                run("SELECT VALUE ARRAY(SELECT VALUE p.givenName FROM c IN f.children JOIN p IN c.pets) FROM Families f", FAM));
        assertEquals("[1,2]", run("SELECT VALUE (SELECT VALUE COUNT(1) FROM c IN f.children) FROM Families f", FAM));
    }

    @Test
    void parameters() {
        assertEquals("[\"WakefieldFamily\"]", runP("SELECT VALUE f.id FROM Families f WHERE f.address.state = @s", "[{\"name\":\"@s\",\"value\":\"NY\"}]", FAM));
    }

    @Test
    void mathAndStringFunctionsFromDocExamples() {
        assertEquals("[{\"$1\":1,\"$2\":0,\"$3\":1}]", run("SELECT ABS(-1), ABS(0), ABS(1) FROM r", docs("{}")));
        assertEquals("[{\"$1\":124,\"$2\":-123,\"$3\":0}]", run("SELECT CEILING(123.45), CEILING(-123.45), CEILING(0.0) FROM r", docs("{}")));
        assertEquals("[{\"$1\":123,\"$2\":-124,\"$3\":0}]", run("SELECT FLOOR(123.45), FLOOR(-123.45), FLOOR(0.0) FROM r", docs("{}")));
        assertEquals("[{\"$1\":2,\"$2\":3,\"$3\":3,\"$4\":-2,\"$5\":-3}]", run("SELECT ROUND(2.4), ROUND(2.6), ROUND(2.5), ROUND(-2.4), ROUND(-2.6) FROM r", docs("{}")));
        assertEquals("[{\"$1\":2,\"$2\":2,\"$3\":2,\"$4\":-2,\"$5\":-2}]", run("SELECT TRUNC(2.4), TRUNC(2.6), TRUNC(2.5), TRUNC(-2.4), TRUNC(-2.6) FROM r", docs("{}")));
        assertEquals("[{\"$1\":8,\"$2\":15.625}]", run("SELECT POWER(2, 3), POWER(2.5, 3) FROM r", docs("{}")));
        assertEquals("[{\"$1\":\"b\",\"$2\":0,\"$3\":\"ab\",\"$4\":\"cbA\",\"$5\":\"aaa\"}]",
                run("SELECT SUBSTRING(\"abc\", 1, 1), INDEX_OF(\"abc\", \"ab\"), LEFT(\"abc\", 2), REVERSE(\"Abc\"), REPLICATE(\"a\", 3) FROM r", docs("{}")));
        assertEquals("[{\"$1\":false,\"$2\":false,\"$3\":true}]", run("SELECT STARTSWITH(\"abc\", \"b\"), ENDSWITH(\"abc\", \"b\"), CONTAINS(\"abc\", \"ab\") FROM r", docs("{}")));
        assertEquals("[{\"$1\":true,\"$2\":true}]", run("SELECT REGEXMATCH(\"abcd\", \"ABC\", \"i\"), STRINGEQUALS(\"abc\", \"ABC\", true) FROM r", docs("{}")));
        assertEquals("[{\"$1\":[\"strawberries\",\"bananas\"],\"$2\":[\"strawberries\"],\"$3\":[]}]",
                run("SELECT ARRAY_SLICE([\"apples\",\"strawberries\",\"bananas\"], 1), ARRAY_SLICE([\"apples\",\"strawberries\",\"bananas\"], -2, 1), "
                        + "ARRAY_SLICE([\"apples\",\"strawberries\",\"bananas\"], 1, 0) FROM r", docs("{}")));
        assertEquals("[{\"$1\":true,\"$2\":false,\"$3\":true}]", run("SELECT IS_ARRAY([1]), IS_BOOL(1), IS_NULL(null) FROM r", docs("{}")));
        assertEquals("[{\"$1\":true}]", run("SELECT ARRAY_CONTAINS([{\"a\":1,\"b\":2}], {\"a\":1}, true) FROM r", docs("{}")));
        assertEquals("[{\"$1\":\"2021-01-01T00:00:00.0000000Z\"}]", run("SELECT DateTimeAdd(\"yyyy\", 1, \"2020-01-01T00:00:00.0000000Z\") FROM r", docs("{}")));
    }

    @Test
    void likeAndConcat() {
        assertEquals("[\"AndersenFamily\"]", run("SELECT VALUE f.id FROM Families f WHERE f.id LIKE 'And%'", FAM));
        assertEquals("[\"WakefieldFamily\"]", run("SELECT VALUE f.id FROM Families f WHERE f.id NOT LIKE '%sen_amily'", FAM));
        assertEquals("[{\"a\":\"xy\"}]", run("SELECT ('x' || 'y') AS a FROM r", docs("{}")));
    }

    @Test
    void syntaxAndUnsupported() {
        CosmosException e = assertThrows(CosmosException.class, () -> CosmosQuery.compile("SELEC * FROM c", null));
        assertEquals(400, e.status);
        e = assertThrows(CosmosException.class, () -> CosmosQuery.compile("SELECT udf.foo(c.x) FROM c", null));
        assertEquals(400, e.status);
        e = assertThrows(CosmosException.class, () -> run("SELECT nope(1) FROM c", docs("{}")));
        assertEquals(400, e.status);
    }

    @Test
    void routingConstraints() {
        CosmosQuery q = CosmosQuery.compile("SELECT * FROM c WHERE c.pk = 'a' AND c.id = @i AND c.x > 1", JsonParser.parseString("[{\"name\":\"@i\",\"value\":\"7\"}]").getAsJsonArray());
        assertEquals("{/pk=\"a\", /id=\"7\"}", q.equalities().toString());
        assertEquals(true, CosmosQuery.compile("SELECT VALUE COUNT(1) FROM c", null).isPlainCount());
    }

    @Test
    void jsonOrderingAndCanon() {
        assertEquals(0, CosmosJson.compare(JsonParser.parseString("1"), JsonParser.parseString("1.0")));
        assertEquals(true, CosmosJson.equal(JsonParser.parseString("{\"a\":1,\"b\":[1,2]}"), JsonParser.parseString("{\"b\":[1,2],\"a\":1.0}")));
        JsonArray a = new JsonArray();
        assertEquals("undefined", CosmosJson.canon(null));
        assertEquals("[]", CosmosJson.canon(a));
    }
}
