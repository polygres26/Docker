package com.sayonora.warp.dynamowire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class PartiQlParserTest {

    private static Object parse(String stmt, AttributeValue... params) {
        return PartiQl.parseForTest(stmt, List.of(params));
    }

    @Test
    void parsesTheStatementKinds() {
        assertTrue(parse("SELECT * FROM \"t\" WHERE pk = 'a' AND n > 5").toString().contains("Select"));
        assertTrue(parse("SELECT a, b.c, l[1] FROM \"t\".\"idx\" WHERE x IN [1,2] ORDER BY sk DESC").toString().contains("index=idx"));
        assertTrue(parse("INSERT INTO t VALUE {'pk': 'a', 'n': 1, 'l': [1,'x'], 'm': {'k': true}, 's': <<'a','b'>>, 'z': NULL}").toString().contains("Insert"));
        assertTrue(parse("UPDATE t SET a = a + 1, b = 'x' SET c = list_append(c, [1]) REMOVE d, e WHERE pk = ? AND sk = ?",
                AttributeValue.ofS("a"), AttributeValue.ofS("b")).toString().contains("Update"));
        assertTrue(parse("DELETE FROM t WHERE pk = 'a' AND sk = 'b' RETURNING ALL OLD *").toString().contains("ALL_OLD"));
        assertTrue(parse("select * from t where a between 1 and 2 or b is missing and not c is null;").toString().contains("Select"));
        assertTrue(parse("SELECT * FROM t WHERE begins_with(a, 'x') AND contains(b, 'y') AND attribute_type(c, 'S') AND size(d) > 1").toString().contains("Select"));
        assertTrue(parse("SELECT * FROM t WHERE n = -5 AND m = 1.5e3").toString().contains("Select"));
    }

    @Test
    void rejectsMalformedStatementsAndWrongParameterCounts() {
        for (String bad : new String[] {"SELEC * FROM t", "SELECT FROM t", "SELECT * FROM", "INSERT INTO t VALUE", "UPDATE t WHERE pk = 1",
                "SELECT * FROM t WHERE", "SELECT * FROM t WHERE a = 'unterminated", "DELETE t WHERE a = 1", "SELECT * FROM t WHERE a ~ 1"}) {
            DynamoException e = assertThrows(DynamoException.class, () -> parse(bad), bad);
            assertTrue(e.getMessage().startsWith("Statement wasn't well formed"), bad + " -> " + e.getMessage());
        }
        assertEquals("Number of parameters in request and statement don't match.",
                assertThrows(DynamoException.class, () -> parse("SELECT * FROM t WHERE a = ?")).getMessage());
        assertEquals("Number of parameters in request and statement don't match.",
                assertThrows(DynamoException.class, () -> parse("SELECT * FROM t WHERE a = 1", AttributeValue.ofS("x"))).getMessage());
    }
}
