package com.sayonora.warp.datastorewire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.datastore.v1.GqlQuery;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import org.junit.jupiter.api.Test;

class DsGqlTest {

    static final DsKeys.Part PART = new DsKeys.Part("p", "", "");

    static Query parse(String text, boolean literals) {
        return DsGql.parse(GqlQuery.newBuilder().setQueryString(text).setAllowLiterals(literals).build(), PART).query;
    }

    @Test
    void selectWhereOrderLimitOffset() {
        Query q = parse("select name, age from P where age > 25 and city = 'Paris' order by age desc, name limit 5 offset 2", true);
        assertEquals("P", q.getKind(0).getName());
        assertEquals(2, q.getProjectionCount());
        assertEquals(2, q.getFilter().getCompositeFilter().getFiltersCount());
        assertEquals(PropertyFilter.Operator.GREATER_THAN, q.getFilter().getCompositeFilter().getFilters(0).getPropertyFilter().getOp());
        assertEquals("Paris", q.getFilter().getCompositeFilter().getFilters(1).getPropertyFilter().getValue().getStringValue());
        assertEquals(2, q.getOrderCount());
        assertEquals(5, q.getLimit().getValue());
        assertEquals(2, q.getOffset());
    }

    @Test
    void literalsAreRefusedUnlessAllowed() {
        assertEquals("Disallowed literal: 25.", assertThrows(DsException.class, () -> parse("SELECT * FROM P WHERE age > 25", false)).getMessage());
        assertEquals("Disallowed literal: 'Paris'.", assertThrows(DsException.class, () -> parse("SELECT * FROM P WHERE city = 'Paris'", false)).getMessage());
        Query q = DsGql.parse(GqlQuery.newBuilder().setQueryString("SELECT * FROM P WHERE age > @a AND n = @1 LIMIT @lim")
                .putNamedBindings("a", com.google.datastore.v1.GqlQueryParameter.newBuilder().setValue(Value.newBuilder().setIntegerValue(7)).build())
                .putNamedBindings("lim", com.google.datastore.v1.GqlQueryParameter.newBuilder().setValue(Value.newBuilder().setIntegerValue(3)).build())
                .addPositionalBindings(com.google.datastore.v1.GqlQueryParameter.newBuilder().setValue(Value.newBuilder().setStringValue("x")).build()).build(), PART).query;
        assertEquals(7, q.getFilter().getCompositeFilter().getFilters(0).getPropertyFilter().getValue().getIntegerValue());
        assertEquals(3, q.getLimit().getValue());
        assertEquals("Absent named binding: zz.", assertThrows(DsException.class, () -> parse("SELECT * FROM P WHERE a = @zz", false)).getMessage());
    }

    @Test
    void keysDistinctAncestorAndErrors() {
        Query q = parse("SELECT DISTINCT ON (city) name, city FROM P WHERE __key__ HAS ANCESTOR KEY(Root, 'r1', Child, 5) ORDER BY city", true);
        assertEquals(1, q.getDistinctOnCount());
        PropertyFilter pf = q.getFilter().getPropertyFilter();
        assertEquals(PropertyFilter.Operator.HAS_ANCESTOR, pf.getOp());
        assertEquals(2, pf.getValue().getKeyValue().getPathCount());
        assertEquals(5, pf.getValue().getKeyValue().getPath(1).getId());
        assertTrue(parse("SELECT DISTINCT city FROM P", true).getDistinctOnCount() == 1);
        assertThrows(DsException.class, () -> parse("SELECT * FROM", true));
        assertThrows(DsException.class, () -> parse("DELETE FROM P", true));
        assertThrows(DsException.class, () -> parse("SELECT * FROM P WHERE a = 1 OR b = 2", true));
        assertEquals(PropertyFilter.Operator.IN, parse("SELECT * FROM P WHERE a IN (1, 2)", true).getFilter().getPropertyFilter().getOp());
    }
}
