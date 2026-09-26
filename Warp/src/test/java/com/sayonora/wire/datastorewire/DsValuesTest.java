package com.sayonora.wire.datastorewire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.datastore.v1.ArrayValue;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.type.LatLng;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class DsValuesTest {

    static Value i(long n) {
        return Value.newBuilder().setIntegerValue(n).build();
    }

    static Value d(double n) {
        return Value.newBuilder().setDoubleValue(n).build();
    }

    static Value s(String x) {
        return Value.newBuilder().setStringValue(x).build();
    }

    static Value arr(Value... vs) {
        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(List.of(vs))).build();
    }

    static Value ent(String k, Value v) {
        return Value.newBuilder().setEntityValue(Entity.newBuilder().putProperties(k, v)).build();
    }

    @Test
    void oneTotalOrderAcrossTypes() {
        List<Value> ordered = List.of(DsValues.nullValue(), i(-1), i(2), Value.newBuilder().setTimestampValue(Timestamp.newBuilder().setSeconds(1)).build(),
                Value.newBuilder().setBooleanValue(false).build(), Value.newBuilder().setBooleanValue(true).build(),
                Value.newBuilder().setBlobValue(ByteString.copyFrom(new byte[] {1, 2})).build(), Value.newBuilder().setBlobValue(ByteString.copyFrom(new byte[] {1, 3})).build(),
                s("apple"), s("banana"), s("é"), d(-2.5), d(1.5), d(2.0), d(Double.POSITIVE_INFINITY), d(Double.NaN),
                Value.newBuilder().setGeoPointValue(LatLng.newBuilder().setLatitude(1).setLongitude(2)).build(),
                Value.newBuilder().setGeoPointValue(LatLng.newBuilder().setLatitude(1).setLongitude(3)).build(),
                Value.newBuilder().setKeyValue(DsKeysTest.k("A", 5L)).build(), Value.newBuilder().setKeyValue(DsKeysTest.k("A", "b")).build(),
                Value.newBuilder().setKeyValue(DsKeysTest.k("K", "z")).build());
        List<Value> shuffled = new ArrayList<>(ordered);
        Collections.shuffle(shuffled, new java.util.Random(2));
        shuffled.sort(DsValues::compare);
        assertEquals(ordered, shuffled);
        // integers and doubles are separate types: an integer is never equal to a double, every double is above every integer
        assertTrue(DsValues.compare(i(2), d(2.0)) < 0);
        assertEquals(0, DsValues.compare(d(Double.NaN), d(Double.NaN)));
        // integers and timestamps share the fixed-point bracket (micros)
        assertEquals(0, DsValues.compare(i(1_000_000), Value.newBuilder().setTimestampValue(Timestamp.newBuilder().setSeconds(1)).build()));
    }

    @Test
    void indexedValuesFlattenArraysAndEmbeddedEntities() {
        Entity e = Entity.newBuilder().putProperties("tags", arr(s("a"), s("b"))).putProperties("empty", arr())
                .putProperties("nullish", DsValues.nullValue())
                .putProperties("emb", ent("x", i(1)))
                .putProperties("hidden", i(5).toBuilder().setExcludeFromIndexes(true).build())
                .putProperties("a.b", i(9))
                .putProperties("mix", arr(i(1), i(2).toBuilder().setExcludeFromIndexes(true).build())).build();
        assertEquals(List.of(s("a"), s("b")), DsValues.indexed(e, "tags"));
        assertEquals(List.of(DsValues.nullValue()), DsValues.indexed(e, "empty")); // an empty array indexes as null
        assertEquals(List.of(DsValues.nullValue()), DsValues.indexed(e, "nullish"));
        assertEquals(List.of(i(1)), DsValues.indexed(e, "emb.x"));
        assertEquals(List.of(), DsValues.indexed(e, "emb")); // the embedded entity itself is not indexed
        assertEquals(List.of(), DsValues.indexed(e, "hidden"));
        assertEquals(List.of(i(9)), DsValues.indexed(e, "a.b")); // a property whose name contains a dot
        assertEquals(List.of(i(1)), DsValues.indexed(e, "mix"));
        assertEquals(List.of(), DsValues.indexed(e, "missing"));
    }

    @Test
    void validationAndNormalisation() {
        DsKeys.Part part = new DsKeys.Part("p", "", "");
        String tooLong = "x".repeat(1501);
        assertEquals("The value of property \"s\" is longer than 1500 bytes.", assertThrows(DsException.class,
                () -> DsValues.validateAndNormalize(Entity.newBuilder().putProperties("s", s(tooLong)).build(), part)).getMessage());
        DsValues.validateAndNormalize(Entity.newBuilder().putProperties("s", s(tooLong).toBuilder().setExcludeFromIndexes(true).build()).build(), part);
        assertEquals("list_value cannot contain a Value containing another list_value.", assertThrows(DsException.class,
                () -> DsValues.validateAndNormalize(Entity.newBuilder().putProperties("a", arr(arr(i(1)))).build(), part)).getMessage());
        assertEquals("The property.name \"__bad__\" is reserved.", assertThrows(DsException.class,
                () -> DsValues.validateAndNormalize(Entity.newBuilder().putProperties("__bad__", i(1)).build(), part)).getMessage());
        Entity ts = DsValues.validateAndNormalize(Entity.newBuilder().putProperties("t", Value.newBuilder()
                .setTimestampValue(Timestamp.newBuilder().setSeconds(1).setNanos(123456789)).build()).build(), part);
        assertEquals(123456000, ts.getPropertiesMap().get("t").getTimestampValue().getNanos());
        Entity key = DsValues.validateAndNormalize(Entity.newBuilder().putProperties("k", Value.newBuilder().setKeyValue(DsKeysTest.k("Other", "a")).build()).build(), part);
        assertEquals("p", key.getPropertiesMap().get("k").getKeyValue().getPartitionId().getProjectId()); // key values carry their partition
    }
}
