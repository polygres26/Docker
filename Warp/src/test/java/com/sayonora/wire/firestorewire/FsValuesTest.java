package com.sayonora.wire.firestorewire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.type.LatLng;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FsValuesTest {

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

    static Value map(String k, Value v) {
        return Value.newBuilder().setMapValue(MapValue.newBuilder().putFields(k, v)).build();
    }

    static Value vec(double... xs) {
        ArrayValue.Builder a = ArrayValue.newBuilder();
        for (double x : xs) {
            a.addValues(d(x));
        }
        return Value.newBuilder().setMapValue(MapValue.newBuilder().putFields("__type__", s("__vector__"))
                .putFields("value", Value.newBuilder().setArrayValue(a).build())).build();
    }

    @Test
    void typeOrderAcrossAllTypes() {
        List<Value> ordered = List.of(
                FsValues.NULL,
                Value.newBuilder().setBooleanValue(false).build(), Value.newBuilder().setBooleanValue(true).build(),
                d(Double.NaN), d(-1e300), i(-5), d(-0.5), i(0), d(1.5), i(2), d(1e300),
                Value.newBuilder().setTimestampValue(Timestamp.newBuilder().setSeconds(10)).build(),
                Value.newBuilder().setTimestampValue(Timestamp.newBuilder().setSeconds(10).setNanos(1)).build(),
                s(""), s("a"), s("b"), s("é"), s("😀"),
                Value.newBuilder().setBytesValue(ByteString.copyFrom(new byte[] {1})).build(),
                Value.newBuilder().setBytesValue(ByteString.copyFrom(new byte[] {1, 2})).build(),
                Value.newBuilder().setReferenceValue("projects/p/databases/d/documents/a/b").build(),
                Value.newBuilder().setReferenceValue("projects/p/databases/d/documents/a/b/c/d").build(),
                Value.newBuilder().setReferenceValue("projects/p/databases/d/documents/a/c").build(),
                Value.newBuilder().setGeoPointValue(LatLng.newBuilder().setLatitude(1).setLongitude(2)).build(),
                Value.newBuilder().setGeoPointValue(LatLng.newBuilder().setLatitude(1).setLongitude(3)).build(),
                arr(), arr(i(1)), arr(i(1), i(2)), arr(i(2)),
                vec(9), vec(1, 2), vec(1, 3),
                map("a", i(1)), map("a", i(2)), map("b", i(0)));
        List<Value> shuffled = new ArrayList<>(ordered);
        Collections.shuffle(shuffled, new java.util.Random(7));
        shuffled.sort(FsValues::compare);
        assertEquals(ordered, shuffled);
    }

    @Test
    void integerAndDoubleCompareExactly() {
        long big = (1L << 53) + 1;
        assertTrue(FsValues.compare(i(big), d((double) (1L << 53))) > 0);
        assertEquals(0, FsValues.compare(i(2), d(2.0)));
        assertEquals(0, FsValues.compare(d(-0.0), d(0.0)));
        assertTrue(FsValues.compare(i(Long.MAX_VALUE), d(9.223372036854775807E18)) < 0);
        assertTrue(FsValues.compare(d(Double.NaN), i(Long.MIN_VALUE)) < 0);
        assertEquals(0, FsValues.compare(d(Double.NaN), d(Double.NaN)));
        assertTrue(FsValues.equal(i(3), d(3.0)));
    }

    @Test
    void stringsCompareByCodePointNotUtf16() {
        // U+FF5E (BMP) < U+1F600 (surrogate pair in UTF-16, which Java's compareTo would order the other way)
        assertTrue(FsValues.compareStrings("～", "😀") < 0);
        assertTrue("～".compareTo("😀") > 0);
        assertTrue(FsValues.compareStrings("a", "ab") < 0);
    }

    @Test
    void fieldPathParsing() {
        assertEquals(List.of("a", "b", "c"), FsValues.parseFieldPath("a.b.c"));
        assertEquals(List.of("a b", "c"), FsValues.parseFieldPath("`a b`.c"));
        assertEquals(List.of("a`b"), FsValues.parseFieldPath("`a\\`b`"));
        assertEquals("a.`b c`", FsValues.joinPath(List.of("a", "b c")));
        FsException e = assertThrows(FsException.class, () -> FsValues.parseFieldPath("a..b"));
        assertTrue(e.getMessage().startsWith("Invalid property path \"a..b\". Unquoted property paths must match regex"));
        assertEquals("Invalid empty property path string.", assertThrows(FsException.class, () -> FsValues.parseFieldPath("")).getMessage());
        assertThrows(FsException.class, () -> FsValues.parseFieldPath("`a"));
        assertThrows(FsException.class, () -> FsValues.parseFieldPath("sp ace"));
    }

    @Test
    void fieldSetAndRemoveKeepSiblings() {
        Map<String, Value> f = Map.of("a", map("b", i(1)), "z", i(9));
        Map<String, Value> set = FsValues.set(f, List.of("a", "c", "d"), 0, i(2));
        assertEquals(i(2), FsValues.get(set, List.of("a", "c", "d")));
        assertEquals(i(1), FsValues.get(set, List.of("a", "b")));
        Map<String, Value> gone = FsValues.remove(set, List.of("a", "b"), 0);
        assertEquals(null, FsValues.get(gone, List.of("a", "b")));
        assertEquals(i(9), gone.get("z"));
        assertEquals(f, FsValues.remove(f, List.of("nope", "x"), 0));
    }

    @Test
    void validationRules() {
        assertThrows(FsException.class, () -> FsValues.validateFields(Map.of("__x__", i(1))));
        assertThrows(FsException.class, () -> FsValues.validateFields(Map.of("", i(1))));
        FsException nested = assertThrows(FsException.class, () -> FsValues.validateFields(Map.of("a", arr(arr(i(1))))));
        assertEquals("Nested arrays are not allowed", nested.getMessage());
        Value deep = i(1);
        for (int k = 0; k < 20; k++) {
            deep = map("x", deep);
        }
        FsValues.validateFields(Map.of("a", deep)); // 20 levels of maps is allowed
        Value tooDeep = map("x", deep);
        assertEquals("Property a contains an invalid nested entity.", assertThrows(FsException.class, () -> FsValues.validateFields(Map.of("a", tooDeep))).getMessage());
        assertThrows(FsException.class, () -> FsValues.validateFields(Map.of("g", Value.newBuilder().setGeoPointValue(LatLng.newBuilder().setLatitude(91)).build())));
        String big = "x".repeat(FsValues.MAX_VALUE_BYTES + 1);
        assertThrows(FsException.class, () -> FsValues.validateFields(Map.of("s", s(big))));
    }

    @Test
    void timestampsAreTruncatedToMicroseconds() {
        Value t = Value.newBuilder().setTimestampValue(Timestamp.newBuilder().setSeconds(5).setNanos(123456789)).build();
        Map<String, Value> out = FsValues.truncateTimestamps(Map.of("t", t, "n", map("in", arr(t))));
        assertEquals(123456000, out.get("t").getTimestampValue().getNanos());
        assertEquals(123456000, out.get("n").getMapValue().getFieldsMap().get("in").getArrayValue().getValues(0).getTimestampValue().getNanos());
        Map<String, Value> same = Map.of("a", i(1));
        assertTrue(FsValues.truncateTimestamps(same) == same);
    }

    @Test
    void documentSizeFollowsFirestoreRules() {
        // name: 16 + sum(segment bytes + 1); fields: name bytes + 1 + value; +32
        long size = FsValues.docSize("c/d", Map.of("f", i(1)));
        assertEquals((16 + 2 + 2) + 32 + (1 + 1 + 8), size);
        assertFalse(FsValues.isVector(map("__type__", s("__vector__"))));
        assertTrue(FsValues.isVector(vec(1, 2)));
    }
}
