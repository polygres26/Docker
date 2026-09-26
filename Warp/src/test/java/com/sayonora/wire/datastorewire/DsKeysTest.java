package com.sayonora.wire.datastorewire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.datastore.v1.Key;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class DsKeysTest {

    static Key k(Object... path) {
        Key.Builder b = Key.newBuilder();
        for (int i = 0; i < path.length; i += 2) {
            Key.PathElement.Builder e = Key.PathElement.newBuilder().setKind((String) path[i]);
            if (path[i + 1] instanceof Long l) {
                e.setId(l);
            } else if (path[i + 1] instanceof String s) {
                e.setName(s);
            }
            b.addPath(e);
        }
        return b.build();
    }

    @Test
    void encodedBytesOrderEqualsKeyOrder() {
        List<Key> ordered = List.of(k("A", 5L), k("A", 5L, "B", 1L), k("A", 6L), k("A", "a"), k("A", "a", "C", "z"), k("A", "b"), k("A", "b", "B", 1L),
                k("B", -3L), k("B", 0L), k("B", 2L), k("B", "x"), k("Root", "r1"), k("Root", "r1", "Child", 9L), k("Root", "r1", "Child", "c1"),
                k("Root", "r10"), k("Root", "r2"));
        List<Key> shuffled = new ArrayList<>(ordered);
        Collections.shuffle(shuffled, new java.util.Random(11));
        shuffled.sort((a, b) -> java.util.Arrays.compareUnsigned(DsKeys.encode(a), DsKeys.encode(b)));
        assertEquals(ordered, shuffled);
        List<Key> viaCompare = new ArrayList<>(ordered);
        Collections.shuffle(viaCompare, new java.util.Random(5));
        viaCompare.sort(DsKeys::compare);
        assertEquals(ordered, viaCompare);
    }

    @Test
    void decodeIsTheInverseOfEncodeAndRootIsTheFirstElement() {
        Key key = k("Root", "r1", "Child", 42L, "Leaf", "xé");
        assertEquals(key, DsService.decodeKey(DsKeys.encode(key)));
        assertArrayEquals(DsKeys.encode(k("Root", "r1")), DsStore.rootOf(DsKeys.encode(key)));
        assertArrayEquals(DsKeys.encode(k("A", -7L)), DsStore.rootOf(DsKeys.encode(k("A", -7L, "B", "c"))));
    }

    @Test
    void ancestorArithmetic() {
        Key key = k("Root", "r1", "Child", "c1");
        assertTrue(DsKeys.hasAncestor(key, k("Root", "r1")));
        assertTrue(DsKeys.hasAncestor(key, key));
        assertFalse(DsKeys.hasAncestor(key, k("Root", "r2")));
        assertFalse(DsKeys.hasAncestor(k("Root", "r1"), key));
        assertFalse(DsKeys.hasAncestor(k("Root", 1L), k("Root", "1")));
    }

    @Test
    void validationMessages() {
        assertEquals("Key path is empty.", assertThrows(DsException.class, () -> DsKeys.validate(Key.getDefaultInstance(), false, false)).getMessage());
        assertEquals("The kind is the empty string.", assertThrows(DsException.class, () -> DsKeys.validate(k("", "x"), false, false)).getMessage());
        assertEquals("The kind \"__bad__\" is reserved.", assertThrows(DsException.class, () -> DsKeys.validate(k("__bad__", "x"), false, false)).getMessage());
        DsKeys.validate(k("__kind__", "x"), false, true); // metadata kinds allowed for queries
        assertEquals("Key path element must not be incomplete: [User: , Post: x]",
                assertThrows(DsException.class, () -> DsKeys.validate(k("User", 0L, "Post", "x"), true, false)).getMessage());
        DsKeys.validate(k("User", "u", "Post", 0L), true, false); // the last element may be incomplete for insert/upsert
        assertEquals("Key path element must not be incomplete: [User: ]",
                assertThrows(DsException.class, () -> DsKeys.validate(k("User", 0L), false, false)).getMessage());
        assertEquals("The key path element name is longer than 1500 bytes.",
                assertThrows(DsException.class, () -> DsKeys.validate(k("K", "x".repeat(1501)), false, false)).getMessage());
        assertEquals("The namespace id \"__bad__\" is reserved.", assertThrows(DsException.class, () -> DsKeys.checkNamespace("__bad__")).getMessage());
        assertTrue(DsKeys.complete(k("A", "x")) && !DsKeys.complete(k("A", 0L)));
    }

    @Test
    void partitionsMustAgreeOnTheProject() {
        DsKeys.Part req = new DsKeys.Part("p", "", "");
        Key other = Key.newBuilder().mergeFrom(k("K", "x")).setPartitionId(com.google.datastore.v1.PartitionId.newBuilder().setProjectId("q")).build();
        assertEquals("mismatched databases within request: <unknown!>~p vs. <unknown!>~q", assertThrows(DsException.class, () -> DsKeys.partOf(req, other)).getMessage());
        Key ns = Key.newBuilder().mergeFrom(k("K", "x")).setPartitionId(com.google.datastore.v1.PartitionId.newBuilder().setProjectId("p").setNamespaceId("n1")).build();
        assertEquals("p//n1", DsKeys.partOf(req, ns).scope());
    }
}
