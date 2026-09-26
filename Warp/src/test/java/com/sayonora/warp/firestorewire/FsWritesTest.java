package com.sayonora.warp.firestorewire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.DocumentMask;
import com.google.firestore.v1.DocumentTransform;
import com.google.firestore.v1.Precondition;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import com.google.firestore.v1.WriteResult;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FsWritesTest {

    static final FsNames.Db DB = new FsNames.Db("p", "(default)");

    static Value i(long n) {
        return FsValuesTest.i(n);
    }

    static Value d(double n) {
        return FsValuesTest.d(n);
    }

    static Value arrOf(Value... vs) {
        return Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(List.of(vs))).build();
    }

    static DocumentTransform.FieldTransform inc(String f, Value v) {
        return DocumentTransform.FieldTransform.newBuilder().setFieldPath(f).setIncrement(v).build();
    }

    @Test
    void incrementClampsIntegersAndPromotesToDouble() {
        assertEquals(i(8), FsWrites.increment(i(5), i(3)));
        assertEquals(i(Long.MAX_VALUE), FsWrites.increment(i(Long.MAX_VALUE), i(5)));
        assertEquals(i(Long.MIN_VALUE), FsWrites.increment(i(Long.MIN_VALUE), i(-5)));
        assertEquals(d(8.5), FsWrites.increment(i(8), d(0.5)));
        assertEquals(i(7), FsWrites.increment(FsValues.ofString("s"), i(7)));
        assertEquals(i(7), FsWrites.increment(null, i(7)));
        assertThrows(FsException.class, () -> FsWrites.increment(i(1), FsValues.ofString("x")));
    }

    @Test
    void maximumMinimumFollowFirestoreRules() {
        assertEquals(i(100), FsWrites.maxMin(d(8.5), i(100), true)); // takes the larger operand's type
        assertEquals(i(100), FsWrites.maxMin(i(100), d(100.0), true)); // equal: the stored value stays
        assertEquals(d(50.5), FsWrites.maxMin(i(100), d(50.5), false));
        assertEquals(i(4), FsWrites.maxMin(null, i(4), true));
        assertEquals(i(9), FsWrites.maxMin(FsValues.ofString("s"), i(9), true));
        assertTrue(Double.isNaN(FsWrites.maxMin(i(3), d(Double.NaN), true).getDoubleValue()));
        assertTrue(Double.isNaN(FsWrites.maxMin(d(Double.NaN), i(3), false).getDoubleValue()));
    }

    @Test
    void arrayTransformsUseFirestoreEquality() {
        DocumentTransform.FieldTransform add = DocumentTransform.FieldTransform.newBuilder().setFieldPath("a")
                .setAppendMissingElements(ArrayValue.newBuilder().addValues(i(3)).addValues(i(4)).addValues(d(4.0)).addValues(i(5))).build();
        Value[] r = FsWrites.transform(add, arrOf(i(1), i(2), d(3.0)), 1);
        assertEquals(arrOf(i(1), i(2), d(3.0), i(4), i(5)), r[0]);
        assertEquals(FsValues.NULL, r[1]);
        DocumentTransform.FieldTransform rm = DocumentTransform.FieldTransform.newBuilder().setFieldPath("a")
                .setRemoveAllFromArray(ArrayValue.newBuilder().addValues(i(1)).addValues(d(4.0))).build();
        assertEquals(arrOf(i(2), i(5)), FsWrites.transform(rm, arrOf(i(1), i(2), i(4), i(5), d(1.0)), 1)[0]);
        assertEquals(arrOf(), FsWrites.transform(rm, i(3), 1)[0]); // not an array: becomes the empty array
        assertEquals(arrOf(i(9)), FsWrites.transform(DocumentTransform.FieldTransform.newBuilder().setFieldPath("a")
                .setAppendMissingElements(ArrayValue.newBuilder().addValues(i(9))).build(), null, 1)[0]);
    }

    @Test
    void serverTimestampIsTheCommitTimeInMicroseconds() {
        DocumentTransform.FieldTransform t = DocumentTransform.FieldTransform.newBuilder().setFieldPath("t")
                .setSetToServerValue(DocumentTransform.FieldTransform.ServerValue.REQUEST_TIME).build();
        Value v = FsWrites.transform(t, null, 1_234_567L)[0];
        assertEquals(Timestamp.newBuilder().setSeconds(1).setNanos(234_567_000).build(), v.getTimestampValue());
    }

    @Test
    void transformValidation() {
        assertEquals("Cannot transform property n and its nested property at the same time.",
                assertThrows(FsException.class, () -> FsWrites.validateTransforms(List.of(inc("n", i(1)), inc("n.sub", i(1))))).getMessage());
        assertEquals("Operation type must be specified for field transformation.",
                assertThrows(FsException.class, () -> FsWrites.validateTransforms(List.of(DocumentTransform.FieldTransform.newBuilder().setFieldPath("n").build()))).getMessage());
        assertEquals("Input must be int64 or double.", assertThrows(FsException.class, () -> FsWrites.validateTransforms(List.of(inc("n", FsValues.ofString("x"))))).getMessage());
        FsWrites.validateTransforms(List.of(inc("n", i(1)), inc("n", i(2)))); // the same field twice applies in order
    }

    private FsWrites.Overlay overlay(long commitUs, FsStore.Doc... docs) {
        java.util.HashMap<String, FsStore.Doc> m = new java.util.HashMap<>();
        for (FsStore.Doc x : docs) {
            m.put(x.rel, x);
        }
        return new FsWrites.Overlay(DB, commitUs, m);
    }

    private static Write update(String rel, Map<String, Value> fields) {
        return Write.newBuilder().setUpdate(Document.newBuilder().setName(DB.docsRoot() + "/" + rel).putAllFields(fields)).build();
    }

    @Test
    void preconditionsAndMasksAndNoOpWrites() {
        FsWrites.Overlay o = overlay(2000);
        assertEquals(2000, FsClock.micros(FsWrites.apply(o, update("c/a", Map.of("x", i(1), "y", i(2)))).getUpdateTime()));
        // exists=false on an existing doc
        Write dup = update("c/a", Map.of()).toBuilder().setCurrentDocument(Precondition.newBuilder().setExists(false)).build();
        assertEquals("Document already exists: " + DB.docsRoot() + "/c/a", assertThrows(FsException.class, () -> FsWrites.apply(o, dup)).getMessage());
        // a masked update keeps the other fields and deletes masked fields that are absent from the body
        Write masked = update("c/a", Map.of("x", i(10))).toBuilder().setUpdateMask(DocumentMask.newBuilder().addFieldPaths("x").addFieldPaths("y")).build();
        FsWrites.apply(o, masked);
        assertEquals(Map.of("x", i(10)), o.state.get("c/a").fields);
        // an identical rewrite is a no-op: same update time, no change to store
        FsWrites.Overlay o2 = overlay(3000, new FsStore.Doc("c/a", Map.of("x", i(10)), 2000, 2000));
        WriteResult wr = FsWrites.apply(o2, update("c/a", Map.of("x", i(10))));
        assertEquals(2000, FsClock.micros(wr.getUpdateTime()));
        assertTrue(o2.changes().isEmpty());
        // update_time precondition
        Write stale = update("c/a", Map.of("x", i(1))).toBuilder()
                .setCurrentDocument(Precondition.newBuilder().setUpdateTime(FsClock.ts(1))).build();
        assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, assertThrows(FsException.class, () -> FsWrites.apply(o2, stale)).code);
        // a delete has no update_time
        assertEquals(WriteResult.getDefaultInstance(), FsWrites.apply(o2, Write.newBuilder().setDelete(DB.docsRoot() + "/c/a").build()));
        assertEquals(1, o2.changes().size());
    }
}
