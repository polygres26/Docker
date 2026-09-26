package com.sayonora.warp.firestorewire;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.DocumentTransform;
import com.google.firestore.v1.Precondition;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import com.google.firestore.v1.WriteResult;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Applies {@link Write}s to an in-memory overlay of documents: preconditions, update masks and field transforms. */
final class FsWrites {

    private FsWrites() {
    }

    static final long MAX_DOC_BYTES = 1_048_576;

    /** The state of the documents touched by one commit: starts as the locked current state, evolves write by write. */
    static final class Overlay {
        final FsNames.Db db;
        final long commitUs;
        final Map<String, FsStore.Doc> initial;
        final Map<String, FsStore.Doc> state = new LinkedHashMap<>();
        final Set<String> touched = new LinkedHashSet<>();

        Overlay(FsNames.Db db, long commitUs, Map<String, FsStore.Doc> current) {
            this.db = db;
            this.commitUs = commitUs;
            this.initial = current;
            this.state.putAll(current);
        }

        List<FsStore.Change> changes() {
            List<FsStore.Change> out = new ArrayList<>();
            for (String rel : touched) {
                FsStore.Doc a = initial.get(rel);
                FsStore.Doc b = state.get(rel);
                if (a == b) {
                    continue;
                }
                out.add(new FsStore.Change(rel, b));
            }
            return out;
        }
    }

    /** The relative document path a write targets (validated), or null for a write with no operation. */
    static String target(FsNames.Db db, Write w) {
        String name;
        switch (w.getOperationCase()) {
            case UPDATE:
                name = w.getUpdate().getName();
                break;
            case DELETE:
                name = w.getDelete();
                break;
            case TRANSFORM:
                name = w.getTransform().getDocument();
                break;
            default:
                throw FsException.invalid("empty write operation");
        }
        FsNames.Loc l = FsNames.parseDoc(name);
        if (!l.db().equals(db)) {
            throw FsException.invalid("Document name \"" + name + "\" does not lie within database \"" + db.name() + "\"");
        }
        return l.rel();
    }

    private static void checkPrecondition(Precondition p, FsStore.Doc cur, String name, boolean isDelete) {
        switch (p.getConditionTypeCase()) {
            case EXISTS:
                if (p.getExists() && cur == null) {
                    throw FsException.notFound("No document to update: " + name);
                }
                if (!p.getExists() && cur != null) {
                    throw FsException.exists("Document already exists: " + name);
                }
                break;
            case UPDATE_TIME:
                if (cur == null) {
                    throw FsException.notFound("No document to update: " + name);
                }
                if (FsClock.micros(p.getUpdateTime()) != cur.updateUs) {
                    throw FsException.precondition("the stored version (" + cur.updateUs
                            + ") does not match the required base version (" + FsClock.micros(p.getUpdateTime()) + ")");
                }
                break;
            default:
                break;
        }
    }

    /** Applies one write; returns its WriteResult. Throws on a failed precondition or invalid input. */
    static WriteResult apply(Overlay o, Write w) {
        String rel = target(o.db, w);
        String name = o.db.docsRoot() + "/" + rel;
        FsStore.Doc cur = o.state.get(rel);
        if (w.hasCurrentDocument()) {
            checkPrecondition(w.getCurrentDocument(), cur, name, w.getOperationCase() == Write.OperationCase.DELETE);
        }
        o.touched.add(rel);
        switch (w.getOperationCase()) {
            case DELETE:
                o.state.remove(rel);
                return WriteResult.getDefaultInstance(); // a delete has no update_time
            case UPDATE:
                return applyUpdate(o, rel, cur, w.getUpdate().getFieldsMap(), w.hasUpdateMask() ? w.getUpdateMask().getFieldPathsList() : null,
                        w.getUpdateTransformsList());
            case TRANSFORM:
                return applyUpdate(o, rel, cur, null, List.of(), w.getTransform().getFieldTransformsList());
            default:
                throw FsException.invalid("Write must have an operation");
        }
    }

    private static WriteResult applyUpdate(Overlay o, String rel, FsStore.Doc cur, Map<String, Value> body, List<String> mask,
            List<DocumentTransform.FieldTransform> transforms) {
        Map<String, Value> fields;
        if (body != null) {
            FsValues.validateFields(body);
        }
        if (body == null) {
            fields = cur == null ? Map.of() : cur.fields;
        } else if (mask == null) {
            fields = body;
        } else {
            fields = cur == null ? Map.of() : cur.fields;
            List<List<String>> paths = new ArrayList<>();
            for (String mp : mask) {
                List<String> p = FsValues.parseFieldPath(mp);
                paths.add(p);
            }
            for (List<String> p : paths) {
                Value v = FsValues.get(body, p);
                fields = v != null ? FsValues.set(fields, p, 0, v) : FsValues.remove(fields, p, 0);
            }
        }
        List<Value> tr = new ArrayList<>();
        validateTransforms(transforms);
        for (DocumentTransform.FieldTransform ft : transforms) {
            List<String> p = FsValues.parseFieldPath(ft.getFieldPath());
            Value existing = FsValues.get(fields, p);
            Value[] res = transform(ft, existing, o.commitUs);
            fields = FsValues.set(fields, p, 0, res[0]);
            tr.add(res[1]);
        }
        fields = FsValues.truncateTimestamps(fields);
        long size = FsValues.docSize(rel, fields);
        if (size > MAX_DOC_BYTES) {
            throw FsException.invalid("Document \"" + o.db.docsRoot() + "/" + rel + "\" cannot be written because its size (" + size
                    + " bytes) exceeds the maximum allowed size of 1,048,576 bytes.");
        }
        WriteResult.Builder wr = WriteResult.newBuilder().addAllTransformResults(tr);
        if (cur != null && cur.fields.equals(fields)) {
            wr.setUpdateTime(FsClock.ts(cur.updateUs));
        } else {
            FsStore.Doc nd = new FsStore.Doc(rel, fields, cur == null ? o.commitUs : cur.createUs, o.commitUs);
            o.state.put(rel, nd);
            wr.setUpdateTime(FsClock.ts(o.commitUs));
        }
        return wr.build();
    }

    // ------------------------------------------------------------------ field transforms

    /** Checks every transform of one write before any is applied (paths, operation set, no property and its nested property). */
    static void validateTransforms(List<DocumentTransform.FieldTransform> transforms) {
        List<List<String>> paths = new ArrayList<>();
        for (DocumentTransform.FieldTransform ft : transforms) {
            paths.add(FsValues.parseFieldPath(ft.getFieldPath()));
            switch (ft.getTransformTypeCase()) {
                case TRANSFORMTYPE_NOT_SET:
                    throw FsException.invalid("Operation type must be specified for field transformation.");
                case SET_TO_SERVER_VALUE:
                    if (ft.getSetToServerValue() != DocumentTransform.FieldTransform.ServerValue.REQUEST_TIME) {
                        throw FsException.invalid("ServerValue type must be specified for set_to_server_value transformation.");
                    }
                    break;
                case INCREMENT:
                    if (!isNum(ft.getIncrement())) {
                        throw FsException.invalid("Input must be int64 or double.");
                    }
                    break;
                case MAXIMUM:
                    if (!isNum(ft.getMaximum())) {
                        throw FsException.invalid("Input must be int64 or double.");
                    }
                    break;
                case MINIMUM:
                    if (!isNum(ft.getMinimum())) {
                        throw FsException.invalid("Input must be int64 or double.");
                    }
                    break;
                default:
                    break;
            }
        }
        for (List<String> a : paths) {
            for (List<String> b : paths) {
                if (a.size() < b.size() && b.subList(0, a.size()).equals(a)) {
                    throw FsException.invalid("Cannot transform property " + FsValues.joinPath(a) + " and its nested property at the same time.");
                }
            }
        }
    }

    /** Returns {newValue, transformResult}. */
    static Value[] transform(DocumentTransform.FieldTransform ft, Value cur, long commitUs) {
        switch (ft.getTransformTypeCase()) {
            case SET_TO_SERVER_VALUE: {
                Value ts = FsValues.ofTs(commitUs);
                return new Value[] {ts, ts};
            }
            case INCREMENT: {
                Value v = increment(cur, ft.getIncrement());
                return new Value[] {v, v};
            }
            case MAXIMUM: {
                Value v = maxMin(cur, ft.getMaximum(), true);
                return new Value[] {v, v};
            }
            case MINIMUM: {
                Value v = maxMin(cur, ft.getMinimum(), false);
                return new Value[] {v, v};
            }
            case APPEND_MISSING_ELEMENTS: {
                List<Value> list = new ArrayList<>(cur != null && cur.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE
                        ? cur.getArrayValue().getValuesList() : List.of());
                for (Value e : ft.getAppendMissingElements().getValuesList()) {
                    boolean present = false;
                    for (Value x : list) {
                        if (FsValues.typeOrder(x) == FsValues.typeOrder(e) && FsValues.equal(x, e)) {
                            present = true;
                            break;
                        }
                    }
                    if (!present) {
                        list.add(e);
                    }
                }
                return new Value[] {Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(list)).build(), FsValues.NULL};
            }
            case REMOVE_ALL_FROM_ARRAY: {
                List<Value> list = new ArrayList<>();
                if (cur != null && cur.getValueTypeCase() == Value.ValueTypeCase.ARRAY_VALUE) {
                    for (Value x : cur.getArrayValue().getValuesList()) {
                        boolean remove = false;
                        for (Value e : ft.getRemoveAllFromArray().getValuesList()) {
                            if (FsValues.typeOrder(x) == FsValues.typeOrder(e) && FsValues.equal(x, e)) {
                                remove = true;
                                break;
                            }
                        }
                        if (!remove) {
                            list.add(x);
                        }
                    }
                }
                return new Value[] {Value.newBuilder().setArrayValue(ArrayValue.newBuilder().addAllValues(list)).build(), FsValues.NULL};
            }
            default:
                throw FsException.invalid("FieldTransform must have a transform type");
        }
    }

    private static boolean isNum(Value v) {
        return v != null && (v.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE || v.getValueTypeCase() == Value.ValueTypeCase.DOUBLE_VALUE);
    }

    private static double dbl(Value v) {
        return v.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE ? (double) v.getIntegerValue() : v.getDoubleValue();
    }

    static Value increment(Value cur, Value op) {
        if (!isNum(op)) {
            throw FsException.invalid("Increment operand must be an integer or a double");
        }
        if (!isNum(cur)) {
            return op;
        }
        if (cur.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE && op.getValueTypeCase() == Value.ValueTypeCase.INTEGER_VALUE) {
            long a = cur.getIntegerValue();
            long b = op.getIntegerValue();
            long r = a + b;
            if (((a ^ r) & (b ^ r)) < 0) {
                r = a < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
            }
            return FsValues.ofLong(r);
        }
        return FsValues.ofDouble(dbl(cur) + dbl(op));
    }

    static Value maxMin(Value cur, Value op, boolean max) {
        if (!isNum(op)) {
            throw FsException.invalid((max ? "Maximum" : "Minimum") + " operand must be an integer or a double");
        }
        if (!isNum(cur)) {
            return op;
        }
        if (FsValues.isNaN(cur)) {
            return cur;
        }
        if (FsValues.isNaN(op)) {
            return op;
        }
        int c = FsValues.compareNumbers(op, cur);
        if (c == 0) {
            return cur;
        }
        return (max ? c > 0 : c < 0) ? op : cur;
    }

    static Document masked(Document d, List<String> maskPaths) {
        if (maskPaths == null || maskPaths.isEmpty()) {
            return d;
        }
        Map<String, Value> out = new LinkedHashMap<>();
        for (String mp : maskPaths) {
            List<String> p = FsValues.parseFieldPath(mp);
            Value v = FsValues.get(d.getFieldsMap(), p);
            if (v != null) {
                out = FsValues.set(out, p, 0, v);
            }
        }
        return d.toBuilder().clearFields().putAllFields(out).build();
    }
}
