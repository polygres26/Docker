package com.sayonora.warp.datastorewire;

import com.google.datastore.v1.Key;
import com.google.datastore.v1.PartitionId;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Datastore keys: validation with Cloud Datastore's error texts, a canonical partition, an order-preserving byte encoding
 * (bytes order == Datastore key order: element by element, kind then id-before-name) and ancestor arithmetic.
 */
final class DsKeys {

    private DsKeys() {
    }

    /** A request's partition: project, database and namespace. */
    record Part(String project, String database, String namespace) {
        /** The storage scope string. */
        String scope() {
            return project + "/" + database + "/" + namespace;
        }

        PartitionId toProto() {
            PartitionId.Builder b = PartitionId.newBuilder().setProjectId(project);
            if (!database.isEmpty()) {
                b.setDatabaseId(database);
            }
            if (!namespace.isEmpty()) {
                b.setNamespaceId(namespace);
            }
            return b.build();
        }
    }

    private static final Pattern RESERVED = Pattern.compile("^__.*__$", Pattern.DOTALL);

    static Part part(String project, String database, PartitionId pid) {
        String p = project == null ? "" : project;
        String ns = "";
        if (pid != null && !pid.equals(PartitionId.getDefaultInstance())) {
            if (!pid.getProjectId().isEmpty() && !pid.getProjectId().equals(p) && !p.isEmpty()) {
                throw DsException.invalid("mismatched databases within request: <unknown!>~" + p + " vs. <unknown!>~" + pid.getProjectId());
            }
            if (p.isEmpty()) {
                p = pid.getProjectId();
            }
            ns = pid.getNamespaceId();
            if (!pid.getDatabaseId().isEmpty()) {
                database = pid.getDatabaseId();
            }
        }
        if (p.isEmpty()) {
            throw DsException.invalid("Missing project id.");
        }
        checkNamespace(ns);
        return new Part(p, database == null ? "" : database, ns);
    }

    static void checkNamespace(String ns) {
        if (!ns.isEmpty() && RESERVED.matcher(ns).matches()) {
            throw DsException.invalid("The namespace id \"" + ns + "\" is reserved.");
        }
    }

    /** Partition of a key: its own partition_id merged over the request's (the project must agree, the namespace is the key's). */
    static Part partOf(Part req, Key k) {
        if (!k.hasPartitionId() || k.getPartitionId().equals(PartitionId.getDefaultInstance())) {
            return req;
        }
        PartitionId pid = k.getPartitionId();
        if (!pid.getProjectId().isEmpty() && !pid.getProjectId().equals(req.project())) {
            throw DsException.invalid("mismatched databases within request: <unknown!>~" + req.project() + " vs. <unknown!>~" + pid.getProjectId());
        }
        checkNamespace(pid.getNamespaceId());
        return new Part(req.project(), pid.getDatabaseId().isEmpty() ? req.database() : pid.getDatabaseId(), pid.getNamespaceId());
    }

    static boolean complete(Key k) {
        if (k.getPathCount() == 0) {
            return false;
        }
        Key.PathElement last = k.getPath(k.getPathCount() - 1);
        return last.getIdTypeCase() == Key.PathElement.IdTypeCase.NAME || last.getIdTypeCase() == Key.PathElement.IdTypeCase.ID && last.getId() != 0;
    }

    private static boolean elemComplete(Key.PathElement e) {
        return e.getIdTypeCase() == Key.PathElement.IdTypeCase.NAME || e.getIdTypeCase() == Key.PathElement.IdTypeCase.ID && e.getId() != 0;
    }

    static String describe(Key k) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < k.getPathCount(); i++) {
            Key.PathElement e = k.getPath(i);
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(e.getKind()).append(": ");
            if (e.getIdTypeCase() == Key.PathElement.IdTypeCase.NAME) {
                sb.append(e.getName());
            } else if (e.getId() != 0) {
                sb.append(e.getId());
            }
        }
        return sb.append("]").toString();
    }

    /**
     * Validates a key. {@code allowIncompleteLast}: the last element may lack an id/name (insert/upsert allocate one). Metadata kinds
     * ({@code __kind__}, {@code __namespace__}, {@code __property__}) are allowed when {@code metadata} is set (query filters/results).
     */
    static void validate(Key k, boolean allowIncompleteLast, boolean metadata) {
        if (k.getPathCount() == 0) {
            throw DsException.invalid("Key path is empty.");
        }
        if (k.getPathCount() > 100) {
            throw DsException.invalid("The key path is too long: at most 100 elements are allowed.");
        }
        for (int i = 0; i < k.getPathCount(); i++) {
            Key.PathElement e = k.getPath(i);
            if (e.getKind().isEmpty()) {
                throw DsException.invalid("The kind is the empty string.");
            }
            if (e.getKind().getBytes(StandardCharsets.UTF_8).length > 1500) {
                throw DsException.invalid("The kind is longer than 1500 bytes.");
            }
            if (!metadata && RESERVED.matcher(e.getKind()).matches()) {
                throw DsException.invalid("The kind \"" + e.getKind() + "\" is reserved.");
            }
            if (e.getIdTypeCase() == Key.PathElement.IdTypeCase.NAME) {
                if (e.getName().isEmpty()) {
                    throw DsException.invalid("The key path element name is the empty string.");
                }
                if (e.getName().getBytes(StandardCharsets.UTF_8).length > 1500) {
                    throw DsException.invalid("The key path element name is longer than 1500 bytes.");
                }
            }
            boolean last = i == k.getPathCount() - 1;
            if (!elemComplete(e) && !(last && allowIncompleteLast)) {
                throw DsException.invalid("Key path element must not be incomplete: " + describe(k));
            }
        }
    }

    // ------------------------------------------------------------------ encoding and order

    /** Order-preserving encoding: per element {@code kind 0x00 (0x01 + 8-byte biased id | 0x02 name 0x00)}. */
    static byte[] encode(Key k) {
        return encode(k, k.getPathCount());
    }

    static byte[] encode(Key k, int elements) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < elements; i++) {
            Key.PathElement e = k.getPath(i);
            out.writeBytes(e.getKind().getBytes(StandardCharsets.UTF_8));
            out.write(0);
            if (e.getIdTypeCase() == Key.PathElement.IdTypeCase.NAME) {
                out.write(2);
                out.writeBytes(e.getName().getBytes(StandardCharsets.UTF_8));
                out.write(0);
            } else {
                out.write(1);
                long biased = e.getId() ^ Long.MIN_VALUE;
                for (int s = 56; s >= 0; s -= 8) {
                    out.write((int) (biased >>> s) & 0xFF);
                }
            }
        }
        return out.toByteArray();
    }

    static byte[] rootKey(Key k) {
        return encode(k, 1);
    }

    /** Datastore key order without encoding (used for values and metadata). */
    static int compare(Key a, Key b) {
        int n = Math.min(a.getPathCount(), b.getPathCount());
        for (int i = 0; i < n; i++) {
            Key.PathElement x = a.getPath(i);
            Key.PathElement y = b.getPath(i);
            int c = DsValues.compareStrings(x.getKind(), y.getKind());
            if (c != 0) {
                return c;
            }
            boolean xn = x.getIdTypeCase() == Key.PathElement.IdTypeCase.NAME;
            boolean yn = y.getIdTypeCase() == Key.PathElement.IdTypeCase.NAME;
            if (xn != yn) {
                return xn ? 1 : -1;
            }
            c = xn ? DsValues.compareStrings(x.getName(), y.getName()) : Long.compare(x.getId(), y.getId());
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(a.getPathCount(), b.getPathCount());
    }

    /** True when {@code anc} equals {@code k} or is one of its ancestors. */
    static boolean hasAncestor(Key k, Key anc) {
        if (anc.getPathCount() > k.getPathCount()) {
            return false;
        }
        for (int i = 0; i < anc.getPathCount(); i++) {
            Key.PathElement x = k.getPath(i);
            Key.PathElement y = anc.getPath(i);
            if (!x.getKind().equals(y.getKind()) || x.getIdTypeCase() != y.getIdTypeCase()
                    || (x.getIdTypeCase() == Key.PathElement.IdTypeCase.NAME ? !x.getName().equals(y.getName()) : x.getId() != y.getId())) {
                return false;
            }
        }
        return true;
    }

    /** The key with its partition filled from {@code part} (what results carry). */
    static Key withPartition(Key k, Part part) {
        return k.toBuilder().setPartitionId(part.toProto()).build();
    }

    static List<Key.PathElement> path(Key k) {
        return new ArrayList<>(k.getPathList());
    }
}
