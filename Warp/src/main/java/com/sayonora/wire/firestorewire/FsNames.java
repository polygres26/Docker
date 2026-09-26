package com.sayonora.wire.firestorewire;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/** Resource-name parsing and document-path helpers. */
final class FsNames {

    private FsNames() {
    }

    record Db(String project, String database) {
        String name() {
            return "projects/" + project + "/databases/" + database;
        }

        String docsRoot() {
            return name() + "/documents";
        }
    }

    /** A parsed document or collection location under a database: {@code rel} is the path below /documents ("" = root). */
    record Loc(Db db, String rel) {
        List<String> segs() {
            return rel.isEmpty() ? List.of() : List.of(rel.split("/", -1));
        }

        String fullName() {
            return rel.isEmpty() ? db.docsRoot() : db.docsRoot() + "/" + rel;
        }
    }

    private static final Pattern RESERVED = Pattern.compile("^__.*__$", Pattern.DOTALL);

    /**
     * Parses {@code projects/p/databases/d/documents[/rel]} with the exact error texts Google's resource-name parser produces
     * ({@code Document name "..." lacks "documents" at index 31.}). {@code label} is "Document name" or "Document parent name";
     * the path below /documents must have an even number of segments (a document, or with {@code allowRoot} the database root).
     */
    static Loc parseName(String name, String label, boolean allowRoot) {
        String n = name == null ? "" : name;
        String[] parts = n.split("/", -1);
        int[] start = new int[parts.length];
        int pos = 0;
        for (int i = 0; i < parts.length; i++) {
            start[i] = pos;
            pos += parts[i].length() + 1;
        }
        String[] lit = {"projects", null, "databases", null, "documents"};
        for (int i = 0; i < 5; i++) {
            if (i >= parts.length) {
                throw FsException.invalid(label + " \"" + n + "\" lacks \"/\" at index " + n.length() + ".");
            }
            if (lit[i] != null) {
                if (!parts[i].equals(lit[i])) {
                    throw FsException.invalid(label + " \"" + n + "\" lacks \"" + lit[i] + "\" at index " + start[i] + ".");
                }
            } else if (parts[i].isEmpty()) {
                throw FsException.invalid(label + " \"" + n + "\" lacks a resource id at index " + start[i] + ".");
            }
        }
        List<String> rest = new ArrayList<>();
        for (int i = 5; i < parts.length; i++) {
            String seg = parts[i];
            if (seg.isEmpty()) {
                throw FsException.invalid(label + " \"" + n + "\" lacks a resource id at index " + start[i] + ".");
            }
            if (seg.equals(".") || seg.equals("..")) {
                throw FsException.invalid(label + " \"" + n + "\" contains a resource id \"" + seg + "\" at index " + start[i] + ".");
            }
            if (RESERVED.matcher(seg).matches()) {
                throw FsException.invalid("Resource id \"" + seg + "\" is invalid because it is reserved.");
            }
            if (seg.getBytes(StandardCharsets.UTF_8).length > 1500) {
                throw FsException.invalid(label + " \"" + n + "\" contains a resource id longer than 1500 bytes at index " + start[i] + ".");
            }
            rest.add(seg);
        }
        if ((rest.size() & 1) == 1 || (rest.isEmpty() && !allowRoot)) {
            throw FsException.invalid(label + " \"" + n + "\" lacks \"/\" at index " + n.length() + ".");
        }
        return new Loc(new Db(parts[1], parts[3]), String.join("/", rest));
    }

    /** A document name (a non-empty, even number of segments below /documents). */
    static Loc parseDoc(String name) {
        return parseName(name, "Document name", false);
    }

    /** A parent: the database root or a document. */
    static Loc parseParent(String name) {
        return parseName(name, "Document parent name", true);
    }

    static Db parseDb(String database) {
        var m = Pattern.compile("^projects/([^/]+)/databases/([^/]+)$").matcher(database == null ? "" : database);
        if (!m.matches()) {
            throw FsException.invalid("Invalid database name: " + database);
        }
        return new Db(m.group(1), m.group(2));
    }

    /** Validates a client-chosen collection id or document id (CreateDocument). */
    static void validateId(String s, boolean collection) {
        if (collection && s.isEmpty()) {
            throw FsException.invalid("collectionId is the empty string.");
        }
        if (s.indexOf('/') >= 0) {
            throw FsException.invalid((collection ? "Collection id" : "Resource id") + " \"" + s + "\" is invalid because it contains \"/\".");
        }
        if (s.equals(".") || s.equals("..")) {
            throw FsException.invalid("Resource id \"" + s + "\" is invalid because it is " + (s.equals(".") ? "." : "..") + ".");
        }
        if (RESERVED.matcher(s).matches()) {
            throw FsException.invalid("Resource id \"" + s + "\" is invalid because it is reserved.");
        }
        if (s.getBytes(StandardCharsets.UTF_8).length > 1500) {
            throw FsException.invalid("The key path element name is longer than 1500 bytes.");
        }
    }

    /** Order-preserving key of a document path: UTF-8 segments joined by 0x00. */
    static byte[] nameKey(String rel) {
        return rel.replace('/', '\u0000').getBytes(StandardCharsets.UTF_8);
    }

    static String collPath(String rel) {
        int i = rel.lastIndexOf('/');
        return i < 0 ? "" : rel.substring(0, i);
    }

    static String collId(String rel) {
        String cp = collPath(rel);
        int i = cp.lastIndexOf('/');
        return i < 0 ? cp : cp.substring(i + 1);
    }

    static String docId(String rel) {
        return rel.substring(rel.lastIndexOf('/') + 1);
    }

    static int depth(String rel) {
        return rel.isEmpty() ? 0 : (int) rel.chars().filter(c -> c == '/').count() + 1;
    }

    static List<String> segments(String rel) {
        return rel.isEmpty() ? new ArrayList<>() : new ArrayList<>(List.of(rel.split("/", -1)));
    }

    /** Segment-wise comparison of two relative paths (== byte order of nameKey). */
    static int compare(String a, String b) {
        return java.util.Arrays.compareUnsigned(nameKey(a), nameKey(b));
    }
}
