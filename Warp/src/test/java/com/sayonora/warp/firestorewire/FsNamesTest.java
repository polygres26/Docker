package com.sayonora.warp.firestorewire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class FsNamesTest {

    static final String ROOT = "projects/wt/databases/(default)/documents";

    private static String err(String name) {
        return assertThrows(FsException.class, () -> FsNames.parseDoc(name)).getMessage();
    }

    @Test
    void documentNamesParseAndErrorLikeGoogleParser() {
        FsNames.Loc l = FsNames.parseDoc(ROOT + "/users/alice/posts/p1");
        assertEquals("users/alice/posts/p1", l.rel());
        assertEquals("projects/wt/databases/(default)", l.db().name());
        assertEquals("users/alice/posts", FsNames.collPath(l.rel()));
        assertEquals("posts", FsNames.collId(l.rel()));
        assertEquals("Document name \"nonsense\" lacks \"projects\" at index 0.", err("nonsense"));
        assertEquals("Document name \"projects/x/databases/(default)/docs/users/alice\" lacks \"documents\" at index 31.",
                err("projects/x/databases/(default)/docs/users/alice"));
        assertEquals("Document name \"" + ROOT + "/users\" lacks \"/\" at index " + (ROOT.length() + 6) + ".", err(ROOT + "/users"));
        assertEquals("Document name \"" + ROOT + "/m//x\" lacks a resource id at index " + (ROOT.length() + 3) + ".", err(ROOT + "/m//x"));
        assertEquals("Document name \"" + ROOT + "/users/..\" contains a resource id \"..\" at index " + (ROOT.length() + 7) + ".", err(ROOT + "/users/.."));
        assertEquals("Resource id \"__x__\" is invalid because it is reserved.", err(ROOT + "/users/__x__"));
        assertEquals("Document parent name \"projects/x\" lacks \"/\" at index 10.",
                assertThrows(FsException.class, () -> FsNames.parseParent("projects/x")).getMessage());
        assertEquals("", FsNames.parseParent(ROOT).rel());
    }

    @Test
    void idValidation() {
        assertEquals("collectionId is the empty string.", assertThrows(FsException.class, () -> FsNames.validateId("", true)).getMessage());
        assertEquals("Collection id \"a/b\" is invalid because it contains \"/\".", assertThrows(FsException.class, () -> FsNames.validateId("a/b", true)).getMessage());
        assertEquals("The key path element name is longer than 1500 bytes.", assertThrows(FsException.class, () -> FsNames.validateId("x".repeat(1501), false)).getMessage());
    }

    @Test
    void nameKeyOrderIsSegmentWise() {
        // 'a/b' sorts before 'a-c/x' segment-wise ('a' < 'a-c') although '-' < '/' would say the opposite for plain strings
        List<String> paths = new ArrayList<>(List.of("a/b", "a-c/x", "a/b/c/d", "a/b/c/e", "b/a", "a/aa"));
        List<String> expected = List.of("a/aa", "a/b", "a/b/c/d", "a/b/c/e", "a-c/x", "b/a");
        Collections.shuffle(paths, new java.util.Random(3));
        paths.sort(FsNames::compare);
        assertEquals(expected, paths);
        assertArrayEquals("a\0b".getBytes(java.nio.charset.StandardCharsets.UTF_8), FsNames.nameKey("a/b"));
        assertTrue(FsNames.compare("a/b", "a/b/c/d") < 0);
    }
}
