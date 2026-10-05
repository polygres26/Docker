package com.sayonora.warp.mywire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** The handshake challenge must survive being read as two C strings. */
class MySqlScrambleTest {

    @Test
    void everyChallengeIsTwentyPrintableBytesWithNoZero() {
        Set<String> distinct = new HashSet<>();
        for (int i = 0; i < 200_000; i++) {
            byte[] s = MySqlMessages.newScramble();
            assertEquals(20, s.length);
            for (byte b : s) {
                assertTrue(b >= 33 && b <= 126, "byte out of the printable range: " + b);
            }
            distinct.add(new String(s, java.nio.charset.StandardCharsets.ISO_8859_1));
        }
        assertEquals(200_000, distinct.size(), "challenges must not repeat");
    }

    @Test
    void theHandshakeCarriesTheChallengeIntact() {
        byte[] scramble = MySqlMessages.newScramble();
        byte[] hs = MySqlMessages.handshakeV10(7, scramble, false);
        String s = new String(hs, java.nio.charset.StandardCharsets.ISO_8859_1);
        String part1 = new String(scramble, 0, 8, java.nio.charset.StandardCharsets.ISO_8859_1);
        String part2 = new String(scramble, 8, 12, java.nio.charset.StandardCharsets.ISO_8859_1);
        assertTrue(s.contains(part1 + "\0"), "first 8 bytes then NUL");
        assertTrue(s.contains(part2 + "\0mysql_native_password"), "last 12 bytes, NUL, then the plugin name");
    }
}
