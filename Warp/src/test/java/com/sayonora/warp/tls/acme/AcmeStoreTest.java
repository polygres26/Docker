package com.sayonora.warp.tls.acme;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AcmeStoreTest {

    @Test
    void certificateFilesAreWrittenAtomicallyWithKeyFileMode600(@TempDir Path dir) throws Exception {
        AcmeStore s = new AcmeStore(dir.resolve("acme"));
        s.init();
        assertEquals(0700, perm(s.dir()));
        s.writeCertificate("FULLCHAIN", "PRIVKEY");
        assertEquals("FULLCHAIN", s.readFullchain());
        assertEquals("PRIVKEY", s.readPrivkey());
        assertEquals(0600, perm(s.dir().resolve("privkey.pem")));
        assertTrue(Files.exists(s.dir().resolve("fullchain.pem")));
    }

    @Test
    void accountRoundTripsAndRejectsAMismatchedDirectory() throws Exception {
        AcmeStore s = new AcmeStore(dir());
        s.init();
        assertNull(s.readAccount("https://ca.example/directory"));
        KeyPair k = AcmeCrypto.newEcKey();
        s.writeAccount("https://ca.example/directory", new AcmeStore.Account(k, "https://ca.example/acct/1"));
        assertEquals(0600, perm(s.dir().resolve("account.key")));
        AcmeStore.Account back = s.readAccount("https://ca.example/directory");
        assertEquals("https://ca.example/acct/1", back.kid());
        assertEquals(AcmeCrypto.thumbprint(k.getPublic()), AcmeCrypto.thumbprint(back.key().getPublic()));
        assertNull(s.readAccount("https://other-ca.example/directory"));   // registered with a different CA/environment
    }

    private static Path dir() throws Exception {
        return Files.createTempDirectory("acme-store-test");
    }

    private static int perm(Path p) throws Exception {
        var perms = Files.getPosixFilePermissions(p);
        return toOctal(perms);
    }

    private static int toOctal(java.util.Set<java.nio.file.attribute.PosixFilePermission> perms) {
        int mode = 0;
        var order = new java.nio.file.attribute.PosixFilePermission[] {
            java.nio.file.attribute.PosixFilePermission.OWNER_READ, java.nio.file.attribute.PosixFilePermission.OWNER_WRITE,
            java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE, java.nio.file.attribute.PosixFilePermission.GROUP_READ,
            java.nio.file.attribute.PosixFilePermission.GROUP_WRITE, java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE,
            java.nio.file.attribute.PosixFilePermission.OTHERS_READ, java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE,
            java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE};
        for (var p : order) {
            mode = (mode << 1) | (perms.contains(p) ? 1 : 0);
        }
        return mode;
    }
}
