package com.sayonora.warp.tls.acme;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;

/** The ACME state directory: {@code account.key} (600), {@code account.json}, {@code fullchain.pem}, {@code privkey.pem} (600), {@code state.json}. */
public final class AcmeStore {

    public record Account(KeyPair key, String kid) {
    }

    private final Path dir;

    public AcmeStore(Path dir) {
        this.dir = dir;
    }

    public Path dir() {
        return dir;
    }

    public void init() throws IOException {
        Files.createDirectories(dir);
        try {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
        } catch (UnsupportedOperationException | IOException ignored) {
            // non-POSIX file system, or a mounted volume we may not chmod
        }
    }

    // ---- files ----

    static void writeSecret(Path target, String content) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.deleteIfExists(tmp);
        try {
            Files.createFile(tmp, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        } catch (UnsupportedOperationException e) {
            Files.createFile(tmp);
        }
        Files.writeString(tmp, content, StandardCharsets.UTF_8);
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    static void writePublic(Path target, String content) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.writeString(tmp, content, StandardCharsets.UTF_8);
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /** Key first, certificate second; each replaced atomically so a reader never sees a half-written file. */
    public void writeCertificate(String fullchainPem, String privateKeyPem) throws IOException {
        writeSecret(dir.resolve("privkey.pem"), privateKeyPem);
        writePublic(dir.resolve("fullchain.pem"), fullchainPem);
    }

    public String readFullchain() {
        return read("fullchain.pem");
    }

    public String readPrivkey() {
        return read("privkey.pem");
    }

    private String read(String name) {
        try {
            Path p = dir.resolve(name);
            return Files.isRegularFile(p) ? Files.readString(p, StandardCharsets.UTF_8) : null;
        } catch (IOException e) {
            return null;
        }
    }

    // ---- account ----

    public Account readAccount(String directoryUrl) {
        try {
            Path meta = dir.resolve("account.json");
            String key = read("account.key");
            if (key == null || !Files.isRegularFile(meta)) {
                return null;
            }
            JsonObject j = JsonParser.parseString(Files.readString(meta)).getAsJsonObject();
            if (!directoryUrl.equals(j.get("directory").getAsString())) {
                return null; // registered with another CA/environment (e.g. staging vs production)
            }
            return new Account(AcmeCrypto.keyPairFromPem(key), j.has("kid") ? j.get("kid").getAsString() : null);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    public void writeAccount(String directoryUrl, Account a) throws IOException {
        writeSecret(dir.resolve("account.key"), AcmeCrypto.keyPairPem(a.key()));
        JsonObject j = new JsonObject();
        j.addProperty("directory", directoryUrl);
        if (a.kid() != null) {
            j.addProperty("kid", a.kid());
        }
        writePublic(dir.resolve("account.json"), j.toString());
    }

    // ---- runtime state ----

    public JsonObject readState() {
        try {
            Path p = dir.resolve("state.json");
            return Files.isRegularFile(p) ? JsonParser.parseString(Files.readString(p)).getAsJsonObject() : new JsonObject();
        } catch (IOException | RuntimeException e) {
            return new JsonObject();
        }
    }

    public void writeState(JsonObject state) {
        try {
            writePublic(dir.resolve("state.json"), new Gson().toJson(state));
        } catch (IOException ignored) {
            // status only; never fatal
        }
    }
}
