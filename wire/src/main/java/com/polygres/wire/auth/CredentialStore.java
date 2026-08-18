package com.polygres.wire.auth;

public final class CredentialStore {

    private final String username;
    private final byte[] password;

    public CredentialStore() {
        this.username = System.getenv().getOrDefault("POLYWIRE_AUTH_USER", "orapg");
        this.password = System.getenv().getOrDefault("POLYWIRE_AUTH_PASSWORD", "orapg")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    public byte[] lookupPassword(String username) {
        return this.username.equalsIgnoreCase(username) ? password : null;
    }
}
