package com.nexagres.wire.orawire.frontend.auth;

public final class AuthConstants {

    public static final int FUNC_AUTH_PHASE_ONE = 118;
    public static final int FUNC_AUTH_PHASE_TWO = 115;

    public static final long AUTH_MODE_LOGON = 0x00000001L;
    public static final long AUTH_MODE_WITH_PASSWORD = 0x00000100L;

    public static final long VERIFIER_TYPE_11G_1 = 0xb152L;
    public static final long VERIFIER_TYPE_11G_2 = 0x1b25L;
    public static final long VERIFIER_TYPE_12C = 0x4815L;

    public static final int PBKDF2_VGEN_COUNT = 4096;

    public static final int PBKDF2_SDER_COUNT = 3;

    private AuthConstants() {
    }
}
