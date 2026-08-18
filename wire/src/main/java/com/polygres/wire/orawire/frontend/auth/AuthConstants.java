package com.polygres.wire.orawire.frontend.auth;

/**
 * O5LOGON constants, per reference/o5logon_auth_spec.md.
 *
 * KNOWN GAP: TNS_VERIFIER_TYPE_11G_1/11G_2 (legacy AES-192/SHA1 scheme) are
 * listed for completeness but not implemented — this server only speaks the
 * 12c/PBKDF2 scheme (spec §2, "our server is greenfield ... implement the
 * 12c/PBKDF2 scheme").
 */
public final class AuthConstants {

    public static final int FUNC_AUTH_PHASE_ONE = 118;
    public static final int FUNC_AUTH_PHASE_TWO = 115;

    public static final long AUTH_MODE_LOGON = 0x00000001L;
    public static final long AUTH_MODE_WITH_PASSWORD = 0x00000100L;

    public static final long VERIFIER_TYPE_11G_1 = 0xb152L;
    public static final long VERIFIER_TYPE_11G_2 = 0x1b25L;
    public static final long VERIFIER_TYPE_12C = 0x4815L;

    // VGEN_COUNT hardens the low-entropy user *password* — deliberately
    // expensive (4096 iterations), confirmed against a live capture of the
    // `orawire` repo's server (docs/option-b-replatform-plan.md) running a
    // real, successful O5LOGON exchange with the actual ojdbc11 driver.
    public static final int PBKDF2_VGEN_COUNT = 4096;

    // SDER_COUNT derives the final combo key from session-key material that
    // is already high-entropy (two 256-bit random halves), so it doesn't
    // need password-grade hardening — the same live capture that confirmed
    // VGEN_COUNT above showed the real value is 3, not 4096. This was
    // previously set to 4096 (presumably copy-pasted from VGEN_COUNT above);
    // since this value is transmitted as a decimal *string* in the
    // AUTH_PBKDF2_SDER_COUNT key/value pair, "4096" (4 bytes) vs "3" (1
    // byte) shifts every field after it in the phase-one response by 3
    // bytes — a real, concrete framing bug, not just a slower KDF.
    public static final int PBKDF2_SDER_COUNT = 3;

    private AuthConstants() {
    }
}
