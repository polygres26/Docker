package com.sayonora.warp.awswire;

import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.params.ParametersWithID;
import org.bouncycastle.crypto.params.ParametersWithRandom;
import org.bouncycastle.crypto.signers.SM2Signer;
import org.bouncycastle.jcajce.provider.asymmetric.util.ECUtil;

/**
 * SM2 (the Chinese national elliptic-curve signature, curve sm2p256v1, SM3 digest) with BouncyCastle's lightweight
 * {@code SM2Signer} -- the BouncyCastle build in the Warp jar predates the JCA {@code SM3withSM2} name. Real KMS offers SM2 in
 * the China regions only; the emulator offers it in every region. The user id is the standard default
 * {@code 1234567812345678}; signatures are DER (r, s).
 */
final class KmsSm2 {

    private static final byte[] USER_ID = "1234567812345678".getBytes(StandardCharsets.UTF_8);

    private KmsSm2() {
    }

    static KmsCrypto.Generated generate() throws Exception {
        KmsCrypto.bc();
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC", "BC");
        g.initialize(new ECGenParameterSpec("sm2p256v1"));
        KeyPair kp = g.generateKeyPair();
        return new KmsCrypto.Generated(kp.getPrivate().getEncoded(), kp.getPublic().getEncoded());
    }

    static byte[] sign(byte[] pkcs8, byte[] message) throws Exception {
        KmsCrypto.bc();
        PrivateKey k = KeyFactory.getInstance("EC", "BC").generatePrivate(new PKCS8EncodedKeySpec(pkcs8));
        CipherParameters p = ECUtil.generatePrivateKeyParameter(k);
        SM2Signer s = new SM2Signer();
        s.init(true, new ParametersWithID(new ParametersWithRandom(p, new SecureRandom()), USER_ID));
        s.update(message, 0, message.length);
        return s.generateSignature();
    }

    static boolean verify(byte[] x509, byte[] message, byte[] signature) throws Exception {
        KmsCrypto.bc();
        PublicKey k = KeyFactory.getInstance("EC", "BC").generatePublic(new X509EncodedKeySpec(x509));
        CipherParameters p = ECUtil.generatePublicKeyParameter(k);
        SM2Signer s = new SM2Signer();
        s.init(false, new ParametersWithID(p, USER_ID));
        s.update(message, 0, message.length);
        return s.verifySignature(signature);
    }
}
