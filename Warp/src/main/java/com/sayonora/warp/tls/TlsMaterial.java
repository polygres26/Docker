package com.sayonora.warp.tls;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

/** One immutable snapshot of the key material (server key + chain, optional client-CA trust anchors). */
public final class TlsMaterial {

    private final PrivateKey key;
    private final X509Certificate[] chain;
    private final X509Certificate[] trust;
    private final char[] keyPassword;
    private final KeyStore keyStore;

    private TlsMaterial(PrivateKey key, X509Certificate[] chain, X509Certificate[] trust) {
        this.key = key;
        this.chain = chain;
        this.trust = trust;
        byte[] pw = new byte[16];
        new SecureRandom().nextBytes(pw);
        this.keyPassword = java.util.HexFormat.of().formatHex(pw).toCharArray();
        try {
            this.keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(null, null);
            keyStore.setKeyEntry("warp", key, keyPassword, chain);
        } catch (GeneralSecurityException | IOException e) {
            throw new TlsException("cannot assemble TLS key material: " + e.getMessage(), e);
        }
    }

    public X509Certificate[] chain() {
        return chain.clone();
    }

    public X509Certificate leaf() {
        return chain[0];
    }

    public X509Certificate[] trust() {
        return trust.clone();
    }

    public PrivateKey key() {
        return key;
    }

    public KeyManagerFactory keyManagerFactory() {
        try {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, keyPassword);
            return kmf;
        } catch (GeneralSecurityException e) {
            throw new TlsException("cannot build key manager: " + e.getMessage(), e);
        }
    }

    /** Trust managers over the client CAs, or null when no client CA is configured. */
    public TrustManagerFactory trustManagerFactory() {
        if (trust.length == 0) {
            return null;
        }
        try {
            KeyStore ts = KeyStore.getInstance("PKCS12");
            ts.load(null, null);
            for (int i = 0; i < trust.length; i++) {
                ts.setCertificateEntry("ca" + i, trust[i]);
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            return tmf;
        } catch (GeneralSecurityException | IOException e) {
            throw new TlsException("cannot build trust manager: " + e.getMessage(), e);
        }
    }

    public String describe() {
        X509Certificate c = leaf();
        return "subject=" + c.getSubjectX500Principal().getName() + " notAfter=" + c.getNotAfter();
    }

    /** Loads material according to {@code s}; every failure is a {@link TlsException} with an operator-readable message. */
    public static TlsMaterial load(TlsSettings s) {
        X509Certificate[] trust = s.ca() == null ? new X509Certificate[0] : PemLoader.readCertificates(s.ca());
        switch (s.source()) {
            case PEM: {
                X509Certificate[] chain = PemLoader.readCertificates(s.cert());
                PrivateKey key = PemLoader.readPrivateKey(s.key(), s.keyPassword());
                checkPair(key, chain[0], s);
                return new TlsMaterial(key, chain, trust);
            }
            case KEYSTORE:
                return fromKeystore(s, trust);
            case SELF_SIGNED:
                return selfSigned(s);
            default:
                throw new IllegalStateException();
        }
    }

    private static void checkPair(PrivateKey key, X509Certificate leaf, TlsSettings s) {
        try {
            byte[] data = new byte[] {1, 2, 3, 4};
            String alg = key.getAlgorithm().equals("EC") ? "SHA256withECDSA"
                    : key.getAlgorithm().equals("RSA") ? "SHA256withRSA" : null;
            if (alg == null) {
                return;
            }
            java.security.Signature sig = java.security.Signature.getInstance(alg);
            sig.initSign(key);
            sig.update(data);
            byte[] signed = sig.sign();
            sig.initVerify(leaf.getPublicKey());
            sig.update(data);
            if (!sig.verify(signed)) {
                throw new TlsException("the private key " + s.key() + " does not match the certificate " + s.cert());
            }
        } catch (GeneralSecurityException e) {
            throw new TlsException("the private key " + s.key() + " does not match the certificate " + s.cert()
                    + " (" + e.getMessage() + ")", e);
        }
    }

    private static TlsMaterial fromKeystore(TlsSettings s, X509Certificate[] pemTrust) {
        char[] pw = s.keystorePassword() == null ? new char[0] : s.keystorePassword().toCharArray();
        KeyStore ks;
        if (!Files.isRegularFile(s.keystore())) {
            throw new TlsException("TLS keystore not found: " + s.keystore());
        }
        try {
            ks = KeyStore.getInstance(s.keystore().toFile(), pw); // auto-detects PKCS12 vs JKS
        } catch (java.nio.file.NoSuchFileException | java.io.FileNotFoundException e) {
            throw new TlsException("TLS keystore not found: " + s.keystore());
        } catch (IOException e) {
            String m = e.getMessage() == null ? "" : e.getMessage();
            if (m.contains("password") || (e.getCause() instanceof java.security.UnrecoverableKeyException)) {
                throw new TlsException("cannot open TLS keystore " + s.keystore()
                        + ": wrong keystore password (check *_TLS_KEYSTORE_PASSWORD)", e);
            }
            throw new TlsException("cannot open TLS keystore " + s.keystore() + ": " + m, e);
        } catch (GeneralSecurityException e) {
            throw new TlsException("cannot open TLS keystore " + s.keystore() + ": " + e.getMessage(), e);
        }
        try {
            for (String alias : Collections.list(ks.aliases())) {
                if (ks.isKeyEntry(alias)) {
                    PrivateKey key = (PrivateKey) ks.getKey(alias, pw);
                    java.security.cert.Certificate[] cc = ks.getCertificateChain(alias);
                    X509Certificate[] chain = new X509Certificate[cc.length];
                    for (int i = 0; i < cc.length; i++) {
                        chain[i] = (X509Certificate) cc[i];
                    }
                    List<X509Certificate> trust = new ArrayList<>(List.of(pemTrust));
                    if (trust.isEmpty() && s.clientAuth() != TlsSettings.ClientAuth.NONE) {
                        for (String a : Collections.list(ks.aliases())) {
                            if (ks.isCertificateEntry(a)) {
                                trust.add((X509Certificate) ks.getCertificate(a));
                            }
                        }
                    }
                    return new TlsMaterial(key, chain, trust.toArray(new X509Certificate[0]));
                }
            }
        } catch (GeneralSecurityException e) {
            throw new TlsException("cannot read the key from TLS keystore " + s.keystore() + ": " + e.getMessage(), e);
        }
        throw new TlsException("TLS keystore " + s.keystore() + " contains no private-key entry");
    }

    /** DEV ONLY: a fresh self-signed RSA cert for localhost, generated with the JDK's own keytool. */
    private static TlsMaterial selfSigned(TlsSettings s) {
        Path dir = null;
        try {
            dir = Files.createTempDirectory("warp-selfsigned");
            Path ks = dir.resolve("ss.p12");
            String sans = "dns:localhost,ip:127.0.0.1,ip:0:0:0:0:0:0:0:1";
            for (String extra : s.selfSignedSans()) {
                sans += "," + (extra.matches("[0-9.]+|[0-9a-fA-F:]+:[0-9a-fA-F:]*") ? "ip:" : "dns:") + extra;
            }
            String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
            Process p = new ProcessBuilder(keytool, "-genkeypair", "-alias", "warp", "-keyalg", "RSA", "-keysize", "2048",
                    "-sigalg", "SHA256withRSA", "-validity", "90", "-dname", "CN=localhost,O=Warp (self-signed dev)",
                    "-ext", "san=" + sans, "-storetype", "PKCS12", "-keystore", ks.toString(), "-storepass", "changeit",
                    "-keypass", "changeit").redirectErrorStream(true).start();
            String out;
            try (InputStream in = p.getInputStream()) {
                out = new String(in.readAllBytes());
            }
            if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) {
                throw new TlsException("self-signed certificate generation (keytool) failed: " + out.trim());
            }
            KeyStore store = KeyStore.getInstance("PKCS12");
            try (InputStream in = Files.newInputStream(ks)) {
                store.load(in, "changeit".toCharArray());
            }
            PrivateKey key = (PrivateKey) store.getKey("warp", "changeit".toCharArray());
            java.security.cert.Certificate[] cc = store.getCertificateChain("warp");
            X509Certificate[] chain = new X509Certificate[cc.length];
            for (int i = 0; i < cc.length; i++) {
                chain[i] = (X509Certificate) cc[i];
            }
            return new TlsMaterial(key, chain, new X509Certificate[0]);
        } catch (IOException | GeneralSecurityException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new TlsException("cannot generate a self-signed certificate: " + e.getMessage(), e);
        } finally {
            if (dir != null) {
                try (var files = Files.walk(dir)) {
                    files.sorted(java.util.Comparator.reverseOrder()).forEach(f -> f.toFile().delete());
                } catch (IOException ignored) {
                    // best effort
                }
            }
        }
    }
}
