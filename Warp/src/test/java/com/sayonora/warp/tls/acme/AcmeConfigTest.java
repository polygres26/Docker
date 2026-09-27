package com.sayonora.warp.tls.acme;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AcmeConfigTest {

    private static Map<String, String> base() {
        Map<String, String> e = new HashMap<>();
        e.put("WARP_ACME_DOMAINS", "warp.example.com");
        e.put("WARP_ACME_TERMS_ACCEPTED", "true");
        e.put("WARP_ACME_EMAIL", "ops@example.com");
        return e;
    }

    @Test
    void unsetDomainsMeansAcmeIsNotRequestedAtAll() {
        assertEquals(null, AcmeConfig.fromEnv(new HashMap<>(), Path.of(".")));
    }

    @Test
    void termsMustBeExplicitlyAccepted() {
        Map<String, String> e = base();
        e.remove("WARP_ACME_TERMS_ACCEPTED");
        AcmeConfig.ConfigException ex = assertThrows(AcmeConfig.ConfigException.class, () -> AcmeConfig.fromEnv(e, Path.of(".")));
        assertTrue(ex.getMessage().contains("WARP_ACME_TERMS_ACCEPTED"));
        e.put("WARP_ACME_TERMS_ACCEPTED", "false");
        assertThrows(AcmeConfig.ConfigException.class, () -> AcmeConfig.fromEnv(e, Path.of(".")));
    }

    @Test
    void defaultsToLetsEncryptProductionUnlessStagingOrDirectoryGiven() {
        AcmeConfig c = AcmeConfig.fromEnv(base(), Path.of("."));
        assertEquals(AcmeConfig.LE_PROD, c.directory());
        assertFalse(c.staging());
        Map<String, String> e = base();
        e.put("WARP_ACME_STAGING", "true");
        assertEquals(AcmeConfig.LE_STAGING, AcmeConfig.fromEnv(e, Path.of(".")).directory());
        e.put("WARP_ACME_DIRECTORY", "https://ca.example/directory");
        assertEquals("https://ca.example/directory", AcmeConfig.fromEnv(e, Path.of(".")).directory());
    }

    @Test
    void wildcardRequiresDns01() {
        Map<String, String> e = base();
        e.put("WARP_ACME_DOMAINS", "*.example.com");
        assertThrows(AcmeConfig.ConfigException.class, () -> AcmeConfig.fromEnv(e, Path.of(".")));
        e.put("WARP_ACME_CHALLENGE", "dns-01");
        e.put("WARP_ACME_DNS_PROVIDER", "hook");
        e.put("WARP_ACME_DNS_HOOK", "true");
        AcmeConfig c = AcmeConfig.fromEnv(e, Path.of("."));
        assertEquals(AcmeConfig.Challenge.DNS_01, c.challenge());
    }

    @Test
    void rejectsIpAddressesAndLocalhost() {
        for (String bad : new String[] {"127.0.0.1", "localhost", "10.0.0.5"}) {
            Map<String, String> e = base();
            e.put("WARP_ACME_DOMAINS", bad);
            assertThrows(AcmeConfig.ConfigException.class, () -> AcmeConfig.fromEnv(e, Path.of(".")), bad);
        }
    }

    @Test
    void dnsProviderRequiresItsOwnCredentials() {
        Map<String, String> e = base();
        e.put("WARP_ACME_CHALLENGE", "dns-01");
        assertThrows(AcmeConfig.ConfigException.class, () -> AcmeConfig.fromEnv(e, Path.of(".")));
        e.put("WARP_ACME_DNS_PROVIDER", "cloudflare");
        assertThrows(AcmeConfig.ConfigException.class, () -> AcmeConfig.fromEnv(e, Path.of(".")));
        e.put("CLOUDFLARE_API_TOKEN", "tok");
        AcmeConfig.fromEnv(e, Path.of("."));   // now fine
        e.put("WARP_ACME_DNS_PROVIDER", "route53");
        assertThrows(AcmeConfig.ConfigException.class, () -> AcmeConfig.fromEnv(e, Path.of(".")));
        e.put("AWS_ACCESS_KEY_ID", "ak");
        e.put("AWS_SECRET_ACCESS_KEY", "sk");
        AcmeConfig.fromEnv(e, Path.of("."));
    }

    @Test
    void keyTypeAndRenewDaysAreValidated() {
        Map<String, String> e = base();
        e.put("WARP_ACME_KEY_TYPE", "ed25519");
        assertThrows(AcmeConfig.ConfigException.class, () -> AcmeConfig.fromEnv(e, Path.of(".")));
        e.put("WARP_ACME_KEY_TYPE", "rsa2048");
        assertEquals("rsa2048", AcmeConfig.fromEnv(e, Path.of(".")).keyType());
        e.put("WARP_ACME_RENEW_DAYS", "not-a-number");
        assertThrows(AcmeConfig.ConfigException.class, () -> AcmeConfig.fromEnv(e, Path.of(".")));
    }

    @Test
    void acmeDirDefaultsUnderWorkDirButCanBeOverridden() {
        AcmeConfig c1 = AcmeConfig.fromEnv(base(), Path.of("/work"));
        assertEquals(Path.of("/work/acme"), c1.dir());
        Map<String, String> e = base();
        e.put("WARP_ACME_DIR", "/custom/dir");
        assertEquals(Path.of("/custom/dir"), AcmeConfig.fromEnv(e, Path.of("/work")).dir());
    }

    @Test
    void eabKidAndHmacMustBeSetTogether() {
        Map<String, String> e = base();
        e.put("WARP_ACME_EAB_KID", "kid1");
        assertThrows(AcmeConfig.ConfigException.class, () -> AcmeConfig.fromEnv(e, Path.of(".")));
    }
}
