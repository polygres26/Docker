package com.sayonora.warp.tls.acme;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adapts a {@link DnsProvider} + {@link DnsChecker} to the client's challenge {@link AcmeClient.Responder}. */
public final class DnsChallengeResponder implements AcmeClient.Responder {

    private static final Logger log = LoggerFactory.getLogger(DnsChallengeResponder.class);
    private final DnsProvider provider;
    private final DnsChecker checker;
    private final int waitSeconds;

    public DnsChallengeResponder(DnsProvider provider, DnsChecker checker, int waitSeconds) {
        this.provider = provider;
        this.checker = checker;
        this.waitSeconds = waitSeconds;
    }

    static String txtName(String domain) {
        return "_acme-challenge." + domain;
    }

    @Override
    public void present(String domain, String type, String token, String keyAuthorization) throws Exception {
        String name = txtName(domain);
        String value = AcmeCrypto.dnsTxtValue(keyAuthorization);
        provider.present(domain, name, value);
        log.info("ACME dns-01: published TXT {} via {}; waiting up to {}s for propagation", name, provider.name(), waitSeconds);
        if (!checker.await(name, value, waitSeconds)) {
            log.warn("ACME dns-01: TXT {} not visible after {}s; asking the CA to validate anyway", name, waitSeconds);
        }
    }

    @Override
    public void cleanup(String domain, String type, String token, String keyAuthorization) {
        try {
            provider.cleanup(domain, txtName(domain), AcmeCrypto.dnsTxtValue(keyAuthorization));
        } catch (Exception e) {
            log.warn("ACME dns-01: could not remove TXT {} ({}): {}", txtName(domain), provider.name(), e.getMessage());
        }
    }
}
