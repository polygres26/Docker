package com.sayonora.warp.tls.acme;

/** Publishes and removes the dns-01 TXT record {@code _acme-challenge.<domain>}. */
public interface DnsProvider {

    /** @param domain the authorized name without "*." (e.g. example.com); @param txtName {@code _acme-challenge.example.com} */
    void present(String domain, String txtName, String txtValue) throws Exception;

    void cleanup(String domain, String txtName, String txtValue) throws Exception;

    String name();
}
