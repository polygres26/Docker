package com.sayonora.warp.tls.acme;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * dns-01 through an operator command ({@code WARP_ACME_DNS_HOOK}, run with {@code sh -c}) that receives
 * {@code ACME_DOMAIN}, {@code ACME_TXT_NAME}, {@code ACME_TXT_VALUE} and {@code ACME_ACTION=present|cleanup} in its environment.
 * Works with any DNS host, including ones Warp has no API for and hosts behind NAT. A non-zero exit fails the order.
 */
public final class HookDnsProvider implements DnsProvider {

    private static final Logger log = LoggerFactory.getLogger(HookDnsProvider.class);
    private final String command;
    private final long timeoutSeconds;

    public HookDnsProvider(String command, long timeoutSeconds) {
        this.command = command;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    public String name() {
        return "hook";
    }

    @Override
    public void present(String domain, String txtName, String txtValue) throws Exception {
        run("present", domain, txtName, txtValue);
    }

    @Override
    public void cleanup(String domain, String txtName, String txtValue) throws Exception {
        run("cleanup", domain, txtName, txtValue);
    }

    private void run(String action, String domain, String txtName, String txtValue) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("sh", "-c", command).redirectErrorStream(true);
        Map<String, String> env = pb.environment();
        env.put("ACME_ACTION", action);
        env.put("ACME_DOMAIN", domain);
        env.put("ACME_TXT_NAME", txtName);
        env.put("ACME_TXT_VALUE", txtValue);
        Process p = pb.start();
        StringBuilder out = new StringBuilder();
        Thread reader = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String l;
                while ((l = r.readLine()) != null) {
                    if (out.length() < 2000) {
                        out.append(l).append('\n');
                    }
                }
            } catch (java.io.IOException ignored) {
                // process ended
            }
        }, "warp-acme-hook-out");
        reader.setDaemon(true);
        reader.start();
        if (!p.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new java.io.IOException("DNS hook (" + action + ") timed out after " + timeoutSeconds + "s");
        }
        reader.join(1000);
        if (p.exitValue() != 0) {
            throw new java.io.IOException("DNS hook (" + action + ") exited with status " + p.exitValue()
                    + (out.length() == 0 ? "" : ": " + out.toString().strip()));
        }
        log.info("ACME DNS hook {} for {} finished", action, txtName);
    }
}
