package com.sayonora.warp.core;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ready-made fencing for promote mode, so that "stop the old primary before promoting" does not need a hand-written script. A fence is the
 * only thing that closes the case Warp cannot see: a primary that is alive but cut off from Warp and its replica while direct clients can still
 * reach it. Several can be configured; ALL must succeed, in this order, before a replica is promoted.
 *
 * <ul>
 *   <li>{@code WARP_FAILOVER_FENCE_COMMAND}: a shell command, {@code FAILED_PRIMARY_URL} in its environment (the original hook).</li>
 *   <li>{@code WARP_FAILOVER_FENCE_WEBHOOK}: POSTs JSON to a URL (a cloud function, an automation, a power switch API); any 2xx = fenced.
 *       {@code WARP_FAILOVER_FENCE_WEBHOOK_TOKEN} adds {@code Authorization: Bearer}.</li>
 *   <li>{@code WARP_FAILOVER_FENCE_SSH_TARGET} + {@code WARP_FAILOVER_FENCE_SSH_COMMAND}: runs the command on the old primary's machine over SSH
 *       (BatchMode, so key-based; extra options in {@code WARP_FAILOVER_FENCE_SSH_OPTS}). {@code {host}} in the target is the failed primary's host.</li>
 *   <li>{@code WARP_FAILOVER_FENCE_EXEC}: an argument vector run WITHOUT a shell, e.g. {@code docker -H ssh://{host} stop pg1},
 *       {@code kubectl -n db delete pod pg-0 --wait=true} or {@code aws ec2 stop-instances --instance-ids i-0abc}; placeholders
 *       {@code {host}}, {@code {port}} and {@code {url}} (the credential-masked JDBC URL).</li>
 * </ul>
 *
 * <p>{@code WARP_FAILOVER_REQUIRE_FENCE=true} makes Warp refuse to promote at all when none of these is configured (see {@link FailoverMonitor}).
 */
public final class FailoverFencers {

    private static final Logger log = LoggerFactory.getLogger(FailoverFencers.class);

    private FailoverFencers() {
    }

    public static boolean requireFence() {
        return "true".equalsIgnoreCase(System.getenv("WARP_FAILOVER_REQUIRE_FENCE"));
    }

    /** The fencer made of whatever is configured in the environment, or null when nothing is. */
    public static FailoverMonitor.Fencer fromEnv(Map<String, String> env) {
        List<FailoverMonitor.Fencer> parts = new ArrayList<>();
        String command = env.get("WARP_FAILOVER_FENCE_COMMAND");
        if (present(command)) {
            parts.add(url -> runShell(command, url));
        }
        String webhook = env.get("WARP_FAILOVER_FENCE_WEBHOOK");
        if (present(webhook)) {
            String token = env.get("WARP_FAILOVER_FENCE_WEBHOOK_TOKEN");
            parts.add(url -> postWebhook(webhook, token, url));
        }
        String sshTarget = env.get("WARP_FAILOVER_FENCE_SSH_TARGET");
        String sshCommand = env.get("WARP_FAILOVER_FENCE_SSH_COMMAND");
        if (present(sshTarget) && present(sshCommand)) {
            String opts = env.getOrDefault("WARP_FAILOVER_FENCE_SSH_OPTS", "");
            String bin = env.getOrDefault("WARP_FAILOVER_FENCE_SSH_BIN", "ssh");
            parts.add(url -> {
                List<String> argv = new ArrayList<>(List.of(bin, "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"));
                argv.addAll(split(opts));
                argv.add(expand(sshTarget, url));
                argv.add(sshCommand);
                return runArgv(argv, url);
            });
        } else if (present(sshTarget) != present(sshCommand)) {
            log.error("fencing: WARP_FAILOVER_FENCE_SSH_TARGET and WARP_FAILOVER_FENCE_SSH_COMMAND must both be set; SSH fencing is off");
        }
        String exec = env.get("WARP_FAILOVER_FENCE_EXEC");
        if (present(exec)) {
            List<String> template = split(exec);
            parts.add(url -> {
                List<String> argv = new ArrayList<>();
                for (String a : template) {
                    argv.add(expand(a, url));
                }
                return runArgv(argv, url);
            });
        }
        if (parts.isEmpty()) {
            return null;
        }
        return url -> {
            for (FailoverMonitor.Fencer f : parts) {
                if (!f.fence(url)) {
                    return false;
                }
            }
            return true;
        };
    }

    private static boolean present(String s) {
        return s != null && !s.isBlank();
    }

    private static List<String> split(String s) {
        List<String> out = new ArrayList<>();
        for (String t : s.trim().split("\\s+")) {
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out;
    }

    /** {@code {host}}, {@code {port}} and {@code {url}} (masked) of the failed primary. */
    static String expand(String template, String jdbcUrl) {
        String host = "";
        String port = "";
        try {
            JdbcHostPort hp = JdbcHostPort.parse(jdbcUrl, 0);
            host = hp.host();
            port = hp.port() == 0 ? "" : String.valueOf(hp.port());
        } catch (IllegalArgumentException e) {
            log.warn("fencing: cannot read a host from {}", BackendSetModel.maskUrl(jdbcUrl));
        }
        return template.replace("{host}", host).replace("{port}", port).replace("{url}", BackendSetModel.maskUrl(jdbcUrl));
    }

    private static boolean runShell(String command, String failedPrimaryUrl) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("sh", "-c", command);
        pb.environment().put("FAILED_PRIMARY_URL", failedPrimaryUrl);
        return run(pb);
    }

    private static boolean runArgv(List<String> argv, String failedPrimaryUrl) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(argv);
        pb.environment().put("FAILED_PRIMARY_URL", failedPrimaryUrl);
        return run(pb);
    }

    private static boolean run(ProcessBuilder pb) throws Exception {
        pb.redirectErrorStream(true);
        Process p = pb.start();
        p.getInputStream().transferTo(java.io.OutputStream.nullOutputStream());
        if (!p.waitFor(60, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            return false;
        }
        return p.exitValue() == 0;
    }

    private static boolean postWebhook(String endpoint, String token, String failedPrimaryUrl) throws Exception {
        String host = "";
        int port = 0;
        try {
            JdbcHostPort hp = JdbcHostPort.parse(failedPrimaryUrl, 0);
            host = hp.host();
            port = hp.port();
        } catch (IllegalArgumentException ignored) {
            // the receiver still gets the masked URL
        }
        com.google.gson.JsonObject body = new com.google.gson.JsonObject();
        body.addProperty("event", "failover-fence");
        body.addProperty("failedPrimaryUrl", BackendSetModel.maskUrl(failedPrimaryUrl));
        body.addProperty("host", host);
        body.addProperty("port", port);
        HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(endpoint)).timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body.toString()));
        if (present(token)) {
            req.header("Authorization", "Bearer " + token);
        }
        HttpResponse<String> resp = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
                .send(req.build(), HttpResponse.BodyHandlers.ofString());
        boolean ok = resp.statusCode() / 100 == 2;
        if (!ok) {
            log.warn("fencing: webhook answered HTTP {}", resp.statusCode());
        }
        return ok;
    }
}
