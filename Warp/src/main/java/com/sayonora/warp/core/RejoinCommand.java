package com.sayonora.warp.core;

import com.sayonora.warp.secrets.SecretResolver;
import java.util.Map;

/**
 * Runs the operator's {@code WARP_FAILOVER_REJOIN_COMMAND}: the part of rebuilding an old primary that needs the host (stopping and starting the
 * database, {@code pg_rewind}, restarting an instance) and that Warp, which only holds a database connection, cannot do itself. The command is
 * run through {@code sh -c} with the node and the current primary described in its environment: {@code WARP_REJOIN_NODE_URL},
 * {@code WARP_REJOIN_PRIMARY_URL}, {@code _HOST}, {@code _PORT} and {@code _USER}, plus whatever the engine adds ({@code PGPASSWORD} for
 * Postgres).
 */
final class RejoinCommand {

    private RejoinCommand() {
    }

    /** @return null when the command ran and exited 0, else why it did not (with the tail of its output) */
    static String run(String command, BackendTarget node, BackendTarget primary, long timeoutSeconds, int defaultPort,
            Map<String, String> extraEnv) {
        try {
            ProcessBuilder pb = new ProcessBuilder("sh", "-c", command).redirectErrorStream(true);
            Map<String, String> env = pb.environment();
            env.put("WARP_REJOIN_NODE_URL", node.jdbcUrl());
            env.put("WARP_REJOIN_PRIMARY_URL", primary.jdbcUrl());
            JdbcHostPort hp = JdbcHostPort.parse(primary.jdbcUrl(), defaultPort);
            env.put("WARP_REJOIN_PRIMARY_HOST", hp.host());
            env.put("WARP_REJOIN_PRIMARY_PORT", String.valueOf(hp.port()));
            env.put("WARP_REJOIN_PRIMARY_USER", primary.user() == null ? "" : primary.user());
            env.putAll(extraEnv);
            Process p = pb.start();
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            Thread drain = new Thread(() -> {
                try {
                    p.getInputStream().transferTo(out);
                } catch (java.io.IOException ignored) {
                    // process ended
                }
            }, "warp-rejoin-output");
            drain.setDaemon(true);
            drain.start();
            if (!p.waitFor(timeoutSeconds, java.util.concurrent.TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return "the rejoin command did not finish within " + timeoutSeconds + "s and was stopped";
            }
            drain.join(1000);
            if (p.exitValue() != 0) {
                return "the rejoin command failed (exit " + p.exitValue() + "): " + tail(out.toString());
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "interrupted while running the rejoin command";
        } catch (Exception e) {
            return "could not run the rejoin command: " + e.getMessage();
        }
    }

    static long timeoutSeconds() {
        String t = System.getenv("WARP_FAILOVER_REJOIN_TIMEOUT_SECONDS");
        return t == null || t.isBlank() ? 120 : Long.parseLong(t.trim());
    }

    static String tail(String s) {
        String t = s.strip().replaceAll("\\s+", " ");
        return t.length() > 300 ? t.substring(t.length() - 300) : t;
    }

    /** The password of {@code node}, resolved, for the engines whose tools read it from the environment. */
    static String password(BackendTarget node) {
        return SecretResolver.resolve(node.password());
    }
}
