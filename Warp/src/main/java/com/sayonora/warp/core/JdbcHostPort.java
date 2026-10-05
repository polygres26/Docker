package com.sayonora.warp.core;

/** Host and port of the (first) server named in a JDBC URL such as {@code jdbc:postgresql://h:5432/db}. */
record JdbcHostPort(String host, int port) {

    /** Parses {@code jdbc:<vendor>://host[:port][,host2...]/...}. Supports bracketed IPv6. A missing port
     * yields {@code defaultPort}. Throws {@link IllegalArgumentException} for a URL with no {@code //host}. */
    static JdbcHostPort parse(String jdbcUrl, int defaultPort) {
        if (jdbcUrl == null) {
            throw new IllegalArgumentException("no JDBC URL");
        }
        int slashes = jdbcUrl.indexOf("//");
        if (slashes < 0) {
            throw new IllegalArgumentException("JDBC URL has no //host part: " + jdbcUrl);
        }
        String rest = jdbcUrl.substring(slashes + 2);
        int end = rest.length();
        for (char c : new char[] {'/', '?', ';', ','}) {
            int i = rest.indexOf(c);
            if (i >= 0 && i < end) {
                end = i;
            }
        }
        String authority = rest.substring(0, end);
        int at = authority.lastIndexOf('@');
        if (at >= 0) {
            authority = authority.substring(at + 1);
        }
        String host;
        int port = defaultPort;
        if (authority.startsWith("[")) {
            int close = authority.indexOf(']');
            if (close < 0) {
                throw new IllegalArgumentException("malformed IPv6 host in " + jdbcUrl);
            }
            host = authority.substring(1, close);
            String after = authority.substring(close + 1);
            if (after.startsWith(":") && after.length() > 1) {
                port = Integer.parseInt(after.substring(1));
            }
        } else {
            int colon = authority.lastIndexOf(':');
            if (colon >= 0) {
                host = authority.substring(0, colon);
                if (colon + 1 < authority.length()) {
                    port = Integer.parseInt(authority.substring(colon + 1));
                }
            } else {
                host = authority;
            }
        }
        if (host.isBlank()) {
            throw new IllegalArgumentException("JDBC URL has an empty host: " + jdbcUrl);
        }
        return new JdbcHostPort(host, port);
    }
}
