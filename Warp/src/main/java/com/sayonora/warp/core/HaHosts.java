package com.sayonora.warp.core;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/** Host comparison for the failover checks, where one side comes from a JDBC URL and the other from the database's own view of a peer. */
final class HaHosts {

    private HaHosts() {
    }

    /** True when both names are the same host: equal ignoring case, both loopback, or resolving to a common address. */
    static boolean sameHost(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        String x = strip(a);
        String y = strip(b);
        if (x.equalsIgnoreCase(y)) {
            return true;
        }
        try {
            Set<InetAddress> xs = new HashSet<>(java.util.List.of(InetAddress.getAllByName(x)));
            for (InetAddress i : InetAddress.getAllByName(y)) {
                if (xs.contains(i) || (i.isLoopbackAddress() && xs.stream().anyMatch(InetAddress::isLoopbackAddress))) {
                    return true;
                }
            }
        } catch (Exception e) {
            return x.toLowerCase(Locale.ROOT).equals(y.toLowerCase(Locale.ROOT));
        }
        return false;
    }

    private static String strip(String h) {
        String t = h.trim();
        return t.startsWith("[") && t.endsWith("]") ? t.substring(1, t.length() - 1) : t;
    }
}
