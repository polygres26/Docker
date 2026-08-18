package com.polygres.wire.acl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * A parsed {@code address/prefixLength} block — IPv4 or IPv6, same {@code CIDR} shape
 * {@code pg_hba.conf} itself uses for its own {@code host} records. Deliberately just address
 * matching, nothing more (no port range, no protocol) — see {@link ClientAcl}'s javadoc for why
 * this project mirrors {@code pg_hba.conf}'s rule shape rather than inventing a richer one.
 */
public final class Cidr {

    private final byte[] network;
    private final int prefixLength;

    private Cidr(byte[] network, int prefixLength) {
        this.network = network;
        this.prefixLength = prefixLength;
    }

    /** Accepts a bare address (implicit /32 or /128) or {@code address/prefixLength}. */
    public static Cidr parse(String spec) {
        String trimmed = spec.trim();
        int slash = trimmed.indexOf('/');
        String addressPart = slash < 0 ? trimmed : trimmed.substring(0, slash);
        try {
            InetAddress address = InetAddress.getByName(addressPart);
            byte[] bytes = address.getAddress();
            int maxPrefix = bytes.length * 8;
            int prefixLength = slash < 0 ? maxPrefix : Integer.parseInt(trimmed.substring(slash + 1));
            if (prefixLength < 0 || prefixLength > maxPrefix) {
                throw new IllegalArgumentException("prefix length " + prefixLength + " out of range for " + addressPart);
            }
            return new Cidr(maskTo(bytes, prefixLength), prefixLength);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("not a valid IP/CIDR: " + spec, e);
        }
    }

    public boolean contains(InetAddress candidate) {
        byte[] candidateBytes = candidate.getAddress();
        if (candidateBytes.length != network.length) {
            return false; // an IPv4 rule never matches an IPv6 candidate and vice versa -- no implicit v4-mapped-v6 coercion
        }
        return Arrays.equals(maskTo(candidateBytes, prefixLength), network);
    }

    private static byte[] maskTo(byte[] address, int prefixLength) {
        byte[] masked = address.clone();
        int fullBytes = prefixLength / 8;
        int remainingBits = prefixLength % 8;
        for (int i = 0; i < masked.length; i++) {
            if (i < fullBytes) {
                continue; // fully inside the prefix -- keep as-is
            }
            if (i == fullBytes && remainingBits > 0) {
                int mask = (0xFF << (8 - remainingBits)) & 0xFF;
                masked[i] = (byte) (masked[i] & mask);
            } else {
                masked[i] = 0; // fully outside the prefix -- zero it
            }
        }
        return masked;
    }

    @Override
    public String toString() {
        return "Cidr{/" + prefixLength + "}";
    }
}
