package com.polygres.wire.acl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

public final class Cidr {

    private final byte[] network;
    private final int prefixLength;

    private Cidr(byte[] network, int prefixLength) {
        this.network = network;
        this.prefixLength = prefixLength;
    }

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
            return false;
        }
        return Arrays.equals(maskTo(candidateBytes, prefixLength), network);
    }

    private static byte[] maskTo(byte[] address, int prefixLength) {
        byte[] masked = address.clone();
        int fullBytes = prefixLength / 8;
        int remainingBits = prefixLength % 8;
        for (int i = 0; i < masked.length; i++) {
            if (i < fullBytes) {
                continue;
            }
            if (i == fullBytes && remainingBits > 0) {
                int mask = (0xFF << (8 - remainingBits)) & 0xFF;
                masked[i] = (byte) (masked[i] & mask);
            } else {
                masked[i] = 0;
            }
        }
        return masked;
    }

    @Override
    public String toString() {
        return "Cidr{/" + prefixLength + "}";
    }
}
