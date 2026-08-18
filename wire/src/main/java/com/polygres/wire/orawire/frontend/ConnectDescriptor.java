package com.polygres.wire.orawire.frontend;

/**
 * Parsed form of the Oracle connect descriptor string sent in the CONNECT
 * packet, e.g. (DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=orcl))...).
 */
public final class ConnectDescriptor {

    private final String serviceName;
    private final String rawDescriptor;

    private ConnectDescriptor(String serviceName, String rawDescriptor) {
        this.serviceName = serviceName;
        this.rawDescriptor = rawDescriptor;
    }

    public static ConnectDescriptor parse(String connectString) {
        // TODO: real TNS descriptor parsing (nested parens/key-value pairs);
        // for now just pull out SERVICE_NAME with a naive scan.
        String serviceName = extractValue(connectString, "SERVICE_NAME");
        return new ConnectDescriptor(serviceName, connectString);
    }

    private static String extractValue(String descriptor, String key) {
        String marker = key + "=";
        int idx = descriptor.indexOf(marker);
        if (idx < 0) {
            return null;
        }
        int start = idx + marker.length();
        int end = descriptor.indexOf(')', start);
        return end < 0 ? descriptor.substring(start) : descriptor.substring(start, end);
    }

    public String serviceName() {
        return serviceName;
    }

    public String rawDescriptor() {
        return rawDescriptor;
    }
}
