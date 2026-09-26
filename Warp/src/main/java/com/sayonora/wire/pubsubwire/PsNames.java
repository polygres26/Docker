package com.sayonora.wire.pubsubwire;

import java.util.regex.Pattern;

/** Pub/Sub resource names: {@code projects/{p}/{topics|subscriptions|snapshots|schemas}/{id}}. */
final class PsNames {

    private static final Pattern ID = Pattern.compile("[A-Za-z][A-Za-z0-9\\-_.~+%]{2,254}");
    private static final String HELP = "Refer to https://cloud.google.com/pubsub/docs/pubsub-basics#resource_names for more information.";

    /** A parsed name: project id, resource id and the full canonical name. */
    record Name(String project, String id, String full) {
    }

    private PsNames() {
    }

    static Name parse(String kind, String name) {
        if (name == null) {
            throw invalidName(name);
        }
        String[] p = name.split("/", -1);
        if (p.length != 4 || !p[0].equals("projects") || !p[2].equals(kind) || p[1].isEmpty() || p[3].isEmpty()) {
            throw invalidName(name);
        }
        if (!ID.matcher(p[3]).matches() || p[3].startsWith("goog")) {
            throw PsException.invalid("Invalid [" + kind + "] name: (name=" + name + ")");
        }
        return new Name(p[1], p[3], name);
    }

    /** A lenient parse for lookups/deletes: a well-formed name that does not exist is NOT_FOUND, not INVALID_ARGUMENT. */
    static Name parseExisting(String kind, String name) {
        if (name == null) {
            throw invalidName(name);
        }
        String[] p = name.split("/", -1);
        if (p.length != 4 || !p[0].equals("projects") || !p[2].equals(kind) || p[1].isEmpty() || p[3].isEmpty()) {
            throw invalidName(name);
        }
        return new Name(p[1], p[3], name);
    }

    static String project(String name) {
        if (name == null) {
            throw invalidName(name);
        }
        String[] p = name.split("/", -1);
        if (p.length != 2 || !p[0].equals("projects") || p[1].isEmpty()) {
            throw invalidName(name);
        }
        return p[1];
    }

    static PsException invalidName(String name) {
        return PsException.invalid("Invalid resource name given (name=" + name + "). " + HELP);
    }
}
