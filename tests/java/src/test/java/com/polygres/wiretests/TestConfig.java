package com.polygres.wiretests;

/**
 * Connection details for a PolyWire instance already running (via ../../docker-compose.test.yml,
 * or your own {@code docker run}) -- these tests don't start or stop anything themselves, since
 * they're meant to run against the public, prebuilt image with no source checkout. Override
 * POLYWIRE_HOST if not running via the compose file's default port mapping on localhost.
 */
final class TestConfig {

    private TestConfig() {
    }

    static final String HOST = System.getenv().getOrDefault("POLYWIRE_HOST", "localhost");

    static int port(String envVar, int defaultPort) {
        String value = System.getenv(envVar);
        return value == null || value.isBlank() ? defaultPort : Integer.parseInt(value.trim());
    }

    static final int PGWIRE_PORT = port("POLYWIRE_PGWIRE_PORT", 15432);
    static final int MYWIRE_PORT = port("POLYWIRE_MYWIRE_PORT", 13306);
    static final int ORAWIRE_PORT = port("POLYWIRE_ORAWIRE_PORT", 11521);
    static final int MSSQLWIRE_PORT = port("POLYWIRE_MSSQLWIRE_PORT", 14333);
    static final int MONGOWIRE_PORT = port("POLYWIRE_MONGOWIRE_PORT", 27017);
    static final int DYNAMOWIRE_PORT = port("POLYWIRE_DYNAMOWIRE_PORT", 18000);
    static final int SQSWIRE_PORT = port("POLYWIRE_SQSWIRE_PORT", 9324);
}
