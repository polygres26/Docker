package com.nexagres.wiretests;

/**
 * Connection details for a Warp instance already running (via ../../docker-compose.test.yml,
 * or your own {@code docker run}) -- these tests don't start or stop anything themselves, since
 * they're meant to run against the public, prebuilt image with no source checkout. Override
 * WARP_HOST if not running via the compose file's default port mapping on localhost.
 */
final class TestConfig {

    private TestConfig() {
    }

    static final String HOST = System.getenv().getOrDefault("WARP_HOST", "localhost");

    static int port(String envVar, int defaultPort) {
        String value = System.getenv(envVar);
        return value == null || value.isBlank() ? defaultPort : Integer.parseInt(value.trim());
    }

    static final int PGWIRE_PORT = port("WARP_PGWIRE_PORT", 15432);
    static final int MYWIRE_PORT = port("WARP_MYWIRE_PORT", 13306);
    static final int ORAWIRE_PORT = port("WARP_ORAWIRE_PORT", 11521);
    static final int MSSQLWIRE_PORT = port("WARP_MSSQLWIRE_PORT", 14333);
    static final int MONGOWIRE_PORT = port("WARP_MONGOWIRE_PORT", 27017);
    static final int DYNAMOWIRE_PORT = port("WARP_DYNAMOWIRE_PORT", 18000);
    static final int SQSWIRE_PORT = port("WARP_SQSWIRE_PORT", 9324);
    static final int OSWIRE_PORT = port("WARP_OSWIRE_PORT", 9200);
    static final int INFLUXWIRE_PORT = port("WARP_INFLUXWIRE_PORT", 8086);
    static final int BOLTWIRE_PORT = port("WARP_BOLTWIRE_PORT", 7687);
}
