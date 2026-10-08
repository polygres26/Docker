package com.sayonora.warp.core;

/**
 * Named places in a slot move where a test can make Warp fail or die, to prove the table is left consistent and the move is cleaned up or
 * resumed. Inert unless {@code WARP_FAULT_AT} names the point: with {@code WARP_FAULT_MODE=halt} the JVM is halted on the spot (no finally blocks,
 * no shutdown hooks -- what a kill -9 or a power cut does), otherwise an exception is thrown. Points: {@code copy-mid}, {@code after-verify},
 * {@code after-publish}, {@code before-switch}, {@code after-switch}, {@code mid-purge}. Meant for tests only; never set it in production.
 */
final class FaultPoints {

    private FaultPoints() {
    }

    static void hit(String point) {
        String at = System.getenv("WARP_FAULT_AT");
        if (at == null || !at.equals(point)) {
            return;
        }
        if ("halt".equalsIgnoreCase(System.getenv("WARP_FAULT_MODE"))) {
            Runtime.getRuntime().halt(137);
        }
        throw new IllegalStateException("injected fault at " + point);
    }
}
