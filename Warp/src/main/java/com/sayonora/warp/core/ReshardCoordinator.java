package com.sayonora.warp.core;

import java.util.Collection;

/**
 * The cluster-wide side of {@link ReshardGate}: the instance doing a move publishes a phase, and every live Warp instance applies it to its own
 * gate and acknowledges once it has. The mover goes on only when all of them have, so no instance is still writing to a moving slot, or
 * computing a shard list from the old map, when the rows are copied or the map is switched. A published phase carries a lease the mover keeps
 * renewing; if it dies, instances release the hold when the lease runs out.
 */
public interface ReshardCoordinator {

    enum Phase {
        /** Hold writes to the given slots (and scatter reads too when asked), wait for admitted writes to finish. */
        FREEZE,
        /** Hold scatter reads of the table and wait for admitted ones to finish (writes stay as they were). */
        SCATTER,
        /** The new map is in the config at {@code flipVersion}: once applied locally, release the writes; scatter reads stay held. */
        FLIP,
        /** Release everything. */
        OPEN
    }

    /** Publishes {@code phase} for {@code table}; returns its epoch. Refuses (IllegalStateException) when another mover holds the table. */
    long publish(String table, Phase phase, Collection<Integer> slots, boolean blockScatter, long flipVersion) throws Exception;

    /** Extends the lease of the phase this process published. */
    void renew(String table) throws Exception;

    /** Waits until every live instance has acknowledged {@code epoch}; throws IllegalStateException naming the ones that have not. */
    void awaitAcks(String table, long epoch, long timeoutMillis) throws Exception;
}
