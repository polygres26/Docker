// Portions adapted from Floci (https://github.com/floci-io/floci), MIT License, Copyright (c) 2025 Floci and its contributors.
// See Warp/NOTICE for the licence text.
package com.sayonora.warp.sqswire;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message move tasks (StartMessageMoveTask / CancelMessageMoveTask / ListMessageMoveTasks): redrive of
 * messages out of a dead-letter queue back to their original source queues (or to an explicit
 * destination). Task state is persisted in {@code sqs_move_tasks} on the catalog backend; a worker thread
 * moves batches in the background -- source and destination may live on different shard backends (see
 * {@link PgQueueStore#moveBatch}). Running tasks are resumed when Warp restarts.
 *
 * <p>Like real SQS, a source must be a dead-letter queue (some queue's redrive policy targets it) and only
 * one task may be active per source queue. A task completes once no visible messages remain in the source;
 * messages that are in flight are not moved.
 */
final class SqsMoveTasks {

    private static final Logger log = LoggerFactory.getLogger(SqsMoveTasks.class);

    private final PgQueueStore store;
    private final ExecutorService workers = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "sqswire-move-task");
        t.setDaemon(true);
        return t;
    });

    SqsMoveTasks(PgQueueStore store) {
        this.store = store;
    }

    String start(String sourceArn, String destinationArn, Integer maxPerSecond) throws SQLException {
        if (sourceArn == null || sourceArn.isEmpty()) {
            throw SqsException.missing("SourceArn");
        }
        String source = PgQueueStore.queueNameFromArn(sourceArn);
        if (source == null) {
            throw SqsException.invalidParam("Value " + sourceArn + " for parameter SourceArn is invalid.");
        }
        if (maxPerSecond != null && (maxPerSecond < 1 || maxPerSecond > 500)) {
            throw SqsException.invalidParam("Value " + maxPerSecond
                    + " for parameter MaxNumberOfMessagesPerSecond is invalid. Reason: Must be between 1 and 500, if provided.");
        }
        PgQueueStore.QueueAttributes src = store.findQueueFresh(source);
        if (src == null) {
            throw new SqsException(400, "ResourceNotFoundException", "The resource that you specified for the SourceArn "
                    + "parameter doesn't exist.");
        }
        if (store.listDeadLetterSources(source, 1, null).isEmpty()) {
            throw SqsException.invalidParam("Source queue must be configured as a Dead Letter Queue.");
        }
        if (destinationArn != null && !destinationArn.isEmpty()) {
            String dest = PgQueueStore.queueNameFromArn(destinationArn);
            if (dest == null) {
                throw SqsException.invalidParam("Value " + destinationArn + " for parameter DestinationArn is invalid.");
            }
            if (store.findQueueFresh(dest) == null) {
                throw new SqsException(400, "ResourceNotFoundException", "The resource that you specified for the "
                        + "DestinationArn parameter doesn't exist.");
            }
        } else {
            destinationArn = null;
        }
        for (PgQueueStore.MoveTask t : store.listMoveTasks(sourceArn, 20)) {
            if (t.status().equals("RUNNING") || t.status().equals("CANCELLING")) {
                throw SqsException.invalidParam("There is already a task running. Only one active task is allowed for a source queue "
                        + "at a time.");
            }
        }
        PgQueueStore.QueueCounts counts = store.countMessages(source, src);
        PgQueueStore.MoveTask task = new PgQueueStore.MoveTask(UUID.randomUUID().toString(), sourceArn, destinationArn,
                maxPerSecond == null ? 0 : maxPerSecond, "RUNNING", 0, counts.visible(), null, System.currentTimeMillis());
        store.insertMoveTask(task);
        workers.execute(() -> run(task));
        return task.handle();
    }

    long cancel(String handle) throws SQLException {
        if (handle == null || handle.isEmpty()) {
            throw SqsException.missing("TaskHandle");
        }
        PgQueueStore.MoveTask t = store.getMoveTask(handle);
        if (t == null) {
            throw new SqsException(400, "ResourceNotFoundException", "The task handle specified is not valid or doesn't exist.");
        }
        if (!store.requestCancelMoveTask(handle)) {
            throw SqsException.invalidParam("Task must be in RUNNING state to be cancelled, but it is " + t.status() + ".");
        }
        // the worker notices on its next batch; report progress so far
        return store.getMoveTask(handle).moved();
    }

    List<PgQueueStore.MoveTask> list(String sourceArn, int max) throws SQLException {
        if (sourceArn == null || sourceArn.isEmpty()) {
            throw SqsException.missing("SourceArn");
        }
        return store.listMoveTasks(sourceArn, max);
    }

    /** Re-launches tasks that were running when the previous Warp process stopped. */
    void resumeRunning() {
        try {
            if (!store.catalogIsPostgres()) {
                return;
            }
            for (PgQueueStore.MoveTask t : store.runningMoveTasks()) {
                workers.execute(() -> run(t));
            }
        } catch (SQLException | RuntimeException e) {
            log.warn("sqswire: could not resume message move tasks: {}", e.getMessage());
        }
    }

    void shutdown() {
        workers.shutdownNow();
    }

    private void run(PgQueueStore.MoveTask task) {
        long moved = task.moved();
        try {
            String source = PgQueueStore.queueNameFromArn(task.sourceArn());
            String fixedDest = task.destinationArn() == null ? null : PgQueueStore.queueNameFromArn(task.destinationArn());
            Map<String, PgQueueStore.MoveDestination> cache = new HashMap<>();
            int batch = task.maxPerSecond() > 0 ? Math.min(10, task.maxPerSecond()) : 10;
            while (true) {
                PgQueueStore.MoveTask current = store.getMoveTask(task.handle());
                if (current == null || !(current.status().equals("RUNNING") || current.status().equals("CANCELLING"))) {
                    return;
                }
                if (current.status().equals("CANCELLING")) {
                    store.updateMoveTask(task.handle(), "CANCELLED", moved, null);
                    return;
                }
                PgQueueStore.QueueAttributes src = store.findQueueFresh(source);
                if (src == null) {
                    store.updateMoveTask(task.handle(), "FAILED", moved, "The source queue no longer exists.");
                    return;
                }
                long batchStart = System.nanoTime();
                PgQueueStore.MoveResult r = store.moveBatch(source, src, batch, msg -> {
                    String destName = fixedDest != null ? fixedDest : PgQueueStore.queueNameFromArn(msg.dlqSourceArn());
                    if (destName == null) {
                        return null;
                    }
                    return cache.computeIfAbsent(destName, n -> {
                        try {
                            PgQueueStore.QueueAttributes a = store.findQueue(n);
                            return a == null ? null : new PgQueueStore.MoveDestination(n, a);
                        } catch (SQLException e) {
                            throw new IllegalStateException(e);
                        }
                    });
                });
                moved += r.moved();
                if (r.scanned() == 0) {
                    store.updateMoveTask(task.handle(), "COMPLETED", moved, null);
                    return;
                }
                if (r.moved() == 0) {
                    store.updateMoveTask(task.handle(), "FAILED", moved, r.skipped() + " message(s) had no resolvable destination "
                            + "queue (the source queue no longer exists or the message has no recorded source).");
                    return;
                }
                if (store.updateMoveTask(task.handle(), "RUNNING", moved, null) == null) {
                    return;
                }
                if (task.maxPerSecond() > 0) {
                    long budgetMs = 1000L * r.moved() / task.maxPerSecond();
                    long spentMs = (System.nanoTime() - batchStart) / 1_000_000L;
                    if (budgetMs > spentMs) {
                        Thread.sleep(budgetMs - spentMs);
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("sqswire: message move task {} failed: {}", task.handle(), e.getMessage());
            try {
                store.updateMoveTask(task.handle(), "FAILED", moved, String.valueOf(e.getMessage()));
            } catch (SQLException ignored) {
                // best effort
            }
        }
    }
}
