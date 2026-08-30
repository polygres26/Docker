package com.nexagres.migration.connectors.sqs;

import com.nexagres.migration.core.ChangeEvent;
import com.nexagres.migration.core.Partition;
import com.nexagres.migration.core.Sink;
import com.nexagres.migration.core.Source;
import com.nexagres.migration.core.StateStore;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * SQS connector -- genuinely different in kind from every other connector in this project, not
 * just in mechanism. Every other source is a durable, queryable store with a real point-in-time
 * snapshot separate from its own change feed (a table row set, a document collection). A queue
 * has no such thing: its only "state" is whatever messages currently sit in it, and once a message
 * is deleted (consumed) it is gone forever, with no redo log/oplog/binlog/change-table to replay.
 * There is consequently no meaningful split between "initial bulk copy" and "live sync" the way
 * {@link #readPartition} vs. {@link #streamChanges} implies for a table -- both do the EXACT SAME
 * work (receive whatever's currently available, forward it, delete it from the source), just with
 * a different stopping condition: {@link #readPartition} stops once the backlog appears drained
 * (a real, if inherently heuristic, "caught up" signal for an at-least-once queue), {@link
 * #streamChanges} runs the identical loop forever until {@link #close}.
 *
 * <p>Writes into the target's real sqswire physical schema (see {@code
 * com.nexagres.wire.sqswire.PgQueueStore} in the {@code wire} module: {@code sqs_queue_<name>}
 * with {@code msg_id bigserial}/{@code receipt_handle text}/{@code vt timestamptz}/{@code
 * enqueued_at timestamptz}/{@code read_ct int}/{@code body text}/{@code message_group_id text}/
 * {@code dedup_id text}, plus a row in the shared {@code sqs_queues_catalog} table) -- same
 * "match the wire's own physical schema exactly" principle as Mongo/DynamoDB, since sqswire, like
 * mongowire/dynamowire, stores its own physical state directly rather than proxying to Postgres
 * (confirmed by reading wire's own code this session). Unlike Mongo/DynamoDB's per-source-schema
 * convention, sqswire's own tables carry NO schema qualifier at all (bare, unqualified names,
 * matching DynamoDB's own {@code dynamo_item_<name>} convention of living in whatever schema the
 * connection's search_path resolves to) -- this connector deliberately does the same, not a schema
 * of its own.
 *
 * <p><b>At-least-once, matching SQS's own guarantee, not weaker than it</b>: a message is deleted
 * from the source only AFTER its target write succeeds. A crash between those two steps can
 * redeliver the same message on restart, landing it in the target a second time (as a genuinely
 * new row -- sqswire's own {@code msg_id} is server-assigned {@code bigserial}, there is no
 * natural per-message dedup key the way a table row has a primary key). This is a deliberate,
 * honest design choice, not a gap glossed over: real SQS itself is already only at-least-once, so
 * a consumer of the MIGRATED queue already has to tolerate an occasional duplicate exactly the way
 * any real SQS consumer already does -- this migration doesn't need to invent stronger guarantees
 * than the source system it's copying ever promised.
 *
 * <p><b>Known, scoped limitations</b>: {@code MessageGroupId}/{@code MessageDeduplicationId} (FIFO
 * queue metadata) are requested from the source but land as {@code NULL} whenever the source
 * doesn't actually return them -- confirmed live against sqswire itself, which doesn't track/
 * expose them at all (a real, current limitation of sqswire, not this connector); a real AWS FIFO
 * source that returns them will have them replicate correctly. Dead-letter-queue redrive policy
 * and per-message delay/visibility-timeout state are not translated in v1.
 */
public final class SqsSource implements Source {

    private static final Logger log = LoggerFactory.getLogger(SqsSource.class);
    private static final Pattern SAFE_QUEUE_NAME = Pattern.compile("[^a-zA-Z0-9_]");
    private static final String PARTITION_DONE = "\"DONE\"";
    private static final String CHANGE_FEED_STARTED = "\"STARTED\"";
    private static final int MAX_MESSAGES_PER_POLL = 10;
    private static final int BACKLOG_DRAIN_WAIT_SECONDS = 2;
    private static final int LIVE_POLL_WAIT_SECONDS = 20; // real long-poll -- also close()'s own shutdown latency bound
    private static final int CONSECUTIVE_EMPTY_POLLS_TO_CONSIDER_BACKLOG_DRAINED = 2;

    private final SqsClient sourceClient;
    private final String sourceQueueUrl;
    private final String queueName;
    private final String targetTable;
    private final String checkpointKey;

    private volatile boolean running = true;

    public SqsSource(SqsClient sourceClient, String sourceQueueUrl, String queueName) {
        this.sourceClient = sourceClient;
        this.sourceQueueUrl = sourceQueueUrl;
        this.queueName = queueName;
        this.targetTable = "sqs_queue_" + SAFE_QUEUE_NAME.matcher(queueName).replaceAll("_");
        this.checkpointKey = "sqs:" + queueName;
    }

    @Override
    public void ensureTargetSchema(Sink sink) throws Exception {
        applyTolerantOfConcurrentCreateRace(sink, "CREATE TABLE IF NOT EXISTS sqs_queues_catalog ("
                + "queue_name text PRIMARY KEY, visibility_timeout int NOT NULL DEFAULT 30, "
                + "is_fifo boolean NOT NULL DEFAULT false, dlq_queue_name text, max_receive_count int)");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE TABLE IF NOT EXISTS \"" + targetTable + "\" ("
                + "msg_id bigserial PRIMARY KEY, receipt_handle text, vt timestamptz NOT NULL DEFAULT now(), "
                + "enqueued_at timestamptz NOT NULL DEFAULT now(), read_ct int NOT NULL DEFAULT 0, "
                + "body text NOT NULL, message_group_id text, dedup_id text)");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE INDEX IF NOT EXISTS \"" + targetTable + "_vt_idx\" "
                + "ON \"" + targetTable + "\" (vt)");
        applyTolerantOfConcurrentCreateRace(sink, "CREATE INDEX IF NOT EXISTS \"" + targetTable + "_dedup_idx\" "
                + "ON \"" + targetTable + "\" (dedup_id, enqueued_at)");

        var attrs = sourceClient.getQueueAttributes(r -> r.queueUrl(sourceQueueUrl).attributeNames(QueueAttributeName.ALL))
                .attributes();
        int visibilityTimeout = attrs.containsKey(QueueAttributeName.VISIBILITY_TIMEOUT)
                ? Integer.parseInt(attrs.get(QueueAttributeName.VISIBILITY_TIMEOUT)) : 30;
        boolean isFifo = "true".equalsIgnoreCase(attrs.get(QueueAttributeName.FIFO_QUEUE));

        sink.apply(new ChangeEvent(
                "INSERT INTO sqs_queues_catalog (queue_name, visibility_timeout, is_fifo) VALUES (?, ?::int, ?::boolean) "
                        + "ON CONFLICT (queue_name) DO UPDATE SET visibility_timeout = EXCLUDED.visibility_timeout, "
                        + "is_fifo = EXCLUDED.is_fifo",
                List.of(queueName, String.valueOf(visibilityTimeout), String.valueOf(isFifo))));
    }

    private static void applyTolerantOfConcurrentCreateRace(Sink sink, String ddl) throws Exception {
        try {
            sink.apply(new ChangeEvent(ddl, List.of()));
        } catch (SQLException e) {
            if (!"23505".equals(e.getSQLState())) {
                throw e;
            }
            log.info("ensureTargetSchema: lost a benign concurrent CREATE race to another worker "
                    + "(23505 on the object catalog) -- the object exists either way, continuing");
        }
    }

    /** Always a single partition -- SQS messages have no natural range/hash key to split parallel
     * reads by the way a table's primary key does, and standard queues make no ordering guarantee
     * across messages anyway. */
    @Override
    public List<Partition> listPartitions() {
        return List.of(new Partition(checkpointKey, null));
    }

    /** Drains whatever's currently in the queue -- the "initial migration" of the existing
     * backlog. Stops once {@link #CONSECUTIVE_EMPTY_POLLS_TO_CONSIDER_BACKLOG_DRAINED} polls in a
     * row come back empty, a real, if inherently heuristic, "caught up" signal (a queue has no
     * durable position to know it's truly exhausted the way a table's row count would). {@link
     * #streamChanges} continues with the IDENTICAL drain operation afterward, indefinitely. */
    @Override
    public void readPartition(Partition partition, Sink sink, StateStore checkpoints) throws Exception {
        if (PARTITION_DONE.equals(checkpoints.load(checkpointKey + "#backlog"))) {
            log.info("sqs source[{}]: initial backlog already drained in a prior run -- skipping "
                    + "straight to live draining", checkpointKey);
            return;
        }
        int consecutiveEmpty = 0;
        long drained = 0;
        while (consecutiveEmpty < CONSECUTIVE_EMPTY_POLLS_TO_CONSIDER_BACKLOG_DRAINED) {
            int received = drainOnce(sink, BACKLOG_DRAIN_WAIT_SECONDS);
            drained += received;
            consecutiveEmpty = received == 0 ? consecutiveEmpty + 1 : 0;
        }
        checkpoints.save(checkpointKey + "#backlog", PARTITION_DONE);
        log.info("sqs source[{}]: initial backlog drained, {} message(s) migrated", checkpointKey, drained);
    }

    @Override
    public void prepareChangeFeed(Sink sink, StateStore checkpoints) throws Exception {
        if (checkpoints.load(checkpointKey) == null) {
            // No real resumable position exists for a queue (unlike an LSN/SCN/binlog offset) --
            // this sentinel exists only so Advisor's Data Sync report has something to show for
            // "this source has an active migration," not as a real resume point.
            checkpoints.save(checkpointKey, CHANGE_FEED_STARTED);
        }
    }

    @Override
    public void streamChanges(Sink sink, StateStore checkpoints) throws Exception {
        while (running) {
            drainOnce(sink, LIVE_POLL_WAIT_SECONDS); // the wait itself IS the poll interval -- a
            // real long poll, not a busy loop with a separate sleep
        }
    }

    /** One receive-forward-delete cycle. Requests {@code MessageSystemAttributeName.ALL} so a
     * real FIFO-capable source (unlike sqswire itself, which doesn't track them -- see this
     * class's own javadoc) has its group/dedup metadata actually replicate. Returns how many
     * messages this poll received, so {@link #readPartition}'s backlog-drained heuristic can act
     * on it. */
    private int drainOnce(Sink sink, int waitTimeSeconds) throws Exception {
        List<Message> messages = sourceClient.receiveMessage(r -> r.queueUrl(sourceQueueUrl)
                .maxNumberOfMessages(MAX_MESSAGES_PER_POLL)
                .waitTimeSeconds(waitTimeSeconds)
                .messageSystemAttributeNames(MessageSystemAttributeName.ALL)).messages();
        List<ChangeEvent> batch = new ArrayList<>(messages.size());
        for (Message m : messages) {
            batch.add(insertEvent(m));
        }
        if (!batch.isEmpty()) {
            sink.applyBatch(batch);
        }
        for (Message m : messages) {
            // Only deleted from the source AFTER the target write above succeeded -- see this
            // class's own javadoc on the resulting at-least-once (not exactly-once) guarantee.
            sourceClient.deleteMessage(r -> r.queueUrl(sourceQueueUrl).receiptHandle(m.receiptHandle()));
        }
        return messages.size();
    }

    private ChangeEvent insertEvent(Message m) {
        String groupId = m.attributesAsStrings().get(MessageSystemAttributeName.MESSAGE_GROUP_ID.toString());
        String dedupId = m.attributesAsStrings().get(MessageSystemAttributeName.MESSAGE_DEDUPLICATION_ID.toString());
        StringBuilder sql = new StringBuilder("INSERT INTO \"" + targetTable + "\" (body, message_group_id, dedup_id) VALUES (?, ");
        List<String> params = new ArrayList<>();
        params.add(m.body());
        sql.append(groupId == null ? "NULL" : "?").append(", ").append(dedupId == null ? "NULL" : "?").append(")");
        if (groupId != null) {
            params.add(groupId);
        }
        if (dedupId != null) {
            params.add(dedupId);
        }
        return new ChangeEvent(sql.toString(), params);
    }

    @Override
    public void close() {
        running = false;
    }
}
