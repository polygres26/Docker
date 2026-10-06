-- sqswire: DeleteQueue drops the queue's table (PgQueueStore). There is no dedup table on this engine.
-- ### drop queue table
DROP TABLE IF EXISTS ${table}
