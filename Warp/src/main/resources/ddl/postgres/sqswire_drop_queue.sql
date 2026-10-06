-- sqswire: DeleteQueue drops the queue's table and, on Postgres, its dedup table (PgQueueStore).
-- ### drop queue table
DROP TABLE IF EXISTS ${table}
-- ### drop dedup table
DROP TABLE IF EXISTS ${dedup_table}
