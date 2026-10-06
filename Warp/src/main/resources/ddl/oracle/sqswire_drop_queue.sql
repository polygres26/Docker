-- sqswire: DeleteQueue drops the queue's table (PgQueueStore). Oracle before 23c has no DROP TABLE IF EXISTS: a plain DROP TABLE raises ORA-00942
-- for a table that is already gone, which PgQueueStore treats as success. There is no dedup table on this engine.
-- ### drop queue table
DROP TABLE ${table}
