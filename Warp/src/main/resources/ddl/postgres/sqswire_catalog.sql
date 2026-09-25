-- ### table
CREATE TABLE IF NOT EXISTS sqs_queues_catalog (
    queue_name TEXT PRIMARY KEY,
    visibility_timeout INT NOT NULL DEFAULT 30,
    is_fifo BOOLEAN NOT NULL DEFAULT false,
    dlq_queue_name TEXT,
    max_receive_count INT
)
-- ### col_attributes_json
ALTER TABLE sqs_queues_catalog ADD COLUMN IF NOT EXISTS attributes_json TEXT
-- ### col_tags_json
ALTER TABLE sqs_queues_catalog ADD COLUMN IF NOT EXISTS tags_json TEXT
-- ### col_created_at
ALTER TABLE sqs_queues_catalog ADD COLUMN IF NOT EXISTS created_at BIGINT
-- ### col_modified_at
ALTER TABLE sqs_queues_catalog ADD COLUMN IF NOT EXISTS modified_at BIGINT
-- ### col_table_name
ALTER TABLE sqs_queues_catalog ADD COLUMN IF NOT EXISTS table_name TEXT
-- ### index_dlq
CREATE INDEX IF NOT EXISTS sqs_queues_catalog_dlq_idx ON sqs_queues_catalog (dlq_queue_name)
-- ### move_tasks
CREATE TABLE IF NOT EXISTS sqs_move_tasks (
    task_handle TEXT PRIMARY KEY,
    source_arn TEXT NOT NULL,
    destination_arn TEXT,
    max_per_second INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    moved BIGINT NOT NULL DEFAULT 0,
    to_move BIGINT NOT NULL DEFAULT 0,
    failure_reason TEXT,
    started_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
)
