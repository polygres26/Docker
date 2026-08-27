-- ### table
CREATE TABLE IF NOT EXISTS sqs_queues_catalog (
    queue_name TEXT PRIMARY KEY,
    visibility_timeout INT NOT NULL DEFAULT 30,
    is_fifo BOOLEAN NOT NULL DEFAULT false,
    dlq_queue_name TEXT,
    max_receive_count INT
)
