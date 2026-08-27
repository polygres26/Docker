-- ### table
CREATE TABLE IF NOT EXISTS _sqs_queues (
    queue_name TEXT PRIMARY KEY,
    visibility_timeout INT NOT NULL DEFAULT 30,
    is_fifo BOOLEAN NOT NULL DEFAULT false,
    dlq_queue_name TEXT,
    max_receive_count INT
)
