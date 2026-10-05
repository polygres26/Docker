-- Statements that failed translation or execution, kept for review (config.FailedStatementLog). Postgres only (control plane).
-- ### table
CREATE TABLE IF NOT EXISTS warp_failed_statements (
    id                    bigserial PRIMARY KEY,
    occurred_at           timestamptz NOT NULL DEFAULT now(),
    dialect               text NOT NULL,
    sql_text              text NOT NULL,
    failure_type          text NOT NULL,
    sql_state             text,
    native_error_returned integer,
    message               text
)
