-- awsparams store (StoreType.AWSPARAMS): the state of Secrets Manager, SSM Parameter Store, KMS and STS, which share one store
-- because they share key management (secrets and SecureString values are sealed with KMS keys). Placement:
--   * secrets and their versions: on the host owning hash(secret name)
--   * SSM parameters and their history: on the host owning hash(parameter name)
--   * KMS keys and their grants and import tokens: on the host owning hash(key id); KMS aliases, STS sessions and SSM
--     run-command records: on the first host of the set (the "home")
-- ### kms_keys
CREATE TABLE IF NOT EXISTS warp_awsparams_kms_keys (
    key_id          TEXT COLLATE "C" PRIMARY KEY,
    arn             TEXT NOT NULL,
    spec            TEXT NOT NULL,
    usage           TEXT NOT NULL,
    origin          TEXT NOT NULL DEFAULT 'AWS_KMS',
    state           TEXT NOT NULL DEFAULT 'Enabled',
    description     TEXT NOT NULL DEFAULT '',
    policy          TEXT NOT NULL,
    tags            JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    deletion_date   TIMESTAMPTZ,
    rotation_on     BOOLEAN NOT NULL DEFAULT false,
    rotation_days   INT NOT NULL DEFAULT 365,
    rotations       JSONB NOT NULL DEFAULT '[]',
    key_manager     TEXT NOT NULL DEFAULT 'CUSTOMER',
    multi_region    BOOLEAN NOT NULL DEFAULT false,
    material        BYTEA,
    public_key      BYTEA,
    expiration_model TEXT,
    valid_to        TIMESTAMPTZ
);
-- ### kms_aliases
CREATE TABLE IF NOT EXISTS warp_awsparams_kms_aliases (
    name       TEXT COLLATE "C" PRIMARY KEY,
    key_id     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### kms_grants
CREATE TABLE IF NOT EXISTS warp_awsparams_kms_grants (
    grant_id   TEXT COLLATE "C" PRIMARY KEY,
    key_id     TEXT COLLATE "C" NOT NULL,
    name       TEXT,
    grantee    TEXT NOT NULL,
    retiring   TEXT,
    operations JSONB NOT NULL,
    constraints JSONB,
    token      TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### kms_grants_key_idx
CREATE INDEX IF NOT EXISTS warp_awsparams_kms_grants_key_idx ON warp_awsparams_kms_grants (key_id);
-- ### kms_import_tokens
CREATE TABLE IF NOT EXISTS warp_awsparams_kms_import_tokens (
    token       TEXT COLLATE "C" PRIMARY KEY,
    key_id      TEXT NOT NULL,
    private_key BYTEA NOT NULL,
    valid_until TIMESTAMPTZ NOT NULL
);
-- ### secrets
CREATE TABLE IF NOT EXISTS warp_awsparams_secrets (
    name          TEXT COLLATE "C" PRIMARY KEY,
    arn           TEXT NOT NULL,
    description   TEXT NOT NULL DEFAULT '',
    kms_key_id    TEXT,
    tags          JSONB NOT NULL DEFAULT '{}',
    policy        TEXT,
    rotation_lambda TEXT,
    rotation_rules  JSONB,
    rotation_enabled BOOLEAN NOT NULL DEFAULT false,
    last_rotated  TIMESTAMPTZ,
    last_changed  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_accessed TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at    TIMESTAMPTZ,
    delete_after  TIMESTAMPTZ,
    replicas      JSONB NOT NULL DEFAULT '[]',
    owning_service TEXT
);
-- ### secret_versions
CREATE TABLE IF NOT EXISTS warp_awsparams_secret_versions (
    name        TEXT COLLATE "C" NOT NULL,
    version_id  TEXT COLLATE "C" NOT NULL,
    stages      JSONB NOT NULL DEFAULT '[]',
    secret_string BYTEA,
    secret_binary BYTEA,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_accessed TIMESTAMPTZ,
    PRIMARY KEY (name, version_id)
);
-- ### ssm_params
CREATE TABLE IF NOT EXISTS warp_awsparams_ssm_params (
    name        TEXT COLLATE "C" PRIMARY KEY,
    type        TEXT NOT NULL,
    version     BIGINT NOT NULL DEFAULT 1,
    value       BYTEA NOT NULL,
    key_id      TEXT,
    description TEXT,
    data_type   TEXT NOT NULL DEFAULT 'text',
    tier        TEXT NOT NULL DEFAULT 'Standard',
    policies    TEXT,
    allowed_pattern TEXT,
    labels      JSONB NOT NULL DEFAULT '{}',
    tags        JSONB NOT NULL DEFAULT '{}',
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_by TEXT
);
-- ### ssm_history
CREATE TABLE IF NOT EXISTS warp_awsparams_ssm_history (
    name        TEXT COLLATE "C" NOT NULL,
    version     BIGINT NOT NULL,
    type        TEXT NOT NULL,
    value       BYTEA NOT NULL,
    key_id      TEXT,
    description TEXT,
    data_type   TEXT NOT NULL DEFAULT 'text',
    tier        TEXT NOT NULL DEFAULT 'Standard',
    policies    TEXT,
    allowed_pattern TEXT,
    labels      JSONB NOT NULL DEFAULT '[]',
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_by TEXT,
    PRIMARY KEY (name, version)
);
-- ### ssm_commands
CREATE TABLE IF NOT EXISTS warp_awsparams_ssm_commands (
    command_id    TEXT COLLATE "C" NOT NULL,
    instance_id   TEXT COLLATE "C" NOT NULL,
    document_name TEXT NOT NULL,
    comment       TEXT,
    parameters    JSONB NOT NULL DEFAULT '{}',
    status        TEXT NOT NULL DEFAULT 'Pending',
    command_status TEXT NOT NULL DEFAULT 'InProgress',
    timeout_seconds INT NOT NULL DEFAULT 3600,
    requested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (command_id, instance_id)
);
-- ### sts_sessions
CREATE TABLE IF NOT EXISTS warp_awsparams_sts_sessions (
    access_key   TEXT COLLATE "C" PRIMARY KEY,
    secret       TEXT NOT NULL,
    token        TEXT NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    principal    TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### iam_roles
CREATE TABLE IF NOT EXISTS warp_awsparams_iam_roles (
    name        TEXT COLLATE "C" PRIMARY KEY,
    arn         TEXT NOT NULL,
    role_id     TEXT NOT NULL,
    path        TEXT NOT NULL DEFAULT '/',
    trust_policy TEXT NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- ### iam_saml
CREATE TABLE IF NOT EXISTS warp_awsparams_iam_saml (
    name       TEXT COLLATE "C" PRIMARY KEY,
    arn        TEXT NOT NULL,
    metadata   TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
