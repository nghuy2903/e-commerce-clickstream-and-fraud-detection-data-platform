-- Banking Fraud Detection — Operational & Serving DB (PostgreSQL 15+)
-- Chạy tự động qua /docker-entrypoint-initdb.d khi volume postgres_data mới.
-- Flink (real-time): INSERT transactions, UPDATE accounts, INSERT fraud_alerts
-- Web API: SELECT accounts theo user_id, SELECT fraud_alerts mới nhất

BEGIN;

-- ---------------------------------------------------------------------------
-- Types
-- ---------------------------------------------------------------------------
CREATE TYPE account_status AS ENUM (
    'ACTIVE',
    'WARNING',
    'LOCKED',
    'SUSPENDED'
);

CREATE TYPE fraud_risk_level AS ENUM (
    'LOW',
    'MEDIUM',
    'HIGH',
    'CRITICAL'
);

-- ---------------------------------------------------------------------------
-- users — thông tin user cơ bản (đồng bộ từ event.user_id lần đầu)
-- ---------------------------------------------------------------------------
CREATE TABLE users (
    user_id         VARCHAR(64)  NOT NULL,
    display_name    VARCHAR(255),
    email           VARCHAR(255),
    phone           VARCHAR(32),
    is_simulated    BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_users PRIMARY KEY (user_id)
);

COMMENT ON TABLE users IS 'Danh mục người dùng; Flink upsert khi gặp user_id mới từ Kafka.';

-- ---------------------------------------------------------------------------
-- accounts — số dư & trạng thái phục vụ UI / Flink cập nhật real-time
-- ---------------------------------------------------------------------------
CREATE TABLE accounts (
    account_id          UUID            NOT NULL DEFAULT gen_random_uuid(),
    user_id             VARCHAR(64)     NOT NULL,
    account_number      VARCHAR(32)     NOT NULL,
    currency            CHAR(3)         NOT NULL DEFAULT 'VND',
    balance             NUMERIC(18, 2)  NOT NULL DEFAULT 0,
    status              account_status  NOT NULL DEFAULT 'ACTIVE',
    status_reason       TEXT,
    last_event_id       UUID,
    last_activity_at    TIMESTAMPTZ,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_accounts PRIMARY KEY (account_id),
    CONSTRAINT fk_accounts_user FOREIGN KEY (user_id)
        REFERENCES users (user_id) ON DELETE RESTRICT,
    CONSTRAINT uq_accounts_number UNIQUE (account_number),
    CONSTRAINT chk_accounts_balance_non_negative CHECK (balance >= 0)
);

COMMENT ON TABLE accounts IS 'Một user có thể có nhiều tài khoản; Flink UPDATE theo account_id.';

-- Mỗi user một tài khoản chính (phổ biến trong demo) — tránh duplicate khi upsert Flink
CREATE UNIQUE INDEX uq_accounts_primary_per_user
    ON accounts (user_id);

-- Flink / API: tra cứu nhanh theo user_id (UI dashboard)
CREATE INDEX idx_accounts_user_status
    ON accounts (user_id, status);

-- Flink: cập nhật balance theo khóa chính (clustered PK đủ; thêm cho filter theo trạng thái)
CREATE INDEX idx_accounts_status_updated
    ON accounts (status, updated_at DESC);

-- ---------------------------------------------------------------------------
-- transactions — chi tiết giao dịch ánh xạ từ banking_events (event_id = PK)
-- ---------------------------------------------------------------------------
CREATE TABLE transactions (
    transaction_id      UUID            NOT NULL,
    account_id          UUID            NOT NULL,
    user_id             VARCHAR(64)     NOT NULL,
    event_type          VARCHAR(32)     NOT NULL,
    amount              NUMERIC(18, 2)  NOT NULL DEFAULT 0,
    currency            CHAR(3)         NOT NULL DEFAULT 'VND',
    ip_address          INET            NOT NULL,
    is_simulated        BOOLEAN         NOT NULL DEFAULT FALSE,
    event_timestamp     TIMESTAMPTZ     NOT NULL,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_transactions PRIMARY KEY (transaction_id),
    CONSTRAINT fk_transactions_account FOREIGN KEY (account_id)
        REFERENCES accounts (account_id) ON DELETE RESTRICT,
    CONSTRAINT fk_transactions_user FOREIGN KEY (user_id)
        REFERENCES users (user_id) ON DELETE RESTRICT,
    CONSTRAINT chk_transactions_amount_non_negative CHECK (amount >= 0)
);

COMMENT ON TABLE transactions IS 'transaction_id = event_id từ Kafka; INSERT idempotent (ON CONFLICT DO NOTHING).';

-- Flink INSERT: chỉ cần PK; thêm index phục vụ API lịch sử giao dịch theo tài khoản
CREATE INDEX idx_transactions_account_time
    ON transactions (account_id, event_timestamp DESC);

CREATE INDEX idx_transactions_user_time
    ON transactions (user_id, event_timestamp DESC);

-- Phân tích theo loại sự kiện trong cửa sổ thời gian (Flink enrichment / API)
CREATE INDEX idx_transactions_user_type_time
    ON transactions (user_id, event_type, event_timestamp DESC);

-- ---------------------------------------------------------------------------
-- fraud_alerts — cảnh báo từ Flink scoring
-- ---------------------------------------------------------------------------
CREATE TABLE fraud_alerts (
    alert_id            UUID                NOT NULL DEFAULT gen_random_uuid(),
    user_id             VARCHAR(64)         NOT NULL,
    account_id          UUID,
    transaction_id      UUID,
    source_event_id     UUID,
    risk_score          NUMERIC(5, 4)       NOT NULL,
    risk_level          fraud_risk_level    NOT NULL,
    rule_name           VARCHAR(128)        NOT NULL,
    alert_message       TEXT,
    is_acknowledged     BOOLEAN             NOT NULL DEFAULT FALSE,
    detected_at         TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_fraud_alerts PRIMARY KEY (alert_id),
    CONSTRAINT fk_fraud_alerts_user FOREIGN KEY (user_id)
        REFERENCES users (user_id) ON DELETE RESTRICT,
    CONSTRAINT fk_fraud_alerts_account FOREIGN KEY (account_id)
        REFERENCES accounts (account_id) ON DELETE SET NULL,
    CONSTRAINT fk_fraud_alerts_transaction FOREIGN KEY (transaction_id)
        REFERENCES transactions (transaction_id) ON DELETE SET NULL,
    CONSTRAINT chk_fraud_alerts_risk_score CHECK (risk_score >= 0 AND risk_score <= 1)
);

COMMENT ON TABLE fraud_alerts IS 'Flink INSERT sau khi chấm điểm; transaction_id có thể NULL nếu alert trước khi commit txn.';

-- Web API: quét cảnh báo mới nhất của user (polling / WebSocket)
CREATE INDEX idx_fraud_alerts_user_detected
    ON fraud_alerts (user_id, detected_at DESC);

-- Chỉ các alert chưa xác nhận — partial index nhỏ, đọc real-time nhanh
CREATE INDEX idx_fraud_alerts_user_unacked
    ON fraud_alerts (user_id, detected_at DESC)
    WHERE is_acknowledged = FALSE;

CREATE INDEX idx_fraud_alerts_account_detected
    ON fraud_alerts (account_id, detected_at DESC)
    WHERE account_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Trigger: tự động cập nhật updated_at (giảm logic trùng lặp ở Flink/API)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

CREATE TRIGGER trg_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

COMMIT;
