CREATE TABLE IF NOT EXISTS main_data.anomalies (
    time        TIMESTAMPTZ      NOT NULL,
    usage_kwh   DOUBLE PRECISION,
    z_score     DOUBLE PRECISION,
    load_type   TEXT,
    detected_at TIMESTAMPTZ      DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS main_data.cost_analysis (
    time         TIMESTAMPTZ      NOT NULL,
    usage_kwh    DOUBLE PRECISION,
    tariff_type  TEXT,
    cost_tl      DOUBLE PRECISION,
    load_type    TEXT
);