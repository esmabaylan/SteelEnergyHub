SET search_path TO main_data;

CREATE TABLE IF NOT EXISTS anomalies (
    time        TIMESTAMPTZ      NOT NULL,
    usage_kwh   DOUBLE PRECISION,
    z_score     DOUBLE PRECISION,
    load_type   TEXT,
    detected_at TIMESTAMPTZ      DEFAULT NOW()
);

SELECT create_hypertable(
    'main_data.anomalies', 'time',
    if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS cost_analysis (
    time         TIMESTAMPTZ      NOT NULL,
    usage_kwh    DOUBLE PRECISION,
    tariff_type  TEXT,
    cost_tl      DOUBLE PRECISION,
    load_type    TEXT
);

SELECT create_hypertable(
    'main_data.cost_analysis', 'time',
    if_not_exists => TRUE
);