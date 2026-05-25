SET search_path TO main_data;

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS energy_readings (
    time         TIMESTAMPTZ        NOT NULL,
    usage_kwh    DOUBLE PRECISION,
    lagging_reactive_power  DOUBLE PRECISION,
    leading_reactive_power  DOUBLE PRECISION,
    co2_tco2     DOUBLE PRECISION,
    lagging_pf   DOUBLE PRECISION,
    leading_pf   DOUBLE PRECISION,
    nsm          INTEGER,
    week_status  VARCHAR(10),
    day_of_week  VARCHAR(10),
    load_type    VARCHAR(20)
);

SELECT create_hypertable(
    'main_data.energy_readings', 'time',
    if_not_exists => TRUE
);

CREATE INDEX ON energy_readings (load_type, time DESC);
