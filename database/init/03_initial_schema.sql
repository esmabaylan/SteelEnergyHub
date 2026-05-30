CREATE TABLE IF NOT EXISTS main_data.energy_readings (
    time                    TIMESTAMPTZ      NOT NULL,
    usage_kwh               DOUBLE PRECISION,
    lagging_reactive_power  DOUBLE PRECISION,
    leading_reactive_power  DOUBLE PRECISION,
    co2_tco2                DOUBLE PRECISION,
    lagging_pf              DOUBLE PRECISION,
    leading_pf              DOUBLE PRECISION,
    nsm                     INTEGER,
    week_status             TEXT,
    day_of_week             TEXT,
    load_type               TEXT
);