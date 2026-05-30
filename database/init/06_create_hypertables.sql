SELECT create_hypertable('main_data.energy_readings', 'time', if_not_exists => TRUE);
SELECT create_hypertable('main_data.anomalies', 'time', if_not_exists => TRUE);
SELECT create_hypertable('main_data.cost_analysis', 'time', if_not_exists => TRUE);
SELECT create_hypertable('main_data.processed_readings', 'time', if_not_exists => TRUE);