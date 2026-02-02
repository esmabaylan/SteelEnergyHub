-- 1. Spark uygulaması veya Python scripti için veri YAZAN kullanıcı
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'data_writer') THEN
        CREATE ROLE data_writer WITH LOGIN PASSWORD 'sifre123';
    END IF;
END
$$;

-- Yazara şema kullanım yetkisi ver
GRANT USAGE ON SCHEMA main_data TO data_writer;
GRANT INSERT, UPDATE, SELECT ON ALL TABLES IN SCHEMA main_data TO data_writer;

-- 2. Grafana veya Raporlama için sadece OKUYAN kullanıcı
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'report_reader') THEN
        CREATE ROLE report_reader WITH LOGIN PASSWORD 'sifre456';
    END IF;
END
$$;

-- Okuyucuya sadece SELECT yetkisi ver (Veriyi bozamaz)
GRANT USAGE ON SCHEMA analytics TO report_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO report_reader;

-- Gelecekte oluşturulacak tablolar için varsayılan yetkiler
ALTER DEFAULT PRIVILEGES IN SCHEMA main_data
GRANT INSERT, UPDATE, SELECT ON TABLES TO data_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
GRANT SELECT ON TABLES TO report_reader;
