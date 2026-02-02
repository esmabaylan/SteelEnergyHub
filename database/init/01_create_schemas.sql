-- schema speration for modularity
-- raw analytics api

-- Ana verilerin duracağı şema
CREATE SCHEMA IF NOT EXISTS main_data;

-- İleride analiz sonuçlarını (maliyet vb.) yazacağın şema
CREATE SCHEMA IF NOT EXISTS analytics;

-- Logları tutacağın şema (Audit logs için)
CREATE SCHEMA IF NOT EXISTS logging;