
# SteelEnergyHub

SteelEnergyHub is an end-to-end data engineering platform that analyzes real-time energy consumption data in the steel industry, performing anomaly detection and cost optimization.

## Project Architecture

```
Synthetic Data Generator (generate_dummy_data.py)
    ↓
TimescaleDB (main_data.energy_readings) — raw data
    ↓
Feature Pipeline (feature_pipeline.py)
    ↓
TimescaleDB (main_data.processed_readings) — enriched data
    ↓
Kafka Producer (energyProducer.py)
    ↓
Apache Kafka (energy_raw topic)
    ↓
Spark Structured Streaming (energy_streaming.py)
    ↓
TimescaleDB
├── main_data.anomalies         (detected anomalies)
└── main_data.cost_analysis     (tariff-based cost records)
    ↓
Grafana Dashboards
├── Overview                    (management)
├── Energy Analysis             (energy engineers)
├── Anomaly Detection           (maintenance team)
└── Cost Analysis               (finance)
```

## Technology Stack

| Layer | Technology |

| Programming Language | Python 3.11 |

| Stream Processing | Apache Spark 3.5.0 (PySpark) |

| Message Queue | Apache Kafka |

| Database | PostgreSQL 15 + TimescaleDB |

| Visualization | Grafana |

| Infrastructure | Docker & Docker Compose |

| Notebook Environment | Jupyter (via Spark container) |

## Service URLs

| Service | URL |

| Jupyter Notebook | http://localhost:8888 |

| Kafka UI | http://localhost:8090 |

| Grafana | http://localhost:3000 |

| pgAdmin | http://localhost:8085 |

| TimescaleDB | localhost:5432 |

## Quick Start

### Prerequisites

- Docker Desktop (must be running)
- PowerShell (Windows)

### 1. Clone the Repository

```bash
git clone https://github.com/esmabaylan/SteelEnergyHub.git
cd SteelEnergyHub
```

### 2. Start the System

Run the system startup script — this starts all services and opens a terminal for each process:

```powershell
.\run_system.ps1
```

This script will:
- Start all Docker services (Kafka, TimescaleDB, Grafana, pgAdmin, Spark)
- Wait for TimescaleDB and Kafka to be healthy
- Create the `energy_raw` Kafka topic
- Open 4 terminals: dummy generator, feature pipeline, Kafka producer, Spark streaming

### 3. Load the Initial Dataset (first run only)

```powershell
docker exec spark_worker python /home/jovyan/work/scripts/load_data.py
```

### 4. Access the Services

| Service | URL | Credentials |

| Grafana | http://localhost:3000 | admin / admin |

| Kafka UI | http://localhost:8090 | — |

| pgAdmin | http://localhost:8085 | admin@admin.com / admin |

| Jupyter | http://localhost:8888 | — |

### 5. Stop the System

```powershell
docker compose down
```

> **Note:** Use `docker compose down` (without `-v`) to preserve data. Use `docker compose down -v` only if you want to reset everything from scratch.

## Project Structure

```
STEELENERGYHUB
├── database/
│   ├── init/                        # Auto-executed SQL on first startup
│   │   ├── 00_enable_extractions.sql
│   │   ├── 01_create_schemas.sql
│   │   ├── 02_create_roles.sql
│   │   ├── 03_initial_schema.sql
│   │   ├── 04_anomaly_cost_tables.sql
│   │   ├── 05_processed_readings.sql
│   │   └── 06_create_hypertables.sql
│   ├── migrations/                  # Schema version history
│   └── config/                      # PostgreSQL configuration
├── data/
│   └── raw/                         # Raw CSV dataset
├── grafana/
│   ├── dashboards/                  # Dashboard JSON exports
│   └── provisioning/                # Grafana datasource & dashboard config
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   └── 02_feature_engineering.ipynb
├── spark/
│   └── streaming/
│       ├── energy_streaming.py      # Kafka → Spark → DB pipeline
│       └── kafka_consumer.py        # Kafka consumer test
├── src/
│   ├── anomaly/                     # Anomaly detection modules
│   ├── cost/                        # Cost analysis modules
│   ├── pipelines/
│   │   └── feature_pipeline.py      # energy_readings → processed_readings
│   └── producer/
│       ├── energyProducer.py        # processed_readings → Kafka
│       └── generate_dummy_data.py   # Synthetic sensor data generator
├── scripts/
│   └── load_data.py                 # CSV → DB initial loader
├── tests/
│   ├── test_db.py                   # Database connection & schema tests
│   ├── test_dummy_data.py           # Data quality tests
│   ├── test_producer.py             # Kafka producer tests
│   └── test_streaming_job.py        # Spark streaming output tests
├── Dockerfile                       # Spark + Python environment
├── docker-compose.yml               # All services
├── run_system.ps1                   # One-command system startup
├── requirements.txt
└── README.md
```

## Database Schema

```
energydb
└── main_data (schema)
    ├── energy_readings      → raw sensor data (hypertable)
    ├── processed_readings   → feature engineered data (hypertable)
    ├── anomalies            → detected anomalies (hypertable)
    └── cost_analysis        → tariff-based cost records (hypertable)
```

## Database Roles

| Role | Permissions | Used By |

| postgres | superuser | administration |

| data_writer | INSERT, UPDATE, SELECT on main_data | Spark, Python scripts |

| report_reader | SELECT on main_data | Grafana |


## Grafana Dashboards

| Dashboard | Folder | Target Audience |

| Overview | Genel Bakış | Management |

| Energy Analysis | Enerji Analizleri | Energy Engineers |

| Anomaly Detection | Anomali Tespiti | Maintenance Team |

| Cost Analysis | Maliyet Analizi | Finance |

## Connection Details

### TimescaleDB
| Parameter | Value |

| Host (inside Docker) | timescaledb |

| Host (from host machine) | localhost |

| Port | 5432 |

| Database | energydb |

| Write user | data_writer |

| Read user | report_reader |

### Kafka
| Parameter | Value |

| Bootstrap server (inside Docker) | kafka:9092 |

| Topic | energy_raw |

| UI | http://localhost:8090 |

## Running Tests

```powershell
docker exec spark_worker python -m pytest /home/jovyan/work/tests/ -v
```



## Remote Access (Ngrok)

To make the system accessible over the internet, use ngrok.

### Prerequisites

- Download ngrok: `https://ngrok.com/download`
- Extract `ngrok.exe` to `C:\ngrok\`
- Create a free account at `https://ngrok.com` and get your auth token

### Setup (one-time)

```powershell
C:\ngrok\ngrok.exe config add-authtoken YOUR_TOKEN_HERE
```

### Start ngrok

After running `run_system.ps1`, open a new terminal:

```powershell
C:\ngrok\ngrok.exe http 3000
```

You will see a public URL like:
```
Forwarding  https://abc123.ngrok-free.app → localhost:3000
```

Share this URL to allow others to access Grafana remotely.

### Important Notes

- The URL changes every time ngrok restarts (free plan)
- ngrok must stay running for the URL to work
- The system (Docker) must be running before starting ngrok
- Grafana credentials remain the same: admin / admin
```
```

## Developer Notes

- Database tables and hypertables are created automatically on first `docker compose up -d`.
- The `energy_raw` Kafka topic is created automatically by `run_system.ps1`.
- Migration files under `database/migrations/` are for version history only — Docker uses `database/init/`.
- To reset everything: `docker compose down -v` then `docker compose up -d`.
