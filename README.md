
# SteelEnergyHub

SteelEnergyHub is an end-to-end data engineering platform that analyzes real-time energy consumption data in the steel industry, performing anomaly detection and cost optimization.

## Project Architecture

```
CSV Data
    ↓
TimescaleDB (main_data.energy_readings)
    ↓
Kafka Producer (energy_producer.py)
    ↓
Apache Kafka (energy_raw topic)
    ↓
Spark Structured Streaming (energy_streaming.py)
    ↓
TimescaleDB
├── main_data.anomalies       (detected anomalies)
└── main_data.cost_analysis   (tariff-based cost records)
    ↓
Grafana Dashboard
```

## Technology Stack

| Layer | Technology |

| Programming Language | Python 3.11 |

| Stream Processing | Apache Spark 3.5.0 (PySpark) |

| Message Queue | Apache Kafka |

| Database | PostgreSQL 15 + TimescaleDB |

| Visualization | Grafana |

| Infrastructure | Docker & Docker Compose |

| Development Environment | VS Code Dev Containers + Jupyter |

## Service URLs

| Service | URL |

| Jupyter Notebook | http://localhost:8888 |

| Kafka UI | http://localhost:8090 |

| Grafana | http://localhost:3000 |

| pgAdmin | http://localhost:8085 |

| TimescaleDB | localhost:5432 |

## Setup

### Prerequisites

- Docker Desktop (must be running)
- Visual Studio Code
- VS Code Extension: Dev Containers (Microsoft)

### 1. Clone the Repository

```bash
git clone https://github.com/esmabaylan/SteelEnergyHub.git
cd SteelEnergyHub
```

### 2. Start Core Services

Run the main `docker-compose.yml` to start Kafka, TimescaleDB, pgAdmin, and Grafana:

```bash
docker compose up -d
```

Verify all services are healthy:

```bash
docker compose ps
```

The `timescaledb` row should show `healthy`.

### 3. Open the Development Environment

In VS Code, press `Ctrl+Shift+P` → select **"Dev Containers: Reopen in Container"**.

VS Code will automatically build and start the Spark and Python environment.

> **Important:** Always run `docker compose up -d` before opening the Dev Container. The Dev Container connects to the `steelenergy_network` created by the main compose file.

### 4. Load the Dataset

In the Dev Container terminal:

```bash
python scripts/load_data.py
```

### 5. Start the Kafka Producer

```bash
python src/producer/energy_producer.py
```

### 6. Start Spark Streaming

Open a new terminal:

```bash
python spark/streaming/energy_streaming.py
```

## Project Structure

```
STEELENERGYHUB
├── .devcontainer/
│   ├── Dockerfile              # Spark + Python environment
│   ├── devcontainer.json       # VS Code configuration
│   └── docker-compose.yml      # Spark service
├── config/                     # Configuration files
├── data/
│   └── raw/                    # Raw CSV data
├── database/
│   ├── init/                   # Docker init SQL files
│   ├── migrations/             # Schema versions
│   └── config/                 # PostgreSQL configuration
├── notebooks/
│   └── 01_data_exploration.ipynb
├── spark/
│   └── streaming/
│       └── energy_streaming.py # Kafka → Spark → DB pipeline
├── src/
│   └── producer/
│       └── energy_producer.py  # DB → Kafka producer
├── scripts/
│   └── load_data.py            # CSV → DB loader
├── tests/
│   └── test_db.py
├── docker-compose.yml          # Core services
└── README.md
```

## Database Schema

```
energydb
└── main_data (schema)
    ├── energy_readings   → raw energy data (hypertable)
    ├── anomalies         → detected anomalies (hypertable)
    └── cost_analysis     → tariff-based cost records (hypertable)
```

## Connection Details

### TimescaleDB
| Parameter | Value |

| Host (inside Docker) | timescaledb |

| Host (from host machine) | localhost |

| Port | 5432 |

| Database | energydb |

| Write user | data_writer |

| Read user | report_reader |

### Grafana
| Parameter | Value |

| URL | http://localhost:3000 |

| Username | admin |

| Datasource | PostgreSQL → timescaledb:5432 |

## Developer Notes

- If dependencies change, update `.devcontainer/Dockerfile` and run **"Rebuild Container"**.
- Migration files are versioned under `database/migrations/` in `V001__`, `V002__` format.
- Core services must always be started before opening the Dev Container.