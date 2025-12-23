### SteelEnergyHub

SteelEnergyHub is a smart energy management platform for steel factories, supporting real-time monitoring, anomaly detection, and consumption forecasting. The project leverages Kafka, Spark Structured Streaming, Docker, PostgreSQL, and visualization tools like Streamlit or Grafana.

## Features

Real-time energy data collection and processing

Anomaly detection to identify irregular energy usage

Energy consumption prediction and analytics

Dashboard visualization (Grafana / Streamlit)


## System Requirements

Docker

Docker Compose

Python 3.11+


## Docker Images

The platform’s core components can be run using the following Docker images:

PgAdmin:

docker pull dpage/pgadmin4:snapshot


PostgreSQL:

docker pull postgres:16.11-alpine3.23


Apache Spark Notebook:

docker pull jupyter/all-spark-notebook:x86_64-python-3.11

## Installation

Clone the repository:

git clone <repository-url>
cd SteelEnergyHub

Start all services using Docker Compose:

docker compose up -d


Configure PgAdmin and PostgreSQL connections.

Start the Spark pipeline and monitor data flow.

Launch the dashboard (Grafana/Streamlit) for visualization.

Project Structure
SteelEnergyHub/
│
├─ docker/                 # Docker Compose and service configurations
├─ spark/                  # Spark Structured Streaming scripts
├─ postgres/               # PostgreSQL configurations
├─ dashboards/             # Grafana / Streamlit dashboards
├─ requirements.txt        # Python dependencies
└─ README.md

Contribution

Pull requests are welcome.

All contributions should adhere to code quality standards and the existing architecture.

License

This project is licensed under the MIT License.