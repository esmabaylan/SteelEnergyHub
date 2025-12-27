
# SteelEnergyHub

SteelEnergyHub is an end-to-end data engineering project designed to analyze, process, and model energy consumption data within the steel industry.

This project utilizes Apache Spark, PostgreSQL, and Docker technologies, operating within an isolated and scalable development environment (Dev Container).

## Project Architecture and Technology Stack

* **Programming Language:** Python 3.11
* **Data Processing:** Apache Spark 3.5.0 (PySpark)
* **Database:** PostgreSQL 15 (TimescaleDB Image)
* **Infrastructure:** Docker & VS Code Dev Containers
* **Development Environment:** Jupyter Notebooks & Visual Studio Code

## Setup Instructions

Please follow the steps below in order to run the project in your local environment.

### Prerequisites

* Docker Desktop (Must be active/running)
* Visual Studio Code
* VS Code Extension: Dev Containers (Microsoft)

### 1. Cloning the Project

Clone the project to your local directory via the terminal:

```bash
git clone [https://github.com/YOUR_USERNAME/SteelEnergyHub.git](https://github.com/YOUR_USERNAME/SteelEnergyHub.git)
cd SteelEnergyHub

```

### 2. Database Setup (Critical Step)

Before initializing the development environment (Dev Container), the PostgreSQL database must be running externally on Docker. Execute the following command in your terminal to start the database:

```bash
docker run -d \
    --name postgres \
    -p 5435:5432 \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=energydb \
    postgres:15

```

*Note: The database will be exposed on port `5435` on the local machine.*

### 3. Initializing the Development Environment (Dev Container)

1. Open Visual Studio Code.
2. Press `F1` to open the command palette and select **"Dev Containers: Reopen in Container"**.
3. VS Code will automatically build and start the Spark and Python environment defined for the project.

## Project Directory Structure

```text
STEELENERGYHUB
├── .devcontainer/          # Docker image and VS Code environment configuration
│   ├── Dockerfile          # Spark and Python environment definition
│   ├── devcontainer.json   # VS Code configuration file
│   └── requirements.txt    # Project dependencies (PySpark, Psycopg2, etc.)
├── config/                 # Configuration files
├── data/                   # Raw and processed data
├── spark/                  # Spark jobs and source code
├── tests/                  # Test scenarios
│   └── test_db.py          # Database test
└── README.md               # Project documentation

```


## Database Connection Configuration

The following parameters are used to access the external PostgreSQL server (running on Docker Desktop) from within the Dev Container:

* **Host:** `host.docker.internal` (Container gateway)
* **Port:** `5435` (External port mapped on Docker)
* **User:** `postgres`
* **Password:** `postgres`
* **Database Name:** `energydb`

## Developer Notes

If any changes are made to the project dependencies (libraries), the `.devcontainer/requirements.txt` file must be updated, and the **"Rebuild Container"** action must be performed via VS Code to apply the changes.

