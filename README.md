# Distributed ETL Orchestrator

A robust, distributed Extract, Transform, Load (ETL) orchestrator designed to move data from CSV files and external APIs into **Supabase (PostgreSQL)**. 

This project features automated CSV profiling, data type inference, dynamic SQL DDL generation, and is built to be deployed on **AWS ECS/Fargate** using **Prefect** for workflow orchestration. It also includes a native Tkinter GUI for easy local testing and execution.

## 🚀 Features

- **Intelligent CSV Profiler:** Automatically scans CSV files to infer PostgreSQL data types, proposes primary keys, and generates `CREATE TABLE` statements.
- **Multi-Source ETL Flows:** 
  - `run-csv`: Ingest local or S3-hosted CSVs directly into Supabase.
  - `run-api`: Fetch data from external APIs via JSON configuration and load it into the database.
- **Cloud-Ready Orchestration:** Includes Dockerfiles and documentation to deploy long-running Prefect workers on AWS ECS/Fargate, fetching secrets securely from AWS Secrets Manager.
- **Local GUI Launcher:** A built-in Tkinter UI to easily pick files, configure API headers, set database credentials, and trigger pipeline runs locally.

## 🛠️ Tech Stack

- **Language:** Python 3
- **Database:** Supabase / PostgreSQL
- **Orchestration:** Prefect (Cloud / Self-hosted)
- **Cloud / Deployment:** Docker, AWS ECR, AWS ECS (Fargate), AWS Secrets Manager
- **UI:** Tkinter (Standard Library)

## ⚙️ Setup & Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/MabasaBee603163/Distributed-ETL-Orchestrator.git
   cd Distributed-ETL-Orchestrator
   ```

2. **Configure Environment Variables:**
   Copy `.env.example` to `.env` and fill in your Supabase and Prefect details:
   ```env
   SUPABASE_URL=your_supabase_url
   SUPABASE_KEY=your_service_role_key
   SUPABASE_DB_URL=your_postgres_connection_string
   ```

## 💻 Usage

### 1. Local GUI Mode (Easiest)
Launch the interactive desktop application to profile data or run ETL jobs without touching the CLI:
```bash
python main.py gui
```

### 2. Command Line Interface (CLI)
**Profile a CSV and generate a schema:**
```bash
python main.py profile --pick
```
*This will generate a JSON report, a Markdown schema proposal, and a drafted `.sql` file in the `/profiles` directory.*

**Run a CSV to Supabase pipeline:**
```bash
python main.py run-csv --pick-csv --supabase-table target_table
```

## ☁️ Deployment (AWS ECS / Fargate)

This application is designed to run as a distributed Prefect worker on AWS. 

1. Build the worker image using `docker/Dockerfile.worker`.
2. Push the image to AWS ECR.
3. Deploy as an ECS/Fargate task polling your Prefect Work Pool.

*(See `deploy/AWS-FARGATE-WORKER.txt` for detailed infrastructure setup instructions).*