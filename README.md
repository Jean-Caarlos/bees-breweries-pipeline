BEES Breweries — Data Engineering Case

A small ELT pipeline that ingests breweries from the Open Brewery DB API, writes bronze → silver → gold datasets, validates the curated layer with Pandera, and orchestrates everything with Apache Airflow (Docker Compose).

Orchestrator: Airflow 2.10 (LocalExecutor, Postgres metadata)

Language: Python 3.12

Storage: local “data lake” on disk (Parquet)

Tests: pytest + pandera

Containerization: Docker Compose

Optional alerts: Slack / Microsoft Teams webhooks

1) Quick start
# 0) prerequisites: Docker Desktop + Docker Compose v2
# 1) create the .env file at the repo root (see section "Config")
# 2) start Postgres
docker compose up -d postgres

# 3) initialize Airflow metadata & admin user
docker compose run --rm airflow-init

# 4) start Airflow webserver + scheduler
docker compose up -d airflow-webserver airflow-scheduler

# 5) open http://localhost:8080  (user: admin / pass: admin)
# 6) trigger the DAG
docker compose exec airflow-webserver airflow dags trigger bees_breweries

# 7) watch logs
docker compose logs -f airflow-scheduler


Outputs go to:

Silver: data/silver/ingestion_date=YYYY-MM-DD/country=.../state=.../*.parquet

Gold: data/gold/run_date=YYYY-MM-DD/breweries_by_type_and_location_HHMMSS.parquet

2) Project layout
.
├─ dags/
│  └─ bees_breweries.py               # DAG with callbacks, retries, scheduling
├─ src/
│  ├─ extract_bronze.py               # API calls + raw persistence
│  ├─ transform_silver.py             # normalization + partitioned parquet
│  ├─ transform_gold.py               # grouped aggregation to gold
│  └─ quality.py                      # Pandera schema & checks
├─ tests/
│  ├─ test_quality_schema.py
│  └─ test_transform_silver.py
├─ data/                              # lake (mounted into containers)
│  ├─ bronze/
│  ├─ silver/
│  └─ gold/
├─ requirements.txt
├─ docker-compose.yml
├─ .env.example
└─ README.md

3) Config

Create a .env file at the repo root:

# Airflow encryption key (required). Single line, no quotes.
AIRFLOW_FERNET_KEY=PUT_YOUR_FERNET_KEY_HERE

# Optional: alerts
ALERT_SLACK_WEBHOOK_URL=
ALERT_TEAMS_WEBHOOK_URL=

# Optional but useful for links in alerts
AIRFLOW__WEBSERVER__BASE_URL=http://localhost:8080


Generate a Fernet key (either way):

# host (if you have python)
python - << 'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY

# or inside an Airflow container
docker compose run --rm airflow-webserver python - << 'PY'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY


Notes (Windows): save .env with LF line endings; no quotes or trailing spaces.

4) What the pipeline does

High level:

Bronze  -> raw API payload (optional on disk)
Silver  -> curated parquet (partitioned): ingestion_date / country / state
Gold    -> aggregated parquet: qty by country/state/brewery_type/city


DAG: bees_breweries

ingest_bronze — calls Open Brewery DB (paginated), captures raw list.

build_silver — normalizes field names/types, filters malformed records, and writes partitioned Parquet:

data/silver/ingestion_date=YYYY-MM-DD/country=.../state=.../part-*.parquet


quality_checks — enforces a Pandera schema (non-null keys, dtype checks, and domain checks for brewery_type aligned to the API’s current taxonomy; if new values appear, the check will fail and surface in alerts).

build_gold — reads the full Silver dataset (forcing string schema to avoid Arrow null-casts), groups by (country, state, brewery_type, city), and writes:

data/gold/run_date=YYYY-MM-DD/breweries_by_type_and_location_HHMMSS.parquet


Scheduling / reliability (in DAG):

retries=3 with exponential backoff on all Python tasks

sensible dagrun_timeout

callbacks for on_success / on_failure (Slack/Teams if configured)

idempotent writes (timestamped file names in Gold, partitioned paths in Silver)

5) Tests

Run the test suite inside the Airflow image so dependencies & paths match:

docker compose run --rm airflow-webserver bash -lc "pytest -q /opt/airflow/tests"


What’s covered:

Schema: SilverSchema checks for required columns/dtypes and allowed domain for brewery_type.

Silver writer: ensures partitioned layout & Parquet writing behavior match expectations.

6) Monitoring & alerting

Pipeline health: Airflow UI + task retries; failures trigger on_failure_callback, posting to Slack/Teams if ALERT_*_WEBHOOK_URL is set.

Data quality: quality_checks fails the run when the Pandera schema is violated (missing fields, bad types, unexpected brewery types, etc.), so issues are visible in the DAG and alerts.

Links to logs: alerts include the Airflow run URL when AIRFLOW__WEBSERVER__BASE_URL is set.

(For production, I’d add SLAs, metrics (Prometheus/Grafana), and a dead-letter path for rejected records.)

7) Design choices & trade-offs

Airflow over Luigi/Mage for familiarity, rich scheduling, and built-in retries/alerts.

LocalExecutor + Postgres (no SQLite) to reflect real-world behavior while keeping the stack lightweight.

Parquet for Silver/Gold due to compression + columnar scans; partitioning by ingestion_date/country/state to speed up reads by location.

Pandera as a simple, explicit schema gate that fails fast and is easy to test.

Simple webhooks for alerts to keep infra minimal (no extra brokers/services).

Local “lake” to keep the case self-contained; cloud mounting instructions could be added if needed.

8) Troubleshooting

“cannot use SQLite with the LocalExecutor”
Ensure AIRFLOW__DATABASE__SQL_ALCHEMY_CONN points to Postgres (see docker-compose.yml) and that you ran docker compose run --rm airflow-init.

“Fernet key must be 32 url-safe base64-encoded bytes”
Regenerate the key and put it in .env (one line, no quotes). Recreate services.

Arrow ‘Unsupported cast from string to null’ while reading Silver
This happens when Arrow infers null for a column; Gold explicitly forces string schema when scanning Silver.

Pandera failures for brewery_type
Either update the allowed set to match the API’s current values or handle normalization before validation.

9) Extending to cloud (optional)

Mount S3/GCS as an external volume or switch to s3://.../gs://... paths with proper credentials (do not commit creds).

For production, I’d move data to object storage and keep only Airflow’s metadata in Postgres.

10) Cleaning up
docker compose down
# remove local volumes (Postgres data etc.)
docker compose down -v

11) License

Use any license you prefer (MIT/Apache-2.0). Add a LICENSE file if needed.

12) Acknowledgments

Open Brewery DB — public dataset used in this exercise.

Appendix — Commands I use often
# list services
docker compose ps

# follow scheduler logs
docker compose logs -f airflow-scheduler

# clear old runs

docker compose exec airflow-webserver airflow tasks clear -y bees_breweries

# backfill a range
docker compose exec airflow-webserver \
  airflow dags backfill bees_breweries -s 2025-08-25 -e 2025-09-01


# Prints de tela:    
airflow:
- DAG success - <img width="1890" height="841" alt="image" src="https://github.com/user-attachments/assets/181906d8-76c0-4605-95d2-c19c2cc68e6b" />
- DAG graph - <img width="1899" height="916" alt="image" src="https://github.com/user-attachments/assets/7524079a-44c6-4b03-844c-d6876e65ad4d" />
- DAG - <img width="1906" height="454" alt="image" src="https://github.com/user-attachments/assets/cd83cc99-0f5a-4d85-8a47-75ae095f1baa" />

VSCODE - <img width="1327" height="1020" alt="image" src="https://github.com/user-attachments/assets/d8fe2fdf-3ca8-4907-970e-70b00d71e633" />
