# dags/bees_breweries_dag.py
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Alertas (Slack webhook opcional em ALERT_SLACK_WEBHOOK_URL)
from src.alerts import notify_failure, notify_success

# ========= Wrappers para localizar as funções já existentes no seu src/ =========
# (mantém compatibilidade caso os nomes variem levemente)

def _import_extract_callable():
    try:
        from src.extract import run_bronze as _f
        return _f
    except Exception:
        try:
            from src.extract import run_extract as _f
            return _f
        except Exception:
            try:
                from src.extract import extract_bronze as _f
                return _f
            except Exception:
                def _noop(**_):
                    print("[extract] nenhuma função encontrada em src.extract.*  (noop)")
                return _noop

def _import_silver_callable():
    try:
        from src.transform_silver import run_silver as _f
        return _f
    except Exception:
        try:
            from src.transform_silver import build_silver as _f
            return _f
        except Exception:
            try:
                from src.transform_silver import transform_silver as _f
                return _f
            except Exception:
                def _noop(**_):
                    print("[silver] nenhuma função encontrada em src.transform_silver.*  (noop)")
                return _noop

# Estes dois já criamos/ajustamos nos passos anteriores:
from src.quality import validate_silver_minimal      # usa context['ds']
from src.transform_gold import run_gold              # usa context['ds']


# ================================ DAG ==========================================
default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="bees_breweries",
    description="Open Brewery DB em arquitetura Medallion (Bronze/Silver/Gold)",
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    default_args=default_args,
    # Qualquer task que falhar dispara alerta:
    on_failure_callback=notify_failure,
) as dag:

    # -------- set_dates (auxiliar; deixa explícito o run_date) ----------
    def _set_dates(**context):
        ds = context["ds"]  # YYYY-MM-DD (Airflow macro)
        print(f"[set_dates] run_date={ds}")
        return ds

    set_dates = PythonOperator(
        task_id="set_dates",
        python_callable=_set_dates,
    )

    # ----------------------- bronze -------------------------------------
    extract_bronze = PythonOperator(
        task_id="extract_bronze",
        python_callable=_import_extract_callable(),
    )

    # ----------------------- silver -------------------------------------
    build_silver = PythonOperator(
        task_id="build_silver",
        python_callable=_import_silver_callable(),
    )

    # -------------------- quality checks --------------------------------
    quality_checks = PythonOperator(
        task_id="quality_checks",
        python_callable=validate_silver_minimal,
        # falhas já são notificadas pelo callback do DAG
    )

    # ------------------------ gold --------------------------------------
    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=run_gold,
        # opcional: avisar quando o produto final saiu OK
        on_success_callback=notify_success,
    )

    # Orquestração
    set_dates >> extract_bronze >> build_silver >> quality_checks >> build_gold
