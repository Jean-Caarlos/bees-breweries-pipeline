# src/alerts.py
from __future__ import annotations
import os, json, requests
from datetime import datetime, timezone

SLACK_WEBHOOK = os.getenv("ALERT_SLACK_WEBHOOK_URL", "").strip()

def _post_slack(payload: dict) -> None:
    if not SLACK_WEBHOOK:
        print("[alert] SLACK_WEBHOOK não configurado, mensagem:", payload)
        return
    try:
        requests.post(SLACK_WEBHOOK, data=json.dumps(payload), timeout=10)
    except Exception as e:
        print("[alert] falha ao enviar pro Slack:", e, payload)

def _fmt_ctx(ctx) -> str:
    dag = ctx["dag"].dag_id
    task = ctx["task"].task_id
    run_id = ctx["run_id"]
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log_url = ctx.get("task_instance").log_url
    return f"*DAG*: `{dag}`  |  *Task*: `{task}`  |  *Run*: `{run_id}`  |  *When*: {ts}\n<${log_url}|Ver logs>"

def notify_failure(context):
    _post_slack({
        "text": f":rotating_light: *Task FAILED*\n{_fmt_ctx(context)}"
    })

def notify_success(context):
    _post_slack({
        "text": f":white_check_mark: *Task SUCCEEDED*\n{_fmt_ctx(context)}"
    })
