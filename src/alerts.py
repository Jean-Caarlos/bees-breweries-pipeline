# src/alerts.py
from __future__ import annotations
import os
import textwrap
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    import requests  # já está no requirements.txt
except Exception:  # fallback suave se não estiver disponível
    requests = None  # type: ignore


def _post_json(url: str, payload: Dict[str, Any]) -> Optional[int]:
    """Envia POST JSON de forma resiliente. Retorna status_code ou None."""
    if not requests:
        print(f"[alerts] requests não está instalado; não foi possível POST em {url}")
        return None
    try:
        resp = requests.post(url, json=payload, timeout=10)
        print(f"[alerts] POST {url} -> {resp.status_code}")
        return resp.status_code
    except Exception as e:
        print(f"[alerts] Falha POST {url}: {e}")
        return None


def _fanout(text: str) -> None:
    """Dispara mensagem para todos os destinos configurados (Slack/Teams) ou loga."""
    sent = False

    slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
    if slack_webhook:
        _post_json(slack_webhook, {"text": text})
        sent = True

    teams_webhook = os.getenv("TEAMS_WEBHOOK_URL")
    if teams_webhook:
        # payload simples compatível com Teams
        _post_json(teams_webhook, {"text": text})
        sent = True

    if not sent:
        # Sem webhooks definidos — apenas imprime no log do task
        print("[alerts]", text)


def _fmt_context(context: Dict[str, Any], status: str) -> str:
    ti = context.get("ti") or context.get("task_instance")
    dag_id = getattr(getattr(ti, "dag_id", None), "__str__", lambda: None)() or getattr(
        context.get("dag"), "dag_id", "unknown_dag"
    )
    task_id = getattr(ti, "task_id", "unknown_task")
    run_id = getattr(ti, "run_id", context.get("run_id", "unknown_run"))
    try_number = getattr(ti, "try_number", None)
    log_url = getattr(ti, "log_url", "N/A")
    start = getattr(ti, "start_date", None)
    end = getattr(ti, "end_date", None)
    duration = None
    if start and end:
        # garantir UTC e evitar tz bugs
        if getattr(start, "tzinfo", None) is not None:
            start = start.astimezone(timezone.utc).replace(tzinfo=None)
        if getattr(end, "tzinfo", None) is not None:
            end = end.astimezone(timezone.utc).replace(tzinfo=None)
        duration = (end - start).total_seconds()

    lines = [
        f"Airflow task {status}",
        f"DAG: {dag_id}",
        f"Task: {task_id}",
        f"Run: {run_id}",
        f"Try: {try_number}" if try_number is not None else None,
        f"Log: {log_url}",
        f"Duration: {int(duration)}s" if duration is not None else None,
        f"When (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}",
    ]
    return "\n".join([ln for ln in lines if ln])


def notify_success(context: Dict[str, Any]) -> None:
    """Callback de sucesso para usar em on_success_callback."""
    text = _fmt_context(context, "SUCCESS ✅")
    _fanout(text)


def notify_failure(context: Dict[str, Any]) -> None:
    """Callback de falha para usar em on_failure_callback."""
    # Inclui último erro (se disponível) nas primeiras linhas
    exception = context.get("exception")
    head = f"Failure reason: {exception}" if exception else None
    base = _fmt_context(context, "FAILURE ❌")
    text = base if not head else head + "\n" + base
    # se houver traceback no context, evita mensagem gigante
    tb = context.get("traceback")
    if tb:
        text = text + "\n" + textwrap.shorten(str(tb), width=500, placeholder=" ...")
    _fanout(text)
