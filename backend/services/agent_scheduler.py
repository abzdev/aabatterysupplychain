from __future__ import annotations

from datetime import datetime, timezone
import os

from apscheduler.schedulers.background import BackgroundScheduler

from services.agent_runner import run_agent_job_sync

JOB_ID = "autonomous_agent_scan"
DEFAULT_INTERVAL_HOURS = 6

_scheduler = BackgroundScheduler(timezone="UTC")
_started = False


def get_agent_interval_hours() -> int:
    value = os.environ.get("AGENT_SCAN_INTERVAL_HOURS")
    if value is None:
        return DEFAULT_INTERVAL_HOURS
    try:
        hours = int(value)
    except ValueError:
        return DEFAULT_INTERVAL_HOURS
    return max(hours, 1)


def _run_scheduled_job() -> None:
    run_agent_job_sync(trigger_source="scheduled", actor="agent:scheduler")


def start_agent_scheduler() -> None:
    global _started
    if _started:
        return
    interval_hours = get_agent_interval_hours()
    _scheduler.add_job(
        _run_scheduled_job,
        "interval",
        hours=interval_hours,
        id=JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc),
    )
    _scheduler.start()
    _started = True


def stop_agent_scheduler() -> None:
    global _started
    if not _started:
        return
    _scheduler.shutdown(wait=False)
    _started = False


def get_agent_next_run_at() -> datetime | None:
    job = _scheduler.get_job(JOB_ID)
    if job is None:
        return None
    return job.next_run_time


def is_agent_scheduler_running() -> bool:
    return _started and _scheduler.running
