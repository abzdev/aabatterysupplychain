from __future__ import annotations

from datetime import datetime, timezone
import threading
from typing import Any

from supabase import Client

from schemas.events import ScanParams
from services.scanner import run_scan
from services.workflow import WorkflowError, create_supabase_client

AGENT_RUN_LOCK = threading.Lock()
COST_PROXIMITY_THRESHOLD = 0.15
DEFAULT_ACTIVITY_LIMIT = 20


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_agent_run(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "trigger_source": str(row.get("trigger_source") or ""),
        "actor": str(row.get("actor") or ""),
        "status": str(row.get("status") or ""),
        "scan_params": row.get("scan_params"),
        "events_scanned": int(row.get("events_scanned") or 0),
        "events_analyzed": int(row.get("events_analyzed") or 0),
        "analysis_failures": int(row.get("analysis_failures") or 0),
        "flagged_for_review": int(row.get("flagged_for_review") or 0),
        "monitored_count": int(row.get("monitored_count") or 0),
        "skipped_reason": row.get("skipped_reason"),
        "error_message": row.get("error_message"),
        "created_at": row.get("created_at"),
        "started_at": row.get("started_at"),
        "completed_at": row.get("completed_at"),
    }


def _normalize_agent_activity(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]) if row.get("id") is not None else None,
        "run_id": int(row["run_id"]),
        "event_id": int(row["event_id"]) if row.get("event_id") is not None else None,
        "action_type": str(row.get("action_type") or ""),
        "message": str(row.get("message") or ""),
        "metadata": row.get("metadata"),
        "created_at": row.get("created_at"),
    }


def _insert_agent_run(
    client: Client,
    *,
    trigger_source: str,
    actor: str,
    params: ScanParams,
) -> dict[str, Any]:
    response = client.table("agent_runs").insert(
        {
            "trigger_source": trigger_source,
            "actor": actor,
            "status": "PENDING",
            "scan_params": params.model_dump(),
        }
    ).execute()
    rows = response.data or []
    if not rows:
        raise WorkflowError(500, "Agent run could not be created.")
    return _normalize_agent_run(rows[0])


def _update_agent_run(client: Client, run_id: int, **updates: Any) -> dict[str, Any]:
    response = client.table("agent_runs").update(updates).eq("id", run_id).execute()
    rows = response.data or []
    if rows:
        return _normalize_agent_run(rows[0])
    row = client.table("agent_runs").select("*").eq("id", run_id).limit(1).execute().data or []
    if not row:
        raise WorkflowError(404, "Agent run not found.")
    return _normalize_agent_run(row[0])


def _append_activity(
    client: Client,
    *,
    run_id: int,
    action_type: str,
    message: str,
    event_id: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = client.table("agent_activity_log").insert(
        {
            "run_id": run_id,
            "event_id": event_id,
            "action_type": action_type,
            "message": message,
            "metadata": metadata,
            "created_at": _now_iso(),
        }
    ).execute()
    rows = response.data or []
    if not rows:
        raise WorkflowError(500, "Agent activity could not be written.")
    return _normalize_agent_activity(rows[0])


def _fetch_events_for_run(client: Client, event_ids: list[int]) -> list[dict[str, Any]]:
    if not event_ids:
        return []
    rows = (
        client.table("events")
        .select(
            "id,sku_id,source_dc,dest_dc,state,recommended_action,confidence,"
            "expected_penalty_cost,cost_transfer,cost_wait,ai_unavailable,days_of_supply,stockout_date"
        )
        .in_("id", event_ids)
        .execute()
        .data
        or []
    )
    rows.sort(
        key=lambda row: (
            row.get("expected_penalty_cost") is not None,
            float(row.get("expected_penalty_cost") or -1),
            str(row.get("stockout_date") or ""),
        ),
        reverse=True,
    )
    return rows


def _costs_close(event: dict[str, Any]) -> bool:
    transfer = event.get("cost_transfer")
    wait = event.get("cost_wait")
    if transfer is None or wait is None:
        return False
    try:
        transfer_value = float(transfer)
        wait_value = float(wait)
    except (TypeError, ValueError):
        return False
    baseline = max(abs(transfer_value), abs(wait_value), 1.0)
    return abs(transfer_value - wait_value) <= baseline * COST_PROXIMITY_THRESHOLD


def _decision_for_event(event: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    event_id = int(event["id"])
    action = str(event.get("recommended_action") or "")
    confidence = str(event.get("confidence") or "")
    penalty = float(event.get("expected_penalty_cost") or 0)

    if event.get("ai_unavailable"):
        return (
            "flag_for_review",
            f"Event {event_id} flagged for review because AI analysis was unavailable.",
            {"reason": "ai_unavailable", "recommended_action": action, "confidence": confidence, "expected_penalty_cost": penalty},
        )

    if _costs_close(event):
        return (
            "flag_for_review",
            f"Event {event_id} flagged for review because transfer and wait costs are within 15% of each other.",
            {"reason": "costs_close", "recommended_action": action, "confidence": confidence, "expected_penalty_cost": penalty},
        )

    if action == "TRANSFER" or confidence == "LOW":
        return (
            "flag_for_review",
            f"Event {event_id} flagged for human review with recommendation {action or 'UNKNOWN'} at {confidence or 'UNKNOWN'} confidence.",
            {"reason": "review_policy", "recommended_action": action, "confidence": confidence, "expected_penalty_cost": penalty},
        )

    return (
        "mark_monitored",
        f"Event {event_id} marked monitored with recommendation {action or 'UNKNOWN'} at {confidence or 'UNKNOWN'} confidence.",
        {"reason": "monitor_policy", "recommended_action": action, "confidence": confidence, "expected_penalty_cost": penalty},
    )


def _activities_for_run(client: Client, *, run_id: int, limit: int = DEFAULT_ACTIVITY_LIMIT) -> list[dict[str, Any]]:
    rows = (
        client.table("agent_activity_log")
        .select("*")
        .eq("run_id", run_id)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
        .data
        or []
    )
    items = [_normalize_agent_activity(row) for row in rows]
    items.reverse()
    return items


def get_agent_run(client: Client, *, run_id: int) -> dict[str, Any]:
    rows = client.table("agent_runs").select("*").eq("id", run_id).limit(1).execute().data or []
    if not rows:
        raise WorkflowError(404, "Agent run not found.")
    return _normalize_agent_run(rows[0])


def get_agent_run_detail(client: Client, *, run_id: int, activity_limit: int = DEFAULT_ACTIVITY_LIMIT) -> dict[str, Any]:
    return {
        "run": get_agent_run(client, run_id=run_id),
        "activities": _activities_for_run(client, run_id=run_id, limit=activity_limit),
    }


def get_latest_agent_run(client: Client) -> dict[str, Any] | None:
    rows = client.table("agent_runs").select("*").order("created_at", desc=True).limit(1).execute().data or []
    if not rows:
        return None
    return _normalize_agent_run(rows[0])


def list_agent_activity(
    client: Client,
    *,
    run_id: int | None = None,
    limit: int = DEFAULT_ACTIVITY_LIMIT,
) -> list[dict[str, Any]]:
    query = client.table("agent_activity_log").select("*").order("created_at", desc=True).limit(limit)
    if run_id is not None:
        query = query.eq("run_id", run_id)
    rows = query.execute().data or []
    items = [_normalize_agent_activity(row) for row in rows]
    items.reverse()
    return items


def _finalize_skipped_run(
    client: Client,
    *,
    run_id: int,
    reason: str,
) -> dict[str, Any]:
    updated = _update_agent_run(
        client,
        run_id,
        status="SKIPPED",
        skipped_reason=reason,
        completed_at=_now_iso(),
    )
    _append_activity(
        client,
        run_id=run_id,
        action_type="run_skipped",
        message=reason,
    )
    return updated


def _execute_run(
    *,
    run_id: int,
    trigger_source: str,
    actor: str,
    params: ScanParams,
) -> None:
    client = create_supabase_client()
    if not AGENT_RUN_LOCK.acquire(blocking=False):
        _finalize_skipped_run(
            client,
            run_id=run_id,
            reason="Another autonomous agent run is already in progress.",
        )
        return

    try:
        _update_agent_run(client, run_id, status="RUNNING", started_at=_now_iso(), skipped_reason=None, error_message=None)
        _append_activity(
            client,
            run_id=run_id,
            action_type="run_started",
            message=f"Autonomous agent run started via {trigger_source}.",
            metadata={"trigger_source": trigger_source, "actor": actor, "scan_params": params.model_dump()},
        )

        result = run_scan(client, actor=actor, params=params)
        _append_activity(
            client,
            run_id=run_id,
            action_type="scan_completed",
            message=f"Scan completed with {result['events_scanned']} events detected and {result['events_analyzed']} analyses.",
            metadata=result,
        )

        events = _fetch_events_for_run(client, result.get("event_ids", []))
        if events:
            _append_activity(
                client,
                run_id=run_id,
                action_type="prioritization",
                message="Agent prioritized flagged events by expected penalty exposure.",
                metadata={"event_ids": [int(event["id"]) for event in events]},
            )

        flagged_for_review = 0
        monitored_count = 0
        for event in events:
            action_type, message, metadata = _decision_for_event(event)
            if action_type == "flag_for_review":
                flagged_for_review += 1
            else:
                monitored_count += 1
            _append_activity(
                client,
                run_id=run_id,
                event_id=int(event["id"]),
                action_type=action_type,
                message=message,
                metadata={
                    **metadata,
                    "sku_id": event.get("sku_id"),
                    "source_dc": event.get("source_dc"),
                    "dest_dc": event.get("dest_dc"),
                },
            )

        _update_agent_run(
            client,
            run_id,
            status="SUCCEEDED",
            events_scanned=int(result.get("events_scanned") or 0),
            events_analyzed=int(result.get("events_analyzed") or 0),
            analysis_failures=int(result.get("analysis_failures") or 0),
            flagged_for_review=flagged_for_review,
            monitored_count=monitored_count,
            completed_at=_now_iso(),
        )
        _append_activity(
            client,
            run_id=run_id,
            action_type="run_completed",
            message=(
                f"Run complete: {flagged_for_review} flagged for review, "
                f"{monitored_count} marked monitored, {int(result.get('analysis_failures') or 0)} analysis failures."
            ),
            metadata={
                "flagged_for_review": flagged_for_review,
                "monitored_count": monitored_count,
                "analysis_failures": int(result.get("analysis_failures") or 0),
            },
        )
    except Exception as exc:
        _update_agent_run(
            client,
            run_id,
            status="FAILED",
            error_message=str(exc),
            completed_at=_now_iso(),
        )
        _append_activity(
            client,
            run_id=run_id,
            action_type="run_failed",
            message=f"Autonomous agent run failed: {exc}",
            metadata={"error": str(exc)},
        )
    finally:
        AGENT_RUN_LOCK.release()


def launch_agent_run(
    *,
    trigger_source: str,
    actor: str,
    params: ScanParams | None = None,
) -> dict[str, Any]:
    client = create_supabase_client()
    resolved_params = params or ScanParams()
    run = _insert_agent_run(client, trigger_source=trigger_source, actor=actor, params=resolved_params)
    thread = threading.Thread(
        target=_execute_run,
        kwargs={
            "run_id": run["id"],
            "trigger_source": trigger_source,
            "actor": actor,
            "params": resolved_params,
        },
        daemon=True,
    )
    thread.start()
    return get_agent_run_detail(client, run_id=run["id"])


def run_agent_job_sync(
    *,
    trigger_source: str,
    actor: str,
    params: ScanParams | None = None,
) -> dict[str, Any]:
    client = create_supabase_client()
    resolved_params = params or ScanParams()
    run = _insert_agent_run(client, trigger_source=trigger_source, actor=actor, params=resolved_params)
    _execute_run(
        run_id=run["id"],
        trigger_source=trigger_source,
        actor=actor,
        params=resolved_params,
    )
    return get_agent_run_detail(client, run_id=run["id"])
