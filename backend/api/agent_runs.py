from __future__ import annotations

from fastapi import APIRouter, HTTPException

from schemas.events import AgentLatestRunResponse, AgentRunDetailResponse
from services.agent_runner import get_agent_run_detail, get_latest_agent_run
from services.agent_scheduler import get_agent_interval_hours, get_agent_next_run_at, is_agent_scheduler_running
from services.workflow import WorkflowError, create_supabase_client


router = APIRouter(prefix="/agent/runs", tags=["agent-runs"])


@router.get("/latest", response_model=AgentLatestRunResponse)
def get_latest_run() -> dict:
    try:
        client = create_supabase_client()
        latest = get_latest_agent_run(client)
        activities: list[dict] = []
        if latest is not None:
            detail = get_agent_run_detail(client, run_id=latest["id"])
            activities = detail["activities"]
        return {
            "run": latest,
            "activities": activities,
            "next_run_at": get_agent_next_run_at(),
            "interval_hours": get_agent_interval_hours(),
            "scheduler_running": is_agent_scheduler_running(),
        }
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/{run_id}", response_model=AgentRunDetailResponse)
def get_run_detail(run_id: int) -> dict:
    try:
        client = create_supabase_client()
        return get_agent_run_detail(client, run_id=run_id, activity_limit=100)
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
