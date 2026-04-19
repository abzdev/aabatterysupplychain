from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from schemas.events import AgentActivityEntry
from services.agent_runner import list_agent_activity
from services.workflow import WorkflowError, create_supabase_client


router = APIRouter(prefix="/agent/activity", tags=["agent-activity"])


@router.get("", response_model=list[AgentActivityEntry])
def get_activity(
    run_id: int | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict]:
    try:
        client = create_supabase_client()
        return list_agent_activity(client, run_id=run_id, limit=limit)
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
