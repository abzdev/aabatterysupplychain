from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Request
from fastapi.responses import JSONResponse

from schemas.events import AgentRunDetailResponse, ScanParams
from services.agent_runner import launch_agent_run
from services.orchestrator import analyze_event
from services.workflow import WorkflowError, actor_from_headers, require_supabase_config


router = APIRouter(prefix="/agent", tags=["agent"])


@router.post("/run", response_model=AgentRunDetailResponse)
def post_run_agent(
    request: Request,
    params: ScanParams | None = Body(default=None),
) -> dict:
    try:
        require_supabase_config()
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    return launch_agent_run(
        trigger_source="manual",
        actor=actor_from_headers(request.headers, default="agent:manual"),
        params=params,
    )


@router.post("/analyze/{event_id}")
def post_analyze_event(event_id: int, request: Request) -> JSONResponse:
    """Run Claude analysis for an event and persist the recommendation."""
    try:
        require_supabase_config()
    except WorkflowError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    result = analyze_event(event_id, actor=actor_from_headers(request.headers, default="system"))
    if not result.get("ok"):
        if result.get("error") == "event_not_found":
            raise HTTPException(status_code=404, detail=result.get("message", "Not found."))
        return JSONResponse(
            status_code=503,
            content={"message": result.get("message", "Try again later."), "detail": result.get("detail")},
        )
    return JSONResponse(status_code=200, content=result)
