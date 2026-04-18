"""
HTTP API for the POP inventory balancing backend.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from services.orchestrator import analyze_event

load_dotenv()

app = FastAPI(title="POP Inventory Management API", version="0.1.0")


@app.post("/agent/analyze/{event_id}")
def post_analyze_event(event_id: int) -> JSONResponse:
    """T8.2 — Run Claude analysis for an event; persist recommendation or return try-again fallback."""
    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_KEY"):
        raise HTTPException(status_code=500, detail="Server missing Supabase configuration.")

    result = analyze_event(event_id)
    if not result.get("ok"):
        if result.get("error") == "event_not_found":
            raise HTTPException(status_code=404, detail=result.get("message", "Not found."))
        # T8.4 — API failure: tell client to try again later
        return JSONResponse(
            status_code=503,
            content={"message": result.get("message", "Try again later."), "detail": result.get("detail")},
        )
    return JSONResponse(status_code=200, content=result)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
