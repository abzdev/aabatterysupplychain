"""HTTP API for the POP inventory balancing backend."""

from __future__ import annotations

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.agent import router as agent_router
from api.agent_activity import router as agent_activity_router
from api.agent_runs import router as agent_runs_router
from api.approval_queue import router as approval_queue_router
from api.audit import router as audit_router
from api.comparison import router as comparison_router
from api.events import router as events_router
from api.inventory import router as inventory_router
from api.scan import router as scan_router
from api.transfer_requests import router as transfer_requests_router
from services.agent_scheduler import start_agent_scheduler, stop_agent_scheduler

load_dotenv()

app = FastAPI(title="POP Inventory Management API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(agent_router)
app.include_router(agent_runs_router)
app.include_router(agent_activity_router)
app.include_router(events_router)
app.include_router(inventory_router)
app.include_router(comparison_router)
app.include_router(scan_router)
app.include_router(transfer_requests_router)
app.include_router(approval_queue_router)
app.include_router(audit_router)


@app.on_event("startup")
def on_startup() -> None:
    start_agent_scheduler()


@app.on_event("shutdown")
def on_shutdown() -> None:
    stop_agent_scheduler()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
