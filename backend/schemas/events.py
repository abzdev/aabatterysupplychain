from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class TransferRequestCreateRequest(BaseModel):
    event_id: int
    source_dc: str
    dest_dc: str
    sku_id: str
    qty: int = Field(..., gt=0)


class TransferRequestRejectRequest(BaseModel):
    reason: str = Field(..., min_length=1)


class AuditLogEntry(BaseModel):
    id: int | None = None
    entity_id: int
    entity_type: str
    old_state: str | None = None
    new_state: str
    actor: str | None = None
    notes: str | None = None
    created_at: datetime | None = None


class TransferRequestResponse(BaseModel):
    id: int
    event_id: int
    source_dc: str
    dest_dc: str
    sku_id: str
    qty: int
    estimated_cost: float | None = None
    state: str
    rejection_reason: str | None = None
    approved_by: str | None = None
    approved_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ApprovalQueueItem(TransferRequestResponse):
    expected_penalty_cost: float | None = None
    event_state: str | None = None
    penalty_risk_level: str | None = None
    recommended_action: str | None = None
    confidence: str | None = None
    stockout_date: str | None = None
    days_of_supply: float | None = None


class EventSummary(BaseModel):
    id: int
    event_key: str
    sku_id: str
    source_dc: str
    dest_dc: str
    state: str
    days_of_supply: float | None = None
    stockout_date: str | None = None
    transferable_qty: int | None = None
    network_total: int | None = None
    relief_arriving: bool | None = None
    relief_eta: str | None = None
    relief_qty: int | None = None
    po_at_risk: bool | None = None
    penalty_risk_level: str | None = None
    penalty_risk_score: float | None = None
    expected_penalty_cost: float | None = None
    recommended_action: str | None = None
    confidence: str | None = None
    reasoning: str | None = None
    cost_transfer: float | None = None
    cost_wait: float | None = None
    ai_unavailable: bool | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class EventDetailResponse(EventSummary):
    agent_outputs: dict[str, dict[str, Any]]
    orchestrator_recommendation: dict[str, Any]
    depletion_projection: list[dict[str, Any]]
    state_history: list[AuditLogEntry]
    transfer_requests: list[TransferRequestResponse]


class InventoryHealthItem(BaseModel):
    sku_id: str
    dc: str
    description: str | None = None
    snapshot_date: str
    available: int | None = None
    on_hand: int | None = None
    demand_30d: float | None = None
    weighted_daily_demand: float | None = None
    demand_basis: str | None = None
    days_of_supply: float | None = None
    stockout_date: str | None = None
    health_status: str
    risk_level: str | None = None
    related_event_id: int | None = None
    recommended_action: str | None = None
    confidence: str | None = None
    expected_penalty_cost: float | None = None


class InventoryHealthSummary(BaseModel):
    snapshot_date: str
    total_cells: int
    at_risk_cells: int
    healthy_cells: int
    avg_days_of_supply: float | None = None


class InventoryHealthResponse(BaseModel):
    summary: InventoryHealthSummary
    items: list[InventoryHealthItem]


class ComparisonSummary(BaseModel):
    event_count: int
    transfer_recommended: int
    system_assisted_cost: float
    manual_baseline_cost: float
    estimated_savings: float
    approved_transfer_count: int
    avg_approval_hours: float | None = None
    manual_approval_hours_assumption: float | None = None
    ai_coverage_rate: float | None = None


class ComparisonEventRow(BaseModel):
    event_id: int
    sku_id: str
    source_dc: str
    dest_dc: str
    state: str
    confidence: str | None = None
    expected_penalty_cost: float | None = None
    manual_action: str
    manual_cost: float
    system_action: str
    system_cost: float
    delta_vs_manual: float
    request_state: str | None = None


class ComparisonResponse(BaseModel):
    summary: ComparisonSummary
    assumptions: list[str]
    rows: list[ComparisonEventRow]


class AgentActivityEntry(BaseModel):
    id: int | None = None
    run_id: int
    event_id: int | None = None
    action_type: str
    message: str
    metadata: dict[str, Any] | None = None
    created_at: datetime | None = None


class AgentRunResponse(BaseModel):
    id: int
    trigger_source: str
    actor: str
    status: str
    scan_params: dict[str, Any] | None = None
    events_scanned: int = 0
    events_analyzed: int = 0
    analysis_failures: int = 0
    flagged_for_review: int = 0
    monitored_count: int = 0
    skipped_reason: str | None = None
    error_message: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class AgentRunDetailResponse(BaseModel):
    run: AgentRunResponse
    activities: list[AgentActivityEntry]


class AgentLatestRunResponse(BaseModel):
    run: AgentRunResponse | None = None
    activities: list[AgentActivityEntry]
    next_run_at: datetime | None = None
    interval_hours: int
    scheduler_running: bool


class ScanParams(BaseModel):
    max_dos: float = 60.0
    demand_window_days: int = 30
    horizon_days: int = 60


class ScanResult(BaseModel):
    events_scanned: int
    events_analyzed: int
    analysis_failures: int
    event_ids: list[int]
    failed_event_ids: list[int]
    actor: str
