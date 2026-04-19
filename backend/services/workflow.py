from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import os
from typing import Any, Iterable, Mapping

from supabase import Client, create_client


RISK_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
EVENT_ENTITY_TYPE = "event"
TRANSFER_REQUEST_ENTITY_TYPE = "transfer_request"
LOCATION_CODE_TO_DC = {1: "SF", 2: "NJ", 3: "LA"}
OPERATIONAL_CHARGEBACK_CAUSE_CODES = {"CRED11-F", "CRED11-O", "CRED08", "CRED12"}


class WorkflowError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def require_supabase_config() -> None:
    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_KEY"):
        raise WorkflowError(500, "Server missing Supabase configuration.")


def create_supabase_client() -> Client:
    require_supabase_config()
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


def actor_from_headers(headers: Mapping[str, str] | None, *, default: str = "demo-user") -> str:
    if not headers:
        return default
    actor = headers.get("x-actor") or headers.get("X-Actor")
    if actor is None:
        return default
    cleaned = str(actor).strip()
    return cleaned or default


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fetch_all_rows(
    client: Client,
    table: str,
    *,
    select: str = "*",
    filters: Mapping[str, Any] | None = None,
    page_size: int = 1000,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    filters = filters or {}
    while True:
        query = client.table(table).select(select)
        for column, value in filters.items():
            query = query.eq(column, value)
        response = query.range(offset, offset + page_size - 1).execute()
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_date_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    return text[:10]


def _normalize_lookup_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).strip().upper().split())
    return text or None


def _avg(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _build_sku_description_lookup(client: Client) -> dict[str, set[str]]:
    lookup: dict[str, set[str]] = {}

    def add_rows(rows: list[dict[str, Any]], *, description_field: str) -> None:
        for row in rows:
            sku_id = str(row.get("sku_id") or "").strip()
            description = _normalize_lookup_text(row.get(description_field))
            if not sku_id or description is None:
                continue
            lookup.setdefault(sku_id, set()).add(description)

    add_rows(_fetch_all_rows(client, "inventory_snapshots", select="sku_id,description"), description_field="description")
    add_rows(_fetch_all_rows(client, "sales_history", select="sku_id,item_desc"), description_field="item_desc")
    add_rows(_fetch_all_rows(client, "po_history", select="sku_id,item_description"), description_field="item_description")
    add_rows(_fetch_all_rows(client, "penalty_history", select="sku_id,item_description"), description_field="item_description")
    return lookup


def _manual_chargeback_baselines(
    client: Client,
    events: list[dict[str, Any]],
) -> tuple[dict[int, float], dict[int, str]]:
    sku_descriptions = _build_sku_description_lookup(client)
    desc_to_skus: dict[str, set[str]] = {}
    for sku_id, descriptions in sku_descriptions.items():
        for description in descriptions:
            desc_to_skus.setdefault(description, set()).add(sku_id)

    sku_dc_costs: dict[tuple[str, str], list[float]] = {}
    sku_costs: dict[str, list[float]] = {}
    dc_costs: dict[str, list[float]] = {}
    global_costs: list[float] = []

    chargeback_rows = _fetch_all_rows(
        client,
        "chargebacks",
        select="location_code,cause_code,item_description,extended_price",
    )
    for row in chargeback_rows:
        cause_code = str(row.get("cause_code") or "").strip()
        if cause_code not in OPERATIONAL_CHARGEBACK_CAUSE_CODES:
            continue
        chargeback_cost = _to_float(row.get("extended_price"))
        if chargeback_cost is None or chargeback_cost <= 0:
            continue
        description = _normalize_lookup_text(row.get("item_description"))
        if description is None:
            continue
        dc = LOCATION_CODE_TO_DC.get(_to_int(row.get("location_code")))
        global_costs.append(chargeback_cost)
        if dc is not None:
            dc_costs.setdefault(dc, []).append(chargeback_cost)
        for sku_id in desc_to_skus.get(description, set()):
            sku_costs.setdefault(sku_id, []).append(chargeback_cost)
            if dc is not None:
                sku_dc_costs.setdefault((sku_id, dc), []).append(chargeback_cost)

    global_avg = _avg(global_costs) or 0.0
    manual_costs: dict[int, float] = {}
    manual_basis: dict[int, str] = {}
    for event in events:
        sku_id = event["sku_id"]
        dest_dc = event["dest_dc"]
        sku_dc_avg = _avg(sku_dc_costs.get((sku_id, dest_dc), []))
        sku_avg = _avg(sku_costs.get(sku_id, []))
        dc_avg = _avg(dc_costs.get(dest_dc, []))
        if sku_dc_avg is not None:
            manual_costs[event["id"]] = float(sku_dc_avg)
            manual_basis[event["id"]] = "actual_chargebacks_sku_dc"
        elif sku_avg is not None:
            manual_costs[event["id"]] = float(sku_avg)
            manual_basis[event["id"]] = "actual_chargebacks_sku"
        elif dc_avg is not None:
            manual_costs[event["id"]] = float(dc_avg)
            manual_basis[event["id"]] = "actual_chargebacks_dc"
        else:
            manual_costs[event["id"]] = float(global_avg)
            manual_basis[event["id"]] = "actual_chargebacks_global"
    return manual_costs, manual_basis


def _normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(event["id"]),
        "event_key": str(event.get("event_key") or ""),
        "sku_id": str(event.get("sku_id") or ""),
        "source_dc": str(event.get("source_dc") or ""),
        "dest_dc": str(event.get("dest_dc") or ""),
        "state": str(event.get("state") or ""),
        "days_of_supply": _to_float(event.get("days_of_supply")),
        "stockout_date": _to_date_str(event.get("stockout_date")),
        "transferable_qty": _to_int(event.get("transferable_qty")),
        "network_total": _to_int(event.get("network_total")),
        "relief_arriving": bool(event.get("relief_arriving")) if event.get("relief_arriving") is not None else None,
        "relief_eta": _to_date_str(event.get("relief_eta")),
        "relief_qty": _to_int(event.get("relief_qty")),
        "po_at_risk": bool(event.get("po_at_risk")) if event.get("po_at_risk") is not None else None,
        "penalty_risk_level": event.get("penalty_risk_level"),
        "penalty_risk_score": _to_float(event.get("penalty_risk_score")),
        "expected_penalty_cost": _to_float(event.get("expected_penalty_cost")),
        "recommended_action": event.get("recommended_action"),
        "confidence": event.get("confidence"),
        "reasoning": event.get("reasoning"),
        "cost_transfer": _to_float(event.get("cost_transfer")),
        "cost_wait": _to_float(event.get("cost_wait")),
        "ai_unavailable": bool(event.get("ai_unavailable")) if event.get("ai_unavailable") is not None else None,
        "created_at": event.get("created_at"),
        "updated_at": event.get("updated_at"),
    }


def _normalize_transfer_request(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "event_id": int(row["event_id"]),
        "source_dc": str(row.get("source_dc") or ""),
        "dest_dc": str(row.get("dest_dc") or ""),
        "sku_id": str(row.get("sku_id") or ""),
        "qty": int(row.get("qty") or 0),
        "estimated_cost": _to_float(row.get("estimated_cost")),
        "state": str(row.get("state") or ""),
        "rejection_reason": row.get("rejection_reason"),
        "approved_by": row.get("approved_by"),
        "approved_at": row.get("approved_at"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def _normalize_audit_entry(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(row["id"]) if row.get("id") is not None else None,
        "entity_id": int(row["entity_id"]),
        "entity_type": str(row.get("entity_type") or ""),
        "old_state": row.get("old_state"),
        "new_state": str(row.get("new_state") or ""),
        "actor": row.get("actor"),
        "notes": row.get("notes"),
        "created_at": row.get("created_at"),
    }


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _normalize_snapshot_row(
    row: dict[str, Any],
    *,
    demand_30d: float | None,
    demand_basis: str | None,
    demand_window_days: int,
    event: dict[str, Any] | None,
) -> dict[str, Any]:
    available = _to_int(row.get("available"))
    on_hand = _to_int(row.get("on_hand"))
    weighted_daily_demand = None
    days_of_supply = None
    stockout_date = None
    if demand_30d is not None and demand_window_days > 0:
        weighted_daily_demand = float(demand_30d) / float(demand_window_days)
        if weighted_daily_demand > 0 and available is not None:
            days_of_supply = float(available) / weighted_daily_demand
            snapshot_date = _parse_date(row.get("snapshot_date"))
            if snapshot_date is not None:
                stockout_date = (snapshot_date + timedelta(days=max(int(days_of_supply), 0))).isoformat()

    if days_of_supply is None:
        health_status = "NO_DEMAND_SIGNAL"
    elif days_of_supply < 7:
        health_status = "AT_RISK"
    elif days_of_supply < 15:
        health_status = "WATCH"
    else:
        health_status = "HEALTHY"

    return {
        "sku_id": str(row.get("sku_id") or ""),
        "dc": str(row.get("dc") or ""),
        "description": row.get("description"),
        "snapshot_date": _to_date_str(row.get("snapshot_date")) or "",
        "available": available,
        "on_hand": on_hand,
        "demand_30d": float(demand_30d) if demand_30d is not None else None,
        "weighted_daily_demand": weighted_daily_demand,
        "demand_basis": demand_basis,
        "days_of_supply": days_of_supply,
        "stockout_date": stockout_date,
        "health_status": health_status,
        "risk_level": event.get("penalty_risk_level") if event else None,
        "related_event_id": event.get("id") if event else None,
        "recommended_action": event.get("recommended_action") if event else None,
        "confidence": event.get("confidence") if event else None,
        "expected_penalty_cost": _to_float(event.get("expected_penalty_cost")) if event else None,
    }


def fetch_event_by_id(client: Client, event_id: int) -> dict[str, Any] | None:
    response = client.table("events").select("*").eq("id", event_id).limit(1).execute()
    rows = response.data or []
    return rows[0] if rows else None


def fetch_transfer_request_by_id(client: Client, request_id: int) -> dict[str, Any] | None:
    response = client.table("transfer_requests").select("*").eq("id", request_id).limit(1).execute()
    rows = response.data or []
    return rows[0] if rows else None


def fetch_transfer_requests_for_event(client: Client, event_id: int) -> list[dict[str, Any]]:
    rows = client.table("transfer_requests").select("*").eq("event_id", event_id).order("created_at").execute().data or []
    return [_normalize_transfer_request(row) for row in rows]


def write_audit_log(
    client: Client,
    *,
    entity_id: int,
    entity_type: str,
    old_state: str | None,
    new_state: str,
    actor: str,
    notes: str | None = None,
) -> None:
    client.table("audit_log").insert(
        {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "old_state": old_state,
            "new_state": new_state,
            "actor": actor,
            "notes": notes,
            "created_at": now_iso(),
        }
    ).execute()


def transition_event_state(
    client: Client,
    *,
    event: dict[str, Any],
    new_state: str,
    actor: str,
    notes: str | None = None,
    updates: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    event_id = int(event["id"])
    old_state = event.get("state")
    payload = {"state": new_state}
    if updates:
        payload.update(dict(updates))
    client.table("events").update(payload).eq("id", event_id).execute()
    updated = fetch_event_by_id(client, event_id)
    if updated is None:
        raise WorkflowError(500, "Event update succeeded but could not be reloaded.")
    if old_state != new_state:
        write_audit_log(
            client,
            entity_id=event_id,
            entity_type=EVENT_ENTITY_TYPE,
            old_state=old_state,
            new_state=new_state,
            actor=actor,
            notes=notes,
        )
    return updated


def transition_transfer_request_state(
    client: Client,
    *,
    transfer_request: dict[str, Any],
    new_state: str,
    actor: str,
    notes: str | None = None,
    updates: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    request_id = int(transfer_request["id"])
    old_state = transfer_request.get("state")
    payload = {"state": new_state}
    if updates:
        payload.update(dict(updates))
    client.table("transfer_requests").update(payload).eq("id", request_id).execute()
    updated = fetch_transfer_request_by_id(client, request_id)
    if updated is None:
        raise WorkflowError(500, "Transfer request update succeeded but could not be reloaded.")
    if old_state != new_state:
        write_audit_log(
            client,
            entity_id=request_id,
            entity_type=TRANSFER_REQUEST_ENTITY_TYPE,
            old_state=old_state,
            new_state=new_state,
            actor=actor,
            notes=notes,
        )
    return updated


def _risk_meets_threshold(level: str | None, minimum: str | None) -> bool:
    if minimum is None:
        return True
    if minimum not in RISK_ORDER:
        raise WorkflowError(400, "min_risk_level must be one of LOW, MEDIUM, HIGH.")
    if level not in RISK_ORDER:
        return False
    return RISK_ORDER[level] >= RISK_ORDER[minimum]


def list_events(
    client: Client,
    *,
    dc: str | None = None,
    min_risk_level: str | None = None,
    state: str | None = None,
) -> list[dict[str, Any]]:
    rows = _fetch_all_rows(client, "events")
    items = [_normalize_event(row) for row in rows]
    if dc:
        items = [item for item in items if item["source_dc"] == dc or item["dest_dc"] == dc]
    if min_risk_level:
        threshold = min_risk_level.strip().upper()
        items = [item for item in items if _risk_meets_threshold(item.get("penalty_risk_level"), threshold)]
    if state:
        wanted = state.strip().upper()
        items = [item for item in items if item.get("state") == wanted]
    items.sort(
        key=lambda item: (
            item.get("expected_penalty_cost") is not None,
            item.get("expected_penalty_cost") or -1.0,
            item.get("created_at") or "",
        ),
        reverse=True,
    )
    return items


def _latest_inventory_snapshot_date(client: Client) -> date:
    response = client.table("inventory_snapshots").select("snapshot_date").order("snapshot_date", desc=True).limit(1).execute()
    rows = response.data or []
    if not rows:
        raise WorkflowError(404, "No inventory snapshots are available.")
    snapshot_date = _parse_date(rows[0].get("snapshot_date"))
    if snapshot_date is None:
        raise WorkflowError(500, "Latest inventory snapshot is missing a valid snapshot_date.")
    return snapshot_date


def _load_sales_window(
    client: Client,
    *,
    start_date: date,
    end_date: date,
    page_size: int = 1000,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        response = (
            client.table("sales_history")
            .select("sku_id,dc,quantity_adj,qty_base_uom,doc_date")
            .gte("doc_date", start_date.isoformat())
            .lte("doc_date", end_date.isoformat())
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def _sales_units(row: dict[str, Any]) -> float:
    base_units = _to_float(row.get("qty_base_uom"))
    if base_units is not None and base_units > 0:
        return base_units
    return float(row.get("quantity_adj") or 0)


def _resolved_demand_total(
    recent_total: float | None,
    fallback_total: float | None,
    *,
    demand_window_days: int,
    fallback_window_days: int,
) -> float | None:
    if recent_total is not None and recent_total > 0:
        return recent_total
    if fallback_total is not None and fallback_total > 0 and fallback_window_days > 0:
        return fallback_total * (float(demand_window_days) / float(fallback_window_days))
    return None


def _resolved_demand_basis(recent_total: float | None, fallback_total: float | None) -> str | None:
    if recent_total is not None and recent_total > 0:
        return "LAST_30_DAYS"
    if fallback_total is not None and fallback_total > 0:
        return "LAST_365_DAYS"
    return None


def list_inventory_health(
    client: Client,
    *,
    demand_window_days: int = 30,
) -> dict[str, Any]:
    if demand_window_days <= 0:
        raise WorkflowError(400, "demand_window_days must be greater than zero.")

    snapshot_date = _latest_inventory_snapshot_date(client)
    inventory_rows = _fetch_all_rows(
        client,
        "inventory_snapshots",
        select="sku_id,description,dc,available,on_hand,snapshot_date",
        filters={"snapshot_date": snapshot_date.isoformat()},
    )
    if not inventory_rows:
        raise WorkflowError(404, f"No inventory rows found for snapshot_date={snapshot_date.isoformat()}.")

    fallback_window_days = max(365, demand_window_days)
    sales_rows = _load_sales_window(
        client,
        start_date=snapshot_date - timedelta(days=fallback_window_days - 1),
        end_date=snapshot_date,
    )
    recent_window_start = snapshot_date - timedelta(days=demand_window_days - 1)
    demand_by_cell_recent: dict[tuple[str, str], float] = {}
    demand_by_cell_fallback: dict[tuple[str, str], float] = {}
    for row in sales_rows:
        key = (str(row.get("sku_id") or "").strip(), str(row.get("dc") or "").strip())
        if not key[0] or not key[1]:
            continue
        units = _sales_units(row)
        if units <= 0:
            continue
        demand_by_cell_fallback[key] = demand_by_cell_fallback.get(key, 0.0) + units
        doc_date = _parse_date(row.get("doc_date"))
        if doc_date is not None and doc_date >= recent_window_start:
            demand_by_cell_recent[key] = demand_by_cell_recent.get(key, 0.0) + units

    events = [_normalize_event(row) for row in _fetch_all_rows(client, "events")]
    event_by_dest: dict[tuple[str, str], dict[str, Any]] = {}
    for event in events:
        key = (event["sku_id"], event["dest_dc"])
        current = event_by_dest.get(key)
        if current is None:
            event_by_dest[key] = event
            continue
        current_penalty = current.get("expected_penalty_cost") or -1
        next_penalty = event.get("expected_penalty_cost") or -1
        if next_penalty > current_penalty or (
            next_penalty == current_penalty and (event.get("updated_at") or "") > (current.get("updated_at") or "")
        ):
            event_by_dest[key] = event

    items = [
        _normalize_snapshot_row(
            row,
            demand_30d=_resolved_demand_total(
                demand_by_cell_recent.get((str(row.get("sku_id") or "").strip(), str(row.get("dc") or "").strip())),
                demand_by_cell_fallback.get((str(row.get("sku_id") or "").strip(), str(row.get("dc") or "").strip())),
                demand_window_days=demand_window_days,
                fallback_window_days=fallback_window_days,
            ),
            demand_basis=_resolved_demand_basis(
                demand_by_cell_recent.get((str(row.get("sku_id") or "").strip(), str(row.get("dc") or "").strip())),
                demand_by_cell_fallback.get((str(row.get("sku_id") or "").strip(), str(row.get("dc") or "").strip())),
            ),
            demand_window_days=demand_window_days,
            event=event_by_dest.get((str(row.get("sku_id") or "").strip(), str(row.get("dc") or "").strip())),
        )
        for row in inventory_rows
    ]
    health_priority = {
        "AT_RISK": 0,
        "WATCH": 1,
        "HEALTHY": 2,
        "NO_DEMAND_SIGNAL": 3,
    }
    items.sort(
        key=lambda item: (
            health_priority.get(item["health_status"], 4),
            item["days_of_supply"] if item["days_of_supply"] is not None else float("inf"),
            item["sku_id"],
            item["dc"],
        )
    )

    dos_values = [item["days_of_supply"] for item in items if item["days_of_supply"] is not None]
    summary = {
        "snapshot_date": snapshot_date.isoformat(),
        "total_cells": len(items),
        "at_risk_cells": sum(1 for item in items if item["health_status"] in {"AT_RISK", "WATCH"}),
        "healthy_cells": sum(1 for item in items if item["health_status"] == "HEALTHY"),
        "avg_days_of_supply": sum(dos_values) / len(dos_values) if dos_values else None,
    }
    return {"summary": summary, "items": items}


def _agent_outputs_for_event(event: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        "demand": {
            "days_of_supply": _to_float(event.get("days_of_supply")),
            "stockout_date": _to_date_str(event.get("stockout_date")),
            "depletion_projection": event.get("depletion_projection") or [],
        },
        "imbalance": {
            "source_dc": event.get("source_dc"),
            "dest_dc": event.get("dest_dc"),
            "transferable_qty": _to_int(event.get("transferable_qty")),
            "network_total": _to_int(event.get("network_total")),
        },
        "supply": {
            "relief_arriving": bool(event.get("relief_arriving")) if event.get("relief_arriving") is not None else None,
            "relief_eta": _to_date_str(event.get("relief_eta")),
            "relief_qty": _to_int(event.get("relief_qty")),
            "po_at_risk": bool(event.get("po_at_risk")) if event.get("po_at_risk") is not None else None,
        },
        "penalty": {
            "penalty_risk_level": event.get("penalty_risk_level"),
            "penalty_risk_score": _to_float(event.get("penalty_risk_score")),
            "expected_penalty_cost": _to_float(event.get("expected_penalty_cost")),
        },
    }


def list_audit_entries(
    client: Client,
    *,
    entity_id: int,
    entity_types: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    rows = _fetch_all_rows(client, "audit_log", filters={"entity_id": entity_id})
    items = [_normalize_audit_entry(row) for row in rows]
    if entity_types is not None:
        allowed = set(entity_types)
        items = [item for item in items if item["entity_type"] in allowed]
    items.sort(key=lambda item: item.get("created_at") or "")
    return items


def get_manual_vs_system_comparison(client: Client) -> dict[str, Any]:
    events = [_normalize_event(row) for row in _fetch_all_rows(client, "events")]
    transfer_requests = [_normalize_transfer_request(row) for row in _fetch_all_rows(client, "transfer_requests")]
    audit_rows = [_normalize_audit_entry(row) for row in _fetch_all_rows(client, "audit_log")]
    manual_cost_by_event, manual_basis_by_event = _manual_chargeback_baselines(client, events)

    request_by_event: dict[int, dict[str, Any]] = {}
    for request in transfer_requests:
        current = request_by_event.get(request["event_id"])
        if current is None or (request.get("created_at") or "") > (current.get("created_at") or ""):
            request_by_event[request["event_id"]] = request

    terminal_audit_by_request: dict[int, dict[str, Any]] = {}
    for entry in audit_rows:
        if entry["entity_type"] != TRANSFER_REQUEST_ENTITY_TYPE:
            continue
        if entry["new_state"] not in {"APPROVED", "REJECTED", "EXECUTED"}:
            continue
        current = terminal_audit_by_request.get(entry["entity_id"])
        if current is None or (entry.get("created_at") or "") < (current.get("created_at") or ""):
            terminal_audit_by_request[entry["entity_id"]] = entry

    rows: list[dict[str, Any]] = []
    approval_hours: list[float] = []
    for event in events:
        request = request_by_event.get(event["id"])
        manual_cost = float(manual_cost_by_event.get(event["id"]) or 0)
        if request and request["state"] in {"PENDING_APPROVAL", "APPROVED", "EXECUTED"}:
            system_action = "TRANSFER"
            system_cost = float(request.get("estimated_cost") or event.get("cost_transfer") or 0)
        else:
            system_action = str(event.get("recommended_action") or "WAIT")
            if system_action == "TRANSFER":
                system_cost = float(event.get("cost_transfer") or 0)
            else:
                system_cost = float(event.get("cost_wait") or event.get("expected_penalty_cost") or 0)

        if request:
            created_at = _parse_datetime(request.get("created_at"))
            terminal_entry = terminal_audit_by_request.get(request["id"])
            terminal_at = _parse_datetime(terminal_entry.get("created_at")) if terminal_entry else None
            if created_at and terminal_at and terminal_at >= created_at:
                approval_hours.append((terminal_at - created_at).total_seconds() / 3600)

        rows.append(
            {
                "event_id": event["id"],
                "sku_id": event["sku_id"],
                "source_dc": event["source_dc"],
                "dest_dc": event["dest_dc"],
                "state": event["state"],
                "confidence": event.get("confidence"),
                "expected_penalty_cost": event.get("expected_penalty_cost"),
                "manual_action": "WAIT",
                "manual_cost": manual_cost,
                "system_action": system_action,
                "system_cost": system_cost,
                "delta_vs_manual": manual_cost - system_cost,
                "request_state": request.get("state") if request else None,
                "manual_cost_basis": manual_basis_by_event.get(event["id"]),
            }
        )

    rows.sort(key=lambda row: (row["delta_vs_manual"], row["expected_penalty_cost"] or 0), reverse=True)
    event_count = len(rows)
    transfer_recommended = sum(1 for event in events if event.get("recommended_action") == "TRANSFER")
    approved_transfer_count = sum(
        1 for request in transfer_requests if request.get("state") in {"APPROVED", "EXECUTED"}
    )
    ai_covered = sum(1 for event in events if event.get("ai_unavailable") is not True)

    manual_baseline_cost = sum(row["manual_cost"] for row in rows)
    system_assisted_cost = sum(row["system_cost"] for row in rows)
    summary = {
        "event_count": event_count,
        "transfer_recommended": transfer_recommended,
        "system_assisted_cost": system_assisted_cost,
        "manual_baseline_cost": manual_baseline_cost,
        "estimated_savings": manual_baseline_cost - system_assisted_cost,
        "approved_transfer_count": approved_transfer_count,
        "avg_approval_hours": (sum(approval_hours) / len(approval_hours)) if approval_hours else None,
        "manual_approval_hours_assumption": 24.0,
        "ai_coverage_rate": (ai_covered / event_count) if event_count else None,
    }
    assumptions = [
        "Manual baseline is derived from actual operational chargeback history for the detected SKUs and destination DCs, with fallbacks to SKU-, DC-, or portfolio-level operational chargeback averages when direct matches are sparse.",
        "System-assisted cost uses the latest transfer request estimate when a request exists; otherwise it uses the current recommended action on the event.",
        "The manual baseline is conservative: it excludes hidden manual-process costs such as emergency split-ship freight, OTIF score degradation, and labor spent disputing chargebacks later.",
        "Manual approval time is shown as a 24-hour operating assumption because the current schema does not track a true manual workflow baseline.",
    ]
    return {"summary": summary, "assumptions": assumptions, "rows": rows}


def get_event_detail(client: Client, event_id: int) -> dict[str, Any]:
    event = fetch_event_by_id(client, event_id)
    if event is None:
        raise WorkflowError(404, "Event not found.")
    normalized = _normalize_event(event)
    normalized["agent_outputs"] = _agent_outputs_for_event(event)
    normalized["orchestrator_recommendation"] = {
        "action": event.get("recommended_action"),
        "confidence": event.get("confidence"),
        "reasoning": event.get("reasoning"),
        "cost_transfer": _to_float(event.get("cost_transfer")),
        "cost_wait": _to_float(event.get("cost_wait")),
        "ai_unavailable": bool(event.get("ai_unavailable")) if event.get("ai_unavailable") is not None else None,
    }
    normalized["depletion_projection"] = event.get("depletion_projection") or []
    normalized["state_history"] = list_audit_entries(
        client,
        entity_id=event_id,
        entity_types=[EVENT_ENTITY_TYPE],
    )
    normalized["transfer_requests"] = fetch_transfer_requests_for_event(client, event_id)
    return normalized


def _enrich_transfer_request(
    transfer_request: dict[str, Any],
    event_lookup: Mapping[int, dict[str, Any]],
) -> dict[str, Any]:
    item = _normalize_transfer_request(transfer_request)
    event = event_lookup.get(item["event_id"], {})
    item.update(
        {
            "expected_penalty_cost": _to_float(event.get("expected_penalty_cost")),
            "event_state": event.get("state"),
            "penalty_risk_level": event.get("penalty_risk_level"),
            "recommended_action": event.get("recommended_action"),
            "confidence": event.get("confidence"),
            "stockout_date": _to_date_str(event.get("stockout_date")),
            "days_of_supply": _to_float(event.get("days_of_supply")),
        }
    )
    return item


def list_approval_queue(client: Client) -> list[dict[str, Any]]:
    rows = client.table("transfer_requests").select("*").eq("state", "PENDING_APPROVAL").execute().data or []
    if not rows:
        return []
    event_ids = sorted({int(row["event_id"]) for row in rows})
    event_lookup = {
        item["id"]: item
        for item in _fetch_all_rows(client, "events")
        if int(item["id"]) in event_ids
    }
    queue = [_enrich_transfer_request(row, event_lookup) for row in rows]
    queue.sort(
        key=lambda item: (
            item.get("expected_penalty_cost") is not None,
            item.get("expected_penalty_cost") or -1.0,
            item.get("created_at") or "",
        ),
        reverse=True,
    )
    return queue


def create_transfer_request(
    client: Client,
    *,
    event_id: int,
    source_dc: str,
    dest_dc: str,
    sku_id: str,
    qty: int,
    actor: str,
) -> dict[str, Any]:
    event = fetch_event_by_id(client, event_id)
    if event is None:
        raise WorkflowError(404, "Event not found.")
    if event.get("state") != "ACTION_PROPOSED":
        raise WorkflowError(409, "Transfer requests can only be created from ACTION_PROPOSED events.")
    if event.get("recommended_action") != "TRANSFER":
        raise WorkflowError(409, "Transfer requests require a TRANSFER recommendation.")
    if str(event.get("source_dc")) != source_dc or str(event.get("dest_dc")) != dest_dc or str(event.get("sku_id")) != sku_id:
        raise WorkflowError(400, "Transfer request fields must match the orchestrator recommendation.")
    transferable_qty = _to_int(event.get("transferable_qty")) or 0
    if qty > transferable_qty:
        raise WorkflowError(400, f"qty must be less than or equal to transferable_qty ({transferable_qty}).")

    response = client.table("transfer_requests").insert(
        {
            "event_id": event_id,
            "source_dc": source_dc,
            "dest_dc": dest_dc,
            "sku_id": sku_id,
            "qty": qty,
            "estimated_cost": _to_float(event.get("cost_transfer")),
            "state": "PENDING_APPROVAL",
        }
    ).execute()
    rows = response.data or []
    if not rows:
        raise WorkflowError(500, "Transfer request was not returned after insert.")
    transfer_request = rows[0]
    write_audit_log(
        client,
        entity_id=int(transfer_request["id"]),
        entity_type=TRANSFER_REQUEST_ENTITY_TYPE,
        old_state=None,
        new_state="PENDING_APPROVAL",
        actor=actor,
        notes="Transfer request created.",
    )
    transition_event_state(
        client,
        event=event,
        new_state="PENDING_APPROVAL",
        actor=actor,
        notes=f"Transfer request {transfer_request['id']} created.",
    )
    return _normalize_transfer_request(fetch_transfer_request_by_id(client, int(transfer_request["id"])) or transfer_request)


def approve_transfer_request(client: Client, *, request_id: int, actor: str) -> dict[str, Any]:
    transfer_request = fetch_transfer_request_by_id(client, request_id)
    if transfer_request is None:
        raise WorkflowError(404, "Transfer request not found.")
    if transfer_request.get("state") != "PENDING_APPROVAL":
        raise WorkflowError(409, "Only pending requests can be approved.")
    updated_request = transition_transfer_request_state(
        client,
        transfer_request=transfer_request,
        new_state="APPROVED",
        actor=actor,
        notes="Transfer request approved.",
        updates={"approved_by": actor, "approved_at": now_iso()},
    )
    event = fetch_event_by_id(client, int(updated_request["event_id"]))
    if event is None:
        raise WorkflowError(404, "Linked event not found.")
    transition_event_state(
        client,
        event=event,
        new_state="APPROVED",
        actor=actor,
        notes=f"Transfer request {request_id} approved.",
    )
    return _normalize_transfer_request(updated_request)


def reject_transfer_request(client: Client, *, request_id: int, actor: str, reason: str) -> dict[str, Any]:
    transfer_request = fetch_transfer_request_by_id(client, request_id)
    if transfer_request is None:
        raise WorkflowError(404, "Transfer request not found.")
    if transfer_request.get("state") != "PENDING_APPROVAL":
        raise WorkflowError(409, "Only pending requests can be rejected.")
    cleaned_reason = reason.strip()
    if not cleaned_reason:
        raise WorkflowError(400, "Rejection reason is required.")
    updated_request = transition_transfer_request_state(
        client,
        transfer_request=transfer_request,
        new_state="REJECTED",
        actor=actor,
        notes=cleaned_reason,
        updates={"rejection_reason": cleaned_reason},
    )
    event = fetch_event_by_id(client, int(updated_request["event_id"]))
    if event is None:
        raise WorkflowError(404, "Linked event not found.")
    transition_event_state(
        client,
        event=event,
        new_state="REJECTED",
        actor=actor,
        notes=f"Transfer request {request_id} rejected: {cleaned_reason}",
    )
    return _normalize_transfer_request(updated_request)
