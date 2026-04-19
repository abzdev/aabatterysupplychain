"""
POST /scan orchestration: runs all four agents in sequence, upserts events,
then immediately runs Claude analysis per event via analyze_event().
"""

from __future__ import annotations

from time import perf_counter
from typing import Any

from supabase import Client

from schemas.events import ScanParams
from services.agents.demand_agent import DemandAgent, DemandAgentConfig
from services.agents.imbalance_agent import ImbalanceAgent, ImbalanceAgentConfig
from services.agents.penalty_agent import PenaltyAgent, PenaltyAgentConfig
from services.orchestrator import analyze_event


def _log_stage_preview(stage: str, rows: list[dict[str, Any]], *, keys: list[str], limit: int = 5) -> None:
    if not rows:
        print(f"scanner: {stage} preview none")
        return
    preview = [{key: row.get(key) for key in keys} for row in rows[:limit]]
    print(f"scanner: {stage} preview count={len(rows)} sample={preview}")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def run_scan(
    client: Client,
    *,
    actor: str = "system",
    params: ScanParams | None = None,
) -> dict[str, Any]:
    p = params or ScanParams()
    scan_started_at = perf_counter()
    print(
        "scanner: starting scan "
        f"actor={actor} max_dos={p.max_dos} demand_window_days={p.demand_window_days} horizon_days={p.horizon_days}"
    )

    # 1. Demand pass — explicit call so demand hits are visible as a named step.
    demand_started_at = perf_counter()
    demand_agent = DemandAgent(
        client,
        config=DemandAgentConfig(
            demand_window_days=p.demand_window_days,
            max_days_of_supply=p.max_dos,
            horizon_days=p.horizon_days,
        ),
    )
    demand_hits_df = demand_agent.build_events()
    demand_elapsed_ms = round((perf_counter() - demand_started_at) * 1000)
    print(
        "scanner: demand stage produced "
        f"{len(demand_hits_df)} low-stock hits elapsed_ms={demand_elapsed_ms}"
    )
    if not demand_hits_df.empty:
        _log_stage_preview(
            "demand_hits",
            demand_hits_df.to_dict(orient="records"),
            keys=["event_key", "sku_id", "source_dc", "dest_dc", "days_of_supply"],
        )

    # 2. Imbalance + Supply pass — supply agent (evaluate_supply_for_event) is called
    #    per hit inside ImbalanceAgent.build_events() as a per-candidate gate.
    imbalance_started_at = perf_counter()
    imbalance_agent = ImbalanceAgent(
        client,
        config=ImbalanceAgentConfig(
            demand_window_days=p.demand_window_days,
            max_days_of_supply=p.max_dos,
        ),
    )
    events_df = imbalance_agent.build_events(demand_hits_df=demand_hits_df)
    imbalance_elapsed_ms = round((perf_counter() - imbalance_started_at) * 1000)
    print(
        "scanner: imbalance stage produced "
        f"{len(events_df)} confirmed events elapsed_ms={imbalance_elapsed_ms} "
        f"dropoff_from_demand={max(len(demand_hits_df) - len(events_df), 0)}"
    )
    if not events_df.empty:
        _log_stage_preview(
            "confirmed_events",
            events_df.to_dict(orient="records"),
            keys=["event_key", "sku_id", "source_dc", "dest_dc", "days_of_supply", "transferable_qty", "network_total"],
        )

    if events_df.empty:
        print("scanner: no confirmed events to persist or analyze.")
        return {
            "events_scanned": 0,
            "events_analyzed": 0,
            "analysis_failures": 0,
            "event_ids": [],
            "failed_event_ids": [],
            "actor": actor,
        }

    # Upsert events with state DETECTED (on_conflict="event_key" refreshes fields).
    upsert_started_at = perf_counter()
    imbalance_agent.persist_events(events_df)
    upsert_elapsed_ms = round((perf_counter() - upsert_started_at) * 1000)
    print(f"scanner: persisted confirmed events elapsed_ms={upsert_elapsed_ms}")

    # 3. Re-fetch the upserted event ids for downstream steps.
    event_keys: list[str] = events_df["event_key"].dropna().tolist()
    event_ids = _fetch_event_ids_by_keys(client, event_keys)
    missing_event_keys = max(len(event_keys) - len(event_ids), 0)
    print(
        "scanner: fetched event ids after upsert "
        f"requested={len(event_keys)} resolved={len(event_ids)} missing={missing_event_keys} event_ids={event_ids}"
    )

    # 4. Penalty pass — scores expected_penalty_cost onto every event in the table.
    penalty_started_at = perf_counter()
    penalty_agent = PenaltyAgent(client, config=PenaltyAgentConfig())
    payload_df = penalty_agent.build_event_penalty_payloads(event_ids=event_ids)
    penalty_elapsed_ms = round((perf_counter() - penalty_started_at) * 1000)
    print(
        "scanner: penalty stage produced "
        f"{len(payload_df)} payload rows elapsed_ms={penalty_elapsed_ms}"
    )
    if not payload_df.empty:
        _log_stage_preview(
            "penalty_payloads",
            payload_df.to_dict(orient="records"),
            keys=["event_id", "sku_id", "dest_dc", "penalty_cost"],
        )
    if not payload_df.empty:
        penalty_persist_started_at = perf_counter()
        penalty_agent.persist_expected_penalty_costs(payload_df)
        penalty_persist_elapsed_ms = round((perf_counter() - penalty_persist_started_at) * 1000)
        penalty_values = [
            value
            for value in (_safe_float(row.get("penalty_cost")) for row in payload_df.to_dict(orient="records"))
            if value is not None
        ]
        print(
            "scanner: persisted penalty scores "
            f"elapsed_ms={penalty_persist_elapsed_ms} "
            f"min_penalty={min(penalty_values) if penalty_values else None} "
            f"max_penalty={max(penalty_values) if penalty_values else None}"
        )
    else:
        print("scanner: skipped penalty persistence because there were no payload rows.")

    # 5. Orchestrator pass — analyze each event; AI failures are soft.
    analyzed: list[int] = []
    failed: list[int] = []
    orchestrator_started_at = perf_counter()
    for event_id in event_ids:
        print(f"scanner: analyzing event_id={event_id}")
        result = analyze_event(event_id, client=client, actor=actor)
        if result.get("ok"):
            analyzed.append(event_id)
            print(
                "scanner: analyze_event succeeded "
                f"event_id={event_id} action={result.get('recommended_action')} "
                f"confidence={result.get('confidence')}"
            )
        else:
            failed.append(event_id)
            print(
                "scanner: analyze_event failed "
                f"event_id={event_id} error={result.get('error')} detail={result.get('detail')}"
            )

    orchestrator_elapsed_ms = round((perf_counter() - orchestrator_started_at) * 1000)
    total_elapsed_ms = round((perf_counter() - scan_started_at) * 1000)
    print(
        "scanner: completed scan "
        f"events_scanned={len(event_ids)} events_analyzed={len(analyzed)} "
        f"analysis_failures={len(failed)} orchestrator_elapsed_ms={orchestrator_elapsed_ms} "
        f"total_elapsed_ms={total_elapsed_ms}"
    )

    return {
        "events_scanned": len(event_ids),
        "events_analyzed": len(analyzed),
        "analysis_failures": len(failed),
        "event_ids": event_ids,
        "failed_event_ids": failed,
        "actor": actor,
    }


def _fetch_event_ids_by_keys(client: Client, event_keys: list[str]) -> list[int]:
    if not event_keys:
        return []
    # Supabase PostgREST: use `.in_("event_key", [...])` for batch lookup.
    response = (
        client.table("events")
        .select("id")
        .in_("event_key", event_keys)
        .execute()
    )
    rows = response.data or []
    return [int(r["id"]) for r in rows if r.get("id") is not None]
