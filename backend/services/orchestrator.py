"""T8.1 system prompt and Claude request schema live at the top of this module."""

from __future__ import annotations

# =============================================================================
# T8.1 — System prompt & JSON request payload (before imports & orchestration)
# =============================================================================
#
# Claude receives a *user* message whose body is a single JSON object (see schema
# below). The *system* message is AGENT_ANALYSIS_SYSTEM_PROMPT.
#
# Request payload JSON (assembled from event row + transfer_cost_lookup):
# {
#   "sku_id": "<string>",
#   "dest_dc": "SF" | "NJ" | "LA",
#   "days_of_supply": <number | null>,
#   "stockout_date": "YYYY-MM-DD" | null,
#   "relief_arriving": <boolean>,
#   "relief_eta": "YYYY-MM-DD" | null,
#   "po_at_risk": <boolean>,
#   "transferable_qty": <integer | null>,
#   "transfer_cost": <number>,
#   "expected_penalty_cost": <number | null>,
#   "penalty_risk_level": "LOW" | "MEDIUM" | "HIGH" | null
# }
#
# transfer_cost: USD from transfer_cost_lookup.avg_cost for dest_dc; if missing,
# use real average 3186.0 (network-wide blended average for this dataset).
#
# Claude (routed via OpenRouter) must respond with JSON only (no markdown), exactly:
# {"action":"TRANSFER"|"WAIT"|"MONITOR","confidence":"HIGH"|"MED"|"LOW","reasoning":"<string>","cost_transfer":<float>,"cost_wait":<float>}

AGENT_ANALYSIS_SYSTEM_PROMPT = """You are the inventory balancing orchestrator for a multi-DC battery supply network.

You will receive one JSON object in the user message. It aggregates outputs from demand, supply, imbalance, and penalty agents for a single event. Field meanings:
- sku_id: product identifier.
- dest_dc: destination distribution center code (critical site).
- days_of_supply: runway at destination; lower is more urgent.
- stockout_date: projected stockout date at destination if known; null if unknown.
- relief_arriving: whether inbound PO relief exists for this SKU at dest_dc.
- relief_eta: expected receipt date for that relief; null if none.
- po_at_risk: true if inbound PO timing is at risk vs commitment.
- transferable_qty: units another DC could move toward dest_dc.
- transfer_cost: estimated USD cost to execute a transfer to dest_dc (from lookup; realistic network average ~3186 when DC-specific data unavailable).
- expected_penalty_cost: estimated USD penalty exposure if stockout occurs; null if not scored.
- penalty_risk_level: LOW / MEDIUM / HIGH risk of chargebacks/penalties; null if unknown.

Your job: choose the best operational recommendation balancing transfer cost vs waiting cost (stockout/penalty risk).

You MUST respond with a single JSON object and nothing else—no markdown fences, no commentary. Use this exact shape and allowed enum values:
{"action":"TRANSFER"|"WAIT"|"MONITOR","confidence":"HIGH"|"MED"|"LOW","reasoning":"<concise business reasoning>","cost_transfer":<float>,"cost_wait":<float>}

Rules:
- action TRANSFER: favor when transferable_qty supports moving stock and penalty/DoS risk justifies the move.
- Only recommend TRANSFER when transfer_cost < expected_penalty_cost.
- If transfer_cost exceeds expected_penalty_cost, recommend WAIT or MONITOR unless there is a material OTIF or customer-relationship factor that clearly overrides the math; if you use such an override, say so explicitly in reasoning.
- action WAIT: favor when relief is timely and sufficient, or when transfer cost outweighs benefit.
- action MONITOR: favor when information is insufficient or situation is borderline; keep reasoning explicit.
- cost_transfer and cost_wait must be non-negative floats (USD). They are your quantitative comparison of the two strategies for this event.
- reasoning must be one short paragraph (plain text inside JSON string, escape quotes).
"""

# Default transfer cost when lookup row missing (real $ average cited for the network).
DEFAULT_TRANSFER_COST_USD = 3186.0

# OpenRouter model id for Claude (see https://openrouter.ai/models). Override with OPENROUTER_MODEL.
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_OPENROUTER_CLAUDE_MODEL = "anthropic/claude-sonnet-4"

# =============================================================================
# Imports & implementation
# =============================================================================

import json
import os
import re
from typing import Any

import requests
from dotenv import load_dotenv
from supabase import Client

from services.workflow import create_supabase_client, transition_event_state

load_dotenv()

OPENROUTER_CHAT_COMPLETIONS_URL = f"{OPENROUTER_BASE_URL}/chat/completions"


def _create_client() -> Client:
    return create_supabase_client()


def _clean_env_text(value: Any, *, fallback: str | None = None) -> str | None:
    if value is None:
        return fallback
    cleaned = str(value).strip().strip("\"'“”‘’")
    return cleaned or fallback


def _openrouter_api_key() -> str:
    key = _clean_env_text(os.environ.get("OPENROUTER_API_KEY"))
    if not key:
        raise RuntimeError("OPENROUTER_API_KEY is not set.")
    return key


def _openrouter_model() -> str:
    return _clean_env_text(
        os.environ.get("OPENROUTER_MODEL"), fallback=DEFAULT_OPENROUTER_CLAUDE_MODEL
    ) or DEFAULT_OPENROUTER_CLAUDE_MODEL


def _openrouter_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_openrouter_api_key()}",
        "Content-Type": "application/json",
        "HTTP-Referer": _clean_env_text(
            os.environ.get("OPENROUTER_HTTP_REFERER"), fallback="http://localhost"
        )
        or "http://localhost",
        "X-Title": _clean_env_text(os.environ.get("OPENROUTER_APP_TITLE"), fallback="POP Supply Chain")
        or "POP Supply Chain",
    }


def build_analysis_request_payload(
    event: dict[str, Any],
    *,
    transfer_cost_usd: float,
) -> dict[str, Any]:
    """T8.1 — JSON body sent to Claude as the user message."""
    return {
        "sku_id": event.get("sku_id"),
        "dest_dc": event.get("dest_dc"),
        "days_of_supply": _num_or_none(event.get("days_of_supply")),
        "stockout_date": _date_str_or_none(event.get("stockout_date")),
        "relief_arriving": bool(event.get("relief_arriving")) if event.get("relief_arriving") is not None else False,
        "relief_eta": _date_str_or_none(event.get("relief_eta")),
        "po_at_risk": bool(event.get("po_at_risk")) if event.get("po_at_risk") is not None else False,
        "transferable_qty": _int_or_none(event.get("transferable_qty")),
        "transfer_cost": float(transfer_cost_usd),
        "expected_penalty_cost": _num_or_none(event.get("expected_penalty_cost")),
        "penalty_risk_level": _risk_level_str_or_none(event.get("penalty_risk_level")),
    }


def fetch_transfer_cost_avg(client: Client, dest_dc: str) -> float:
    """Real lookup: transfer_cost_lookup.avg_cost for dest_dc; else DEFAULT_TRANSFER_COST_USD."""
    try:
        response = client.table("transfer_cost_lookup").select("avg_cost").eq("dest_dc", dest_dc).limit(1).execute()
        rows = response.data or []
        if rows and rows[0].get("avg_cost") is not None:
            return float(rows[0]["avg_cost"])
    except Exception:
        pass
    return DEFAULT_TRANSFER_COST_USD


def fetch_event_by_id(client: Client, event_id: int) -> dict[str, Any] | None:
    response = client.table("events").select("*").eq("id", event_id).limit(1).execute()
    rows = response.data or []
    return rows[0] if rows else None


def _num_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _date_str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if "T" in s:
        s = s.split("T", 1)[0]
    return s[:10] if len(s) >= 10 else s


def _risk_level_str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip().upper()
    if s in ("LOW", "MEDIUM", "HIGH"):
        return "MEDIUM" if s == "MEDIUM" else s
    return None


def _assistant_message_text(content: Any) -> str:
    """Normalize chat completion message.content (string or content parts) to text."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text", "")))
            elif isinstance(block, str):
                parts.append(block)
        return "".join(parts)
    return str(content)


def _parse_claude_json(text: str) -> dict[str, Any]:
    text = text.strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if fence:
        text = fence.group(1).strip()
    return json.loads(text)


def _map_confidence_to_db(confidence: str) -> str:
    c = (confidence or "").strip().upper()
    if c == "MED":
        return "MEDIUM"
    if c in ("LOW", "MEDIUM", "HIGH"):
        return c
    return "LOW"


def apply_cost_proximity_confidence_override(cost_transfer: float, cost_wait: float, confidence_db: str) -> str:
    """T8.3 — if costs within 15%, force LOW."""
    try:
        ct = float(cost_transfer)
        cw = float(cost_wait)
    except (TypeError, ValueError):
        return "LOW"
    m = max(ct, cw, 0.0)
    if m <= 0:
        return confidence_db if confidence_db == "LOW" else "LOW"
    if abs(ct - cw) / m < 0.15:
        return "LOW"
    return confidence_db


def _map_action_to_db(action: str) -> str:
    a = (action or "").strip().upper()
    if a in ("TRANSFER", "WAIT", "MONITOR"):
        return a
    return "MONITOR"


def _enforce_transfer_cost_guardrail(
    *,
    action: str,
    confidence_db: str,
    reasoning: str,
    cost_transfer: float,
    expected_penalty_cost: float | None,
) -> tuple[str, str, str]:
    if action != "TRANSFER" or expected_penalty_cost is None:
        return action, confidence_db, reasoning
    if cost_transfer < expected_penalty_cost:
        return action, confidence_db, reasoning
    guardrail_reasoning = (
        "Transfer recommendation was overridden because modeled transfer cost exceeds expected penalty cost, "
        "and no quantified OTIF or relationship override is available in the event payload."
    )
    if reasoning:
        guardrail_reasoning = f"{reasoning} {guardrail_reasoning}"
    return "WAIT", confidence_db, guardrail_reasoning


def _fallback_reasoning(event: dict[str, Any], exc: Exception) -> str:
    base_reasoning = str(event.get("reasoning") or "").strip()
    failure_detail = str(exc).strip() or "AI provider unavailable."
    detail = failure_detail.splitlines()[0][:240]
    if base_reasoning:
        return (
            "Automated recommendation is currently unavailable, so this event remains in DETECTED state. "
            f"Base event context: {base_reasoning} "
            f"Analysis failure: {detail}. Retry analysis later."
        )
    return (
        "Automated recommendation is currently unavailable, so this event remains in DETECTED state. "
        f"Analysis failure: {detail}. Retry analysis later."
    )


def call_claude_analyze(payload: dict[str, Any]) -> dict[str, Any]:
    """Call Claude through OpenRouter using HTTP (requests); return parsed JSON dict."""
    user_content = json.dumps(payload, separators=(",", ":"))
    body = {
        "model": _openrouter_model(),
        "max_tokens": 2048,
        "messages": [
            {"role": "system", "content": AGENT_ANALYSIS_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
    }
    response = requests.post(
        OPENROUTER_CHAT_COMPLETIONS_URL,
        headers=_openrouter_headers(),
        json=body,
        timeout=120,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"OpenRouter HTTP {response.status_code}: {response.text[:2000]}"
        )
    data = response.json()
    choices = data.get("choices") or []
    if not choices:
        raise RuntimeError(f"OpenRouter returned no choices: {data!r}")
    message = (choices[0] or {}).get("message") or {}
    text = _assistant_message_text(message.get("content"))
    if not str(text).strip():
        raise RuntimeError("Empty assistant content from OpenRouter.")
    return _parse_claude_json(str(text))


def analyze_event(event_id: int, *, client: Client | None = None, actor: str = "system") -> dict[str, Any]:
    """
    T8.2 — Load event, build payload, call Claude via OpenRouter (requests), apply T8.3, update event to ACTION_PROPOSED.
    On API failure (T8.4): set ai_unavailable, message in reasoning, do not advance state.
    """
    own_client = client or _create_client()
    event = fetch_event_by_id(own_client, event_id)
    if not event:
        return {"ok": False, "error": "event_not_found", "message": "Event not found."}

    dest_dc = str(event.get("dest_dc") or "")
    transfer_cost = fetch_transfer_cost_avg(own_client, dest_dc)
    payload = build_analysis_request_payload(event, transfer_cost_usd=transfer_cost)
    print(
        "orchestrator: analyzing "
        f"event_id={event_id} sku={event.get('sku_id')} route={event.get('source_dc')}->{dest_dc} "
        f"transfer_cost={transfer_cost} expected_penalty_cost={event.get('expected_penalty_cost')}"
    )

    try:
        parsed = call_claude_analyze(payload)
    except Exception as exc:
        # T8.4 fallback
        fallback_reasoning = _fallback_reasoning(event, exc)
        fallback_wait_cost = _num_or_none(event.get("expected_penalty_cost")) or 0.0
        print(f"orchestrator: AI analysis failed for event_id={event_id}: {exc}")
        own_client.table("events").update(
            {
                "ai_unavailable": True,
                "recommended_action": "MONITOR",
                "confidence": "LOW",
                "reasoning": fallback_reasoning,
                "cost_transfer": float(transfer_cost),
                "cost_wait": float(fallback_wait_cost),
            }
        ).eq("id", event_id).execute()
        return {
            "ok": False,
            "error": "openrouter_unavailable",
            "message": fallback_reasoning,
            "detail": str(exc),
        }

    action = _map_action_to_db(str(parsed.get("action", "")))
    confidence_db = _map_confidence_to_db(str(parsed.get("confidence", "")))
    cost_transfer = float(parsed.get("cost_transfer", 0) or 0)
    cost_wait = float(parsed.get("cost_wait", 0) or 0)
    reasoning = str(parsed.get("reasoning") or "").strip()
    confidence_db = apply_cost_proximity_confidence_override(cost_transfer, cost_wait, confidence_db)
    original_action = action
    action, confidence_db, reasoning = _enforce_transfer_cost_guardrail(
        action=action,
        confidence_db=confidence_db,
        reasoning=reasoning,
        cost_transfer=cost_transfer,
        expected_penalty_cost=_num_or_none(payload.get("expected_penalty_cost")),
    )
    if original_action != action:
        print(
            "orchestrator: transfer guardrail override "
            f"event_id={event_id} from={original_action} to={action} "
            f"cost_transfer={cost_transfer} expected_penalty_cost={payload.get('expected_penalty_cost')}"
        )

    updated_event = transition_event_state(
        own_client,
        event=event,
        new_state="ACTION_PROPOSED",
        actor=actor,
        notes="Claude orchestrator recommendation generated.",
        updates={
            "recommended_action": action,
            "confidence": confidence_db,
            "reasoning": reasoning,
            "cost_transfer": cost_transfer,
            "cost_wait": cost_wait,
            "ai_unavailable": False,
        },
    )
    print(
        "orchestrator: analysis succeeded "
        f"event_id={event_id} action={action} confidence={confidence_db} "
        f"cost_transfer={cost_transfer} cost_wait={cost_wait}"
    )

    return {
        "ok": True,
        "event_id": event_id,
        "state": updated_event.get("state"),
        "recommended_action": action,
        "confidence": confidence_db,
        "reasoning": reasoning,
        "cost_transfer": cost_transfer,
        "cost_wait": cost_wait,
        "request_payload": payload,
    }
