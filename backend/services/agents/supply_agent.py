"""
T6 Supply agent: evaluate open PO relief for imbalance event candidates.

For each (sku_id, dest_dc) pair this agent:
  T6.1 — queries open POs from Supabase `po_history`
  T6.2 — selects the soonest non-delayed PO and returns relief fields
  T6.3 — suppresses the event when inbound relief arrives before the predicted
          stockout date (unless that PO itself is delayed)

Outputs a DataFrame of SupplyDecision records that the ImbalanceAgent uses
to filter its event list, and can optionally persist relief fields back onto
existing `events` rows.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from time import perf_counter
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

ALL_DCS: tuple[str, ...] = ("SF", "NJ", "LA")


# ---------------------------------------------------------------------------
# Supabase client
# ---------------------------------------------------------------------------

def _create_client() -> Client:
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


# ---------------------------------------------------------------------------
# Generic Supabase helpers (consistent with other agents)
# ---------------------------------------------------------------------------

def _fetch_all_rows(
    client: Client,
    table: str,
    *,
    select: str = "*",
    filters: dict[str, Any] | None = None,
    page_size: int = 1000,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    filters = filters or {}
    page_num = 0
    print(f"supply_agent [fetch] START table={table} filters={filters} page_size={page_size}")
    while True:
        page_num += 1
        print(f"supply_agent [fetch] table={table} page={page_num} offset={offset} total_so_far={len(rows)}")
        t0 = perf_counter()
        query = client.table(table).select(select)
        for column, value in filters.items():
            query = query.eq(column, value)
        response = query.range(offset, offset + page_size - 1).execute()
        elapsed_ms = round((perf_counter() - t0) * 1000)
        batch = response.data or []
        rows.extend(batch)
        print(
            f"supply_agent [fetch] table={table} page={page_num} "
            f"got={len(batch)} elapsed_ms={elapsed_ms} total_so_far={len(rows)}"
        )
        if len(batch) < page_size:
            print(f"supply_agent [fetch] DONE table={table} total_rows={len(rows)}")
            break
        offset += page_size
    return rows


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and value:
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _normalize_record_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class SupplyAgentConfig:
    # A PO is considered "timely" only if it arrives at least this many days
    # before the predicted stockout. Prevents suppressing events for POs that
    # arrive the same day stock runs out.
    relief_buffer_days: int = 1
    page_size: int = 1000


# ---------------------------------------------------------------------------
# Input / Output dataclasses (kept for ImbalanceAgent compatibility)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SupplyEventInput:
    """Describes one event candidate the supply agent must evaluate."""

    sku_id: str
    dest_dc: str
    stockout_date: date | None = None
    days_of_supply: float | None = None
    as_of_date: date | None = None

    def resolve_stockout_date(self, today: date) -> date | None:
        if self.stockout_date is not None:
            return self.stockout_date
        if self.days_of_supply is None:
            return None
        base = self.as_of_date or today
        return base + timedelta(days=max(0, int(self.days_of_supply)))


@dataclass(frozen=True)
class SupplyDecision:
    """Result for one (sku_id, dest_dc) event candidate."""

    sku_id: str
    dest_dc: str
    relief_arriving: bool
    relief_eta: date | None
    relief_qty: int | None
    is_delayed: bool
    suppress_event: bool
    po_at_risk: bool
    selected_po_number: int | None


# ---------------------------------------------------------------------------
# Core agent class
# ---------------------------------------------------------------------------

class SupplyAgent:
    def __init__(self, client: Client, config: SupplyAgentConfig | None = None) -> None:
        self.client = client
        self.config = config or SupplyAgentConfig()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_open_pos(self, sku_id: str, dest_dc: str, today: date) -> pd.DataFrame:
        """
        Pull open POs for a specific SKU + DC.
        A PO is open if receipt_date is null (not yet received) or in the future.
        """
        rows: list[dict[str, Any]] = []
        offset = 0
        page_num = 0
        print(f"supply_agent [open_pos] START sku={sku_id} dc={dest_dc} today={today.isoformat()}")
        while True:
            page_num += 1
            print(f"supply_agent [open_pos] sku={sku_id} dc={dest_dc} page={page_num} offset={offset}")
            t0 = perf_counter()
            response = (
                self.client.table("po_history")
                .select(
                    "po_number,sku_id,dc,qty_ordered,qty_received,"
                    "required_date,promised_ship_date,receipt_date,ship_to_address"
                )
                .eq("sku_id", sku_id)
                .eq("dc", dest_dc)
                .or_(f"receipt_date.is.null,receipt_date.gt.{today.isoformat()}")
                .range(offset, offset + self.config.page_size - 1)
                .execute()
            )
            elapsed_ms = round((perf_counter() - t0) * 1000)
            batch = response.data or []
            rows.extend(batch)
            print(
                f"supply_agent [open_pos] sku={sku_id} dc={dest_dc} page={page_num} "
                f"got={len(batch)} elapsed_ms={elapsed_ms} total_so_far={len(rows)}"
            )
            if len(batch) < self.config.page_size:
                print(f"supply_agent [open_pos] DONE sku={sku_id} dc={dest_dc} total_pos={len(rows)}")
                break
            offset += self.config.page_size

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df["sku_id"] = df["sku_id"].astype("string").str.strip()
        df["dc"] = df["dc"].astype("string").str.strip()
        df["qty_ordered"] = pd.to_numeric(df.get("qty_ordered"), errors="coerce").fillna(0)
        df["qty_received"] = pd.to_numeric(df.get("qty_received"), errors="coerce").fillna(0)
        df["open_qty"] = (df["qty_ordered"] - df["qty_received"]).clip(lower=0).astype(int)
        df["required_date"] = df["required_date"].map(_parse_date)
        df["promised_ship_date"] = df["promised_ship_date"].map(_parse_date)
        df["receipt_date"] = df["receipt_date"].map(_parse_date)

        # ETA: prefer receipt_date, fall back to promised_ship_date, then required_date
        df["eta"] = df.apply(
            lambda r: r["receipt_date"] or r["promised_ship_date"] or r["required_date"],
            axis=1,
        )
        # A PO is delayed if its receipt_date is past its required_date
        df["delayed"] = df.apply(
            lambda r: (
                r["required_date"] is not None
                and r["receipt_date"] is not None
                and r["receipt_date"] > r["required_date"]
            ),
            axis=1,
        )
        return df

    def _load_all_events(self) -> pd.DataFrame:
        """Load all events from Supabase for batch processing."""
        print("supply_agent [events] START loading all events")
        t0 = perf_counter()
        rows = _fetch_all_rows(
            self.client,
            "events",
            select="id,event_key,sku_id,dest_dc,days_of_supply,stockout_date,state",
            page_size=self.config.page_size,
        )
        elapsed_ms = round((perf_counter() - t0) * 1000)
        print(f"supply_agent [events] DONE loaded={len(rows)} elapsed_ms={elapsed_ms}")
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["sku_id"] = df["sku_id"].astype("string").str.strip()
        df["dest_dc"] = df["dest_dc"].astype("string").str.strip()
        df["days_of_supply"] = pd.to_numeric(df["days_of_supply"], errors="coerce")
        df["stockout_date"] = df["stockout_date"].map(_parse_date)
        return df

    # ------------------------------------------------------------------
    # Decision logic
    # ------------------------------------------------------------------

    def _select_relief_po(self, pos_df: pd.DataFrame) -> pd.Series | None:
        """
        T6.2: pick the soonest non-delayed PO with a known ETA and positive open qty.
        On tie, prefer larger relief quantity, then lower PO number.
        """
        candidates = pos_df[
            (~pos_df["delayed"]) & pos_df["eta"].notna() & (pos_df["open_qty"] > 0)
        ].copy()
        if candidates.empty:
            return None
        candidates = candidates.sort_values(
            by=["eta", "open_qty", "po_number"],
            ascending=[True, False, True],
        )
        return candidates.iloc[0]

    def _decide(
        self,
        event: SupplyEventInput,
        pos_df: pd.DataFrame,
        today: date,
    ) -> SupplyDecision:
        """Apply T6.1 / T6.2 / T6.3 logic for one event candidate."""
        is_delayed = bool((~pos_df.empty) and pos_df["delayed"].any())
        best_po = self._select_relief_po(pos_df) if not pos_df.empty else None

        relief_arriving = best_po is not None
        relief_eta: date | None = best_po["eta"] if best_po is not None else None
        relief_qty: int | None = int(best_po["open_qty"]) if best_po is not None else None
        selected_po_number: int | None = (
            int(best_po["po_number"]) if best_po is not None and pd.notna(best_po["po_number"]) else None
        )

        stockout_date = event.resolve_stockout_date(today)
        suppress_event = False
        po_at_risk = False

        # T6.3: suppress only when relief arrives strictly before stockout
        if relief_eta is not None and stockout_date is not None:
            arrives_in_time = relief_eta <= stockout_date - timedelta(days=self.config.relief_buffer_days)
            if arrives_in_time:
                if is_delayed:
                    # Relief is coming but may slip — keep event alive, flag PO risk
                    po_at_risk = True
                else:
                    suppress_event = True

        return SupplyDecision(
            sku_id=event.sku_id,
            dest_dc=event.dest_dc,
            relief_arriving=relief_arriving,
            relief_eta=relief_eta,
            relief_qty=relief_qty,
            is_delayed=is_delayed,
            suppress_event=suppress_event,
            po_at_risk=po_at_risk,
            selected_po_number=selected_po_number,
        )

    # ------------------------------------------------------------------
    # Public API: evaluate a single event (used by ImbalanceAgent inline)
    # ------------------------------------------------------------------

    def evaluate_event(
        self,
        event: SupplyEventInput,
        *,
        today: date | None = None,
    ) -> SupplyDecision:
        as_of = today or date.today()
        pos_df = self._load_open_pos(event.sku_id, event.dest_dc, as_of)
        return self._decide(event, pos_df, as_of)

    # ------------------------------------------------------------------
    # Public API: batch evaluate all events and return a DataFrame
    # ------------------------------------------------------------------

    def build_supply_decisions(self, today: date | None = None) -> pd.DataFrame:
        """
        Load all events from Supabase, evaluate supply relief for each,
        and return a DataFrame of SupplyDecision results.
        """
        as_of = today or date.today()
        events_df = self._load_all_events()
        if events_df.empty:
            print("supply_agent: no events found in Supabase.")
            return pd.DataFrame()

        print(f"supply_agent: evaluating supply relief for {len(events_df)} events as of {as_of.isoformat()}.")

        decisions: list[dict[str, Any]] = []
        counters = {"suppressed": 0, "po_at_risk": 0, "no_relief": 0, "relief_ok": 0}

        for idx, row in enumerate(events_df.itertuples(index=False)):
            print(
                f"supply_agent [decision] event {idx + 1}/{len(events_df)} "
                f"sku={row.sku_id} dc={row.dest_dc} dos={row.days_of_supply} stockout={row.stockout_date}"
            )
            t0 = perf_counter()
            event = SupplyEventInput(
                sku_id=row.sku_id,
                dest_dc=row.dest_dc,
                stockout_date=row.stockout_date,
                days_of_supply=row.days_of_supply,
                as_of_date=as_of,
            )
            pos_df = self._load_open_pos(row.sku_id, row.dest_dc, as_of)
            decision = self._decide(event, pos_df, as_of)
            elapsed_ms = round((perf_counter() - t0) * 1000)
            print(
                f"supply_agent [decision] event {idx + 1}/{len(events_df)} "
                f"sku={row.sku_id} dc={row.dest_dc} "
                f"suppress={decision.suppress_event} relief={decision.relief_arriving} "
                f"po_at_risk={decision.po_at_risk} relief_eta={decision.relief_eta} "
                f"elapsed_ms={elapsed_ms}"
            )

            if decision.suppress_event:
                counters["suppressed"] += 1
            elif decision.po_at_risk:
                counters["po_at_risk"] += 1
            elif decision.relief_arriving:
                counters["relief_ok"] += 1
            else:
                counters["no_relief"] += 1

            decisions.append({
                "event_id": int(row.id),
                "event_key": row.event_key,
                "sku_id": decision.sku_id,
                "dest_dc": decision.dest_dc,
                "relief_arriving": decision.relief_arriving,
                "relief_eta": decision.relief_eta.isoformat() if decision.relief_eta else None,
                "relief_qty": decision.relief_qty,
                "is_delayed": decision.is_delayed,
                "suppress_event": decision.suppress_event,
                "po_at_risk": decision.po_at_risk,
                "selected_po_number": decision.selected_po_number,
            })

        print(
            f"supply_agent: complete — "
            f"suppressed={counters['suppressed']} po_at_risk={counters['po_at_risk']} "
            f"no_relief={counters['no_relief']} relief_ok={counters['relief_ok']}"
        )
        return pd.DataFrame(decisions)

    # ------------------------------------------------------------------
    # Persist supply fields back onto events
    # ------------------------------------------------------------------

    def persist_supply_decisions(self, decisions_df: pd.DataFrame) -> None:
        if decisions_df.empty:
            print("supply_agent: no decisions to persist.")
            return

        records = [
            {key: _normalize_record_value(value) for key, value in row.items()}
            for row in decisions_df.to_dict(orient="records")
        ]

        batch_size = 500
        for start in range(0, len(records), batch_size):
            batch = records[start: start + batch_size]
            batch_started_at = perf_counter()
            print(
                f"supply_agent: upserting rows "
                f"{start}..{start + len(batch) - 1} batch_size={len(batch)}"
            )
            t0 = perf_counter()
            self.client.table("supply_decisions").upsert(
                batch,
                on_conflict="event_key",
            ).execute()
            elapsed_ms = round((perf_counter() - t0) * 1000)
            total_elapsed_ms = round((perf_counter() - batch_started_at) * 1000)
            print(
                f"supply_agent: batch complete "
                f"rows {start}..{start + len(batch) - 1} "
                f"upsert_ms={elapsed_ms} total_ms={total_elapsed_ms}"
            )

        print(f"supply_agent: persisted {len(records)} supply decision rows.")


# ---------------------------------------------------------------------------
# Convenience function used by ImbalanceAgent (backward-compatible)
# ---------------------------------------------------------------------------

def evaluate_supply_for_event(
    event: SupplyEventInput,
    client: Client,
    *,
    today: date | None = None,
    config: SupplyAgentConfig | None = None,
) -> SupplyDecision:
    """
    Thin wrapper so ImbalanceAgent can call supply logic inline without
    constructing a full SupplyAgent instance.
    """
    agent = SupplyAgent(client, config=config)
    return agent.evaluate_event(event, today=today)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_preview(decisions_df: pd.DataFrame, *, limit: int) -> None:
    if decisions_df.empty:
        print("supply_agent: no decisions produced.")
        return
    print(decisions_df.head(limit).to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Supply agent (T6): evaluate open PO relief for imbalance events."
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute and print decisions without persisting.")
    parser.add_argument("--limit", type=int, default=25, help="Rows to print in dry-run preview.")
    parser.add_argument("--relief-buffer-days", type=int, default=1, help="Days of buffer required before stockout.")
    parser.add_argument("--page-size", type=int, default=1000, help="Supabase pagination page size.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_KEY"):
        raise SystemExit("SUPABASE_URL and SUPABASE_KEY must be set (e.g. via .env) to run the supply agent.")

    client = _create_client()
    agent = SupplyAgent(
        client,
        config=SupplyAgentConfig(
            relief_buffer_days=args.relief_buffer_days,
            page_size=args.page_size,
        ),
    )

    decisions_df = agent.build_supply_decisions()
    print(f"supply_agent: prepared {len(decisions_df)} supply decisions.")

    if args.dry_run:
        _print_preview(decisions_df, limit=args.limit)
        return

    agent.persist_supply_decisions(decisions_df)


if __name__ == "__main__":
    main()