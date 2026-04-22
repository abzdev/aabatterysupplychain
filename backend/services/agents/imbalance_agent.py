"""
T4 Imbalance agent:
- T4.1 consume low-stock hits from DemandAgent output DataFrame
- T4.2 confirm cross-DC transferable surplus exists
- T4.3 suppress if timely inbound PO exists at destination
- T4.4 emit DETECTED event records for confirmed imbalances
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, datetime
from time import perf_counter
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from postgrest.base_request_builder import ReturnMethod
from supabase import Client, create_client

from .demand_agent import DemandAgent, DemandAgentConfig
from .supply_agent import SupplyEventInput, evaluate_supply_for_event

load_dotenv()


def _create_client() -> Client:
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and value:
        return datetime.fromisoformat(value).date()
    return None


def _date_to_iso(value: date | None) -> str | None:
    if value is None or pd.isna(value):
        return None
    return value.isoformat()


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _normalize_projection(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return value
    return []


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


def _records_for_upsert(events_df: pd.DataFrame) -> list[dict[str, Any]]:
    integer_fields = {"transferable_qty", "network_total", "relief_qty"}
    records: list[dict[str, Any]] = []
    for row in events_df.to_dict(orient="records"):
        normalized: dict[str, Any] = {}
        for key, value in row.items():
            cleaned = _normalize_record_value(value)
            if key in integer_fields and cleaned is not None:
                normalized[key] = int(float(cleaned))
            else:
                normalized[key] = cleaned
        records.append(normalized)
    return records


@dataclass
class ImbalanceAgentConfig:
    demand_window_days: int = 30
    max_days_of_supply: float = 60.0
    page_size: int = 1000


class ImbalanceAgent:
    def __init__(self, client: Client, config: ImbalanceAgentConfig | None = None) -> None:
        self.client = client
        self.config = config or ImbalanceAgentConfig()
        self.po_repository = client

    def _latest_snapshot_date(self) -> date:
        response = self.client.table("inventory_snapshots").select("snapshot_date").order(
            "snapshot_date", desc=True
        ).limit(1).execute()
        rows = response.data or []
        if not rows:
            raise RuntimeError("inventory_snapshots is empty; cannot run imbalance agent.")
        snapshot_date = _parse_date(rows[0].get("snapshot_date"))
        if snapshot_date is None:
            raise RuntimeError("Failed to parse latest snapshot_date from inventory_snapshots.")
        return snapshot_date

    def _load_inventory(self, snapshot_date: date) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            response = (
                self.client.table("inventory_snapshots")
                .select("sku_id,dc,available,on_hand,snapshot_date")
                .eq("snapshot_date", snapshot_date.isoformat())
                .range(offset, offset + self.config.page_size - 1)
                .execute()
            )
            batch = response.data or []
            rows.extend(batch)
            if len(batch) < self.config.page_size:
                break
            offset += self.config.page_size

        df = pd.DataFrame(rows)
        if df.empty:
            return df

        df["sku_id"] = df["sku_id"].astype("string").str.strip()
        df["dc"] = df["dc"].astype("string").str.strip()
        df["available"] = pd.to_numeric(df["available"], errors="coerce").fillna(0)
        df["on_hand"] = pd.to_numeric(df["on_hand"], errors="coerce").fillna(0)
        # Transferable stock proxy until a dedicated transferable model is introduced.
        df["transferable_qty"] = df["available"].clip(lower=0)
        return df

    def _normalize_low_stock_hits(self, demand_df: pd.DataFrame, as_of_date: date) -> pd.DataFrame:
        if demand_df.empty:
            return pd.DataFrame(
                columns=[
                    "sku_id",
                    "dest_dc",
                    "days_of_supply",
                    "stockout_date",
                    "as_of_date",
                    "depletion_projection",
                ]
            )

        df = demand_df.copy()
        df["sku_id"] = df["sku_id"].astype("string").str.strip()

        if "dc" in df.columns:
            df["dest_dc"] = df["dc"].astype("string").str.strip()
        elif "source_dc" in df.columns:
            # Demand agent currently labels the critical site as source_dc.
            df["dest_dc"] = df["source_dc"].astype("string").str.strip()
        else:
            raise ValueError("Demand output must contain either `dc` or `source_dc` for critical DC.")

        if "days_of_supply" not in df.columns:
            raise ValueError("Demand output must include `days_of_supply`.")

        df["days_of_supply"] = pd.to_numeric(df["days_of_supply"], errors="coerce")
        if "stockout_date" in df.columns:
            df["stockout_date"] = pd.to_datetime(df["stockout_date"], errors="coerce").dt.date
        else:
            df["stockout_date"] = pd.NaT
        if "depletion_projection" not in df.columns:
            df["depletion_projection"] = [[] for _ in range(len(df))]
        else:
            df["depletion_projection"] = df["depletion_projection"].map(_normalize_projection)
        df["as_of_date"] = as_of_date

        normalized = df[
            [
                "sku_id",
                "dest_dc",
                "days_of_supply",
                "stockout_date",
                "as_of_date",
                "depletion_projection",
            ]
        ].dropna(subset=["sku_id", "dest_dc", "days_of_supply"])
        print(
            "imbalance_agent: normalized "
            f"{len(normalized)} low-stock hits from {len(df)} demand rows for snapshot {as_of_date.isoformat()}."
        )
        return normalized

    def _choose_source_dc(self, sku_inventory: pd.DataFrame, dest_dc: str) -> tuple[str | None, int, int]:
        others = sku_inventory[sku_inventory["dc"] != dest_dc].copy()
        if others.empty:
            return None, 0, 0

        network_total_other = int(others["available"].clip(lower=0).sum())
        transferable = others[others["transferable_qty"] > 0].copy()
        if transferable.empty:
            return None, network_total_other, 0

        chosen = transferable.sort_values(["transferable_qty", "available", "dc"], ascending=[False, False, True]).iloc[0]
        source_dc = str(chosen["dc"])
        transferable_qty = int(chosen["transferable_qty"])
        return source_dc, network_total_other, transferable_qty

    def build_events(self, demand_hits_df: pd.DataFrame | None = None) -> pd.DataFrame:
        """
        Build imbalance events using T4.1-4.4 logic.

        If `demand_hits_df` is omitted, it is sourced from DemandAgent.build_events().
        """
        as_of_date = self._latest_snapshot_date()
        inventory_df = self._load_inventory(as_of_date)
        if inventory_df.empty:
            print("imbalance_agent: inventory snapshot is empty; no imbalance events can be built.")
            return pd.DataFrame()
        print(
            "imbalance_agent: loaded "
            f"{len(inventory_df)} inventory rows across {inventory_df['sku_id'].nunique()} SKUs "
            f"for snapshot {as_of_date.isoformat()}."
        )

        if demand_hits_df is None:
            demand_agent = DemandAgent(
                self.client,
                config=DemandAgentConfig(
                    demand_window_days=self.config.demand_window_days,
                    max_days_of_supply=self.config.max_days_of_supply,
                    page_size=self.config.page_size,
                ),
            )
            demand_hits_df = demand_agent.build_events()

        low_stock_hits = self._normalize_low_stock_hits(demand_hits_df, as_of_date)
        if low_stock_hits.empty:
            print("imbalance_agent: no normalized low-stock hits to evaluate.")
            return pd.DataFrame()

        events: list[dict[str, Any]] = []
        counters = {
            "missing_inventory": 0,
            "no_transfer_source": 0,
            "suppressed_by_supply": 0,
            "emitted": 0,
        }

        for hit in low_stock_hits.itertuples(index=False):
            sku_inventory = inventory_df[inventory_df["sku_id"] == hit.sku_id]
            if sku_inventory.empty:
                counters["missing_inventory"] += 1
                print(
                    "imbalance_agent: skip "
                    f"sku={hit.sku_id} dest={hit.dest_dc} reason=no_inventory_rows_for_sku"
                )
                continue

            source_dc, network_total_other, transferable_qty = self._choose_source_dc(
                sku_inventory, hit.dest_dc
            )
            # T4.2: require network stock at another DC and positive transferable stock.
            if source_dc is None or network_total_other <= 0 or transferable_qty <= 0:
                counters["no_transfer_source"] += 1
                print(
                    "imbalance_agent: skip "
                    f"sku={hit.sku_id} dest={hit.dest_dc} reason=no_transferable_surplus "
                    f"network_total_other={network_total_other} transferable_qty={transferable_qty}"
                )
                continue

            event_input = SupplyEventInput(
                sku_id=hit.sku_id,
                dest_dc=hit.dest_dc,
                stockout_date=_parse_date(hit.stockout_date),
                days_of_supply=_to_float(hit.days_of_supply),
                as_of_date=_parse_date(hit.as_of_date),
            )
            supply = evaluate_supply_for_event(event_input, self.po_repository, today=as_of_date)

            # T4.3: suppress when inbound relief arrives before stockout.
            if supply.suppress_event:
                counters["suppressed_by_supply"] += 1
                print(
                    "imbalance_agent: suppress "
                    f"sku={hit.sku_id} source={source_dc} dest={hit.dest_dc} "
                    f"reason=timely_inbound_relief relief_eta={_date_to_iso(supply.relief_eta)} "
                    f"stockout_date={_date_to_iso(event_input.resolve_stockout_date(as_of_date))}"
                )
                continue

            resolved_stockout_date = event_input.resolve_stockout_date(as_of_date)
            event_key = f"{hit.sku_id}|{source_dc}|{hit.dest_dc}|{as_of_date.isoformat()}|imbalance"
            counters["emitted"] += 1
            print(
                "imbalance_agent: emit "
                f"event_key={event_key} days_of_supply={round(float(hit.days_of_supply), 2)} "
                f"transferable_qty={transferable_qty} network_total_other={network_total_other} "
                f"relief_arriving={supply.relief_arriving} relief_eta={_date_to_iso(supply.relief_eta)} "
                f"po_at_risk={supply.po_at_risk}"
            )
            events.append(
                {
                    "event_key": event_key,
                    "state": "DETECTED",
                    "sku_id": hit.sku_id,
                    "source_dc": source_dc,
                    "dest_dc": hit.dest_dc,
                    "days_of_supply": round(float(hit.days_of_supply), 2),
                    "transferable_qty": transferable_qty,
                    "stockout_date": _date_to_iso(resolved_stockout_date),
                    "depletion_projection": _normalize_projection(hit.depletion_projection),
                    "network_total": network_total_other,
                    "relief_arriving": supply.relief_arriving,
                    "relief_eta": _date_to_iso(supply.relief_eta),
                    "relief_qty": supply.relief_qty,
                    "po_at_risk": supply.po_at_risk,
                    "reasoning": (
                        "T4 imbalance confirmed: critical destination with low DoS, "
                        "alternate source has transferable stock, and no timely inbound PO."
                    ),
                }
            )

        print(
            "imbalance_agent: summary "
            f"low_stock_hits={len(low_stock_hits)} emitted={counters['emitted']} "
            f"missing_inventory={counters['missing_inventory']} "
            f"no_transfer_source={counters['no_transfer_source']} "
            f"suppressed_by_supply={counters['suppressed_by_supply']}"
        )
        return pd.DataFrame(events)

    def persist_events(self, events_df: pd.DataFrame) -> None:
        if events_df.empty:
            print("imbalance_agent: no events to upsert.")
            return

        records = _records_for_upsert(events_df)
        batch_size = 500
        for start in range(0, len(records), batch_size):
            batch = records[start : start + batch_size]
            batch_started_at = perf_counter()
            print(
                "imbalance_agent: upserting rows "
                f"{start}..{start + len(batch) - 1} batch_size={len(batch)}"
            )
            self.client.table("events").upsert(
                batch,
                on_conflict="event_key",
                returning=ReturnMethod.minimal,
            ).execute()
            batch_elapsed_ms = round((perf_counter() - batch_started_at) * 1000)
            print(
                "imbalance_agent: upsert batch complete "
                f"rows {start}..{start + len(batch) - 1} elapsed_ms={batch_elapsed_ms}"
            )
        print(f"imbalance_agent: upserted {len(records)} event rows.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Imbalance agent (T4): detect transferable stock imbalances.")
    parser.add_argument("--dry-run", action="store_true", help="Build and print event preview without upsert.")
    parser.add_argument("--limit", type=int, default=25, help="Rows to print in dry-run preview.")
    parser.add_argument("--max-dos", type=float, default=60.0, help="Demand max DoS threshold.")
    parser.add_argument("--demand-window-days", type=int, default=30, help="Demand rolling window in days.")
    parser.add_argument("--page-size", type=int, default=1000, help="Supabase pagination page size.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_KEY"):
        raise SystemExit("SUPABASE_URL and SUPABASE_KEY must be set (e.g. via .env) to run the imbalance agent.")

    client = _create_client()
    agent = ImbalanceAgent(
        client,
        config=ImbalanceAgentConfig(
            demand_window_days=args.demand_window_days,
            max_days_of_supply=args.max_dos,
            page_size=args.page_size,
        ),
    )
    events_df = agent.build_events()
    print(f"imbalance_agent: prepared {len(events_df)} events.")

    if args.dry_run:
        preview = events_df.head(args.limit)
        print(preview.to_string(index=False))
        return

    agent.persist_events(events_df)


if __name__ == "__main__":
    main()
