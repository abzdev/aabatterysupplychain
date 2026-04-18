"""
T5 Demand agent: compute runway from Supabase `inventory_snapshots` + `sales_history`,
rank by `days_of_supply` ascending, and upsert `events` rows with a 61-point depletion projection.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import pandas as pd
from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

ALL_DCS: tuple[str, ...] = ("SF", "NJ", "LA")
DC_RANK = {dc: idx for idx, dc in enumerate(ALL_DCS)}


def _create_client() -> Client:
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


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


def _scalar(client: Client, table: str, column: str) -> Any:
    response = client.table(table).select(column).order(column, desc=True).limit(1).execute()
    batch = response.data or []
    if not batch:
        return None
    return batch[0].get(column)


def _parse_doc_date(value: Any) -> pd.Timestamp | pd.NaTType:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return pd.NaT
    return pd.to_datetime(value, errors="coerce")


def _build_projection(available: float, daily_demand: float, horizon_days: int) -> list[dict[str, float]]:
    projection: list[dict[str, float]] = []
    for day in range(0, horizon_days + 1):
        projection.append({"day": float(day), "available": float(available - day * daily_demand)})
    return projection


def _choose_dest_dc(
    sku_id: str,
    source_dc: str,
    metrics_by_dc: dict[str, pd.Series],
    *,
    horizon_days: int,
) -> str | None:
    best_dc: str | None = None
    best_surplus: float | None = None
    best_rank: int | None = None

    for dc, row in metrics_by_dc.items():
        if dc == source_dc:
            continue
        available = row.get("available")
        wdd = row.get("weighted_daily_demand")
        if available is None or wdd is None or pd.isna(available) or pd.isna(wdd):
            continue
        surplus = float(available) - float(wdd) * float(horizon_days)
        rank = DC_RANK.get(dc, 999)
        if best_dc is None:
            best_dc = dc
            best_surplus = surplus
            best_rank = rank
            continue
        assert best_surplus is not None and best_rank is not None
        if surplus > best_surplus or (surplus == best_surplus and rank < best_rank):
            best_dc = dc
            best_surplus = surplus
            best_rank = rank

    return best_dc


@dataclass
class DemandAgentConfig:
    horizon_days: int = 60
    demand_window_days: int = 30
    max_days_of_supply: float = 60.0
    page_size: int = 1000


class DemandAgent:
    def __init__(self, client: Client, config: DemandAgentConfig | None = None) -> None:
        self.client = client
        self.config = config or DemandAgentConfig()

    def _latest_snapshot_date(self) -> date:
        raw = _scalar(self.client, "inventory_snapshots", "snapshot_date")
        if raw is None:
            raise RuntimeError("inventory_snapshots is empty; cannot determine snapshot_date.")
        if isinstance(raw, str):
            return datetime.fromisoformat(raw).date()
        if isinstance(raw, datetime):
            return raw.date()
        return pd.to_datetime(raw).date()

    def _load_inventory(self, snapshot_date: date) -> pd.DataFrame:
        rows = _fetch_all_rows(
            self.client,
            "inventory_snapshots",
            select="sku_id,dc,available,on_hand,snapshot_date",
            filters={"snapshot_date": snapshot_date.isoformat()},
            page_size=self.config.page_size,
        )
        df = pd.DataFrame(rows)
        if df.empty:
            raise RuntimeError(f"No inventory rows for snapshot_date={snapshot_date.isoformat()}.")
        df["sku_id"] = df["sku_id"].astype("string").str.strip()
        df["dc"] = df["dc"].astype("string").str.strip()
        df["available"] = pd.to_numeric(df["available"], errors="coerce")
        df["on_hand"] = pd.to_numeric(df["on_hand"], errors="coerce")
        return df

    def _load_sales_window(self, start: date, end: date) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            response = (
                self.client.table("sales_history")
                .select("sku_id,dc,doc_date,quantity_adj")
                .gte("doc_date", start.isoformat())
                .lte("doc_date", end.isoformat())
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
        df["quantity_adj"] = pd.to_numeric(df["quantity_adj"], errors="coerce").fillna(0)
        df["doc_date"] = df["doc_date"].map(_parse_doc_date)
        df = df[df["sku_id"].notna() & df["dc"].isin(ALL_DCS) & df["doc_date"].notna()].copy()
        return df

    def build_events(self) -> pd.DataFrame:
        as_of = self._latest_snapshot_date()
        window_start = as_of - timedelta(days=self.config.demand_window_days - 1)

        inventory = self._load_inventory(as_of)
        sales = self._load_sales_window(window_start, as_of)

        if not sales.empty:
            demand = (
                sales.groupby(["sku_id", "dc"], as_index=False)["quantity_adj"]
                .sum()
                .rename(columns={"quantity_adj": "demand_30d"})
            )
        else:
            demand = pd.DataFrame(columns=["sku_id", "dc", "demand_30d"])

        merged = inventory.merge(demand, on=["sku_id", "dc"], how="left")
        merged["demand_30d"] = merged["demand_30d"].fillna(0)
        merged["weighted_daily_demand"] = merged["demand_30d"] / float(self.config.demand_window_days)

        merged["available_for_runway"] = merged["available"]
        merged = merged[merged["available_for_runway"].notna()].copy()

        merged["days_of_supply"] = merged.apply(
            lambda row: float("inf")
            if row["weighted_daily_demand"] <= 0
            else float(row["available_for_runway"]) / float(row["weighted_daily_demand"]),
            axis=1,
        )

        eligible = merged[
            (merged["weighted_daily_demand"] > 0)
            & merged["days_of_supply"].notna()
            & (merged["days_of_supply"] != float("inf"))
            & (merged["days_of_supply"] <= self.config.max_days_of_supply)
        ].copy()

        eligible.sort_values(
            by=["days_of_supply", "sku_id", "dc"],
            ascending=[True, True, True],
            kind="mergesort",
            inplace=True,
        )

        events: list[dict[str, Any]] = []
        for sku_id, sku_all in merged.groupby("sku_id", sort=False):
            metrics_by_dc = {row.dc: row for row in sku_all.itertuples(index=False)}
            sku_eligible = eligible[eligible["sku_id"] == sku_id]
            for row in sku_eligible.itertuples(index=False):
                source_dc = row.dc
                dest_dc = _choose_dest_dc(
                    sku_id,
                    source_dc,
                    metrics_by_dc,
                    horizon_days=self.config.horizon_days,
                )
                if dest_dc is None:
                    continue

                available = float(row.available_for_runway)
                wdd = float(row.weighted_daily_demand)
                dos = float(row.days_of_supply)
                event_key = f"{sku_id}|{source_dc}|{dest_dc}|{as_of.isoformat()}"
                projection = _build_projection(available, wdd, self.config.horizon_days)

                events.append(
                    {
                        "event_key": event_key,
                        "sku_id": sku_id,
                        "source_dc": source_dc,
                        "dest_dc": dest_dc,
                        "state": "DETECTED",
                        "days_of_supply": dos,
                        "depletion_projection": projection,
                        "reasoning": (
                            f"snapshot={as_of.isoformat()}; demand_window={self.config.demand_window_days}d "
                            f"ending {as_of.isoformat()}; dest chosen by max "
                            f"(available - wdd*{self.config.horizon_days}) among other DCs"
                        ),
                    }
                )

        return pd.DataFrame(events)

    def persist_events(self, events_df: pd.DataFrame) -> None:
        if events_df.empty:
            print("demand_agent: no events to upsert.")
            return

        records = json.loads(events_df.to_json(orient="records"))
        batch_size = 500
        for start in range(0, len(records), batch_size):
            batch = records[start : start + batch_size]
            print(f"demand_agent: upserting events rows {start}..{start + len(batch) - 1}")
            self.client.table("events").upsert(batch, on_conflict="event_key").execute()

        print(f"demand_agent: upserted {len(records)} events rows.")


def _print_preview(events_df: pd.DataFrame, *, limit: int) -> None:
    preview = events_df.head(limit)
    print(preview.to_string(index=False))
    if not events_df.empty:
        sample = events_df.iloc[0]
        projection = sample["depletion_projection"]
        print("\nSample projection length:", len(projection))
        print("First/last:", projection[0], projection[-1])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demand agent: build runway events from inventory + sales.")
    parser.add_argument("--dry-run", action="store_true", help="Compute and print ranked preview without upsert.")
    parser.add_argument("--limit", type=int, default=25, help="Rows to print in dry-run preview.")
    parser.add_argument("--max-dos", type=float, default=60.0, help="Only emit events with days_of_supply <= this.")
    parser.add_argument("--horizon-days", type=int, default=60, help="Projection horizon (and surplus lookahead).")
    parser.add_argument("--demand-window-days", type=int, default=30, help="Rolling demand window length in days.")
    parser.add_argument("--page-size", type=int, default=1000, help="Supabase pagination page size.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_KEY"):
        raise SystemExit("SUPABASE_URL and SUPABASE_KEY must be set (e.g. via .env) to run the demand agent.")

    client = _create_client()
    agent = DemandAgent(
        client,
        config=DemandAgentConfig(
            horizon_days=args.horizon_days,
            demand_window_days=args.demand_window_days,
            max_days_of_supply=args.max_dos,
            page_size=args.page_size,
        ),
    )

    events_df = agent.build_events()
    print(f"demand_agent: prepared {len(events_df)} events (ranked by days_of_supply ascending).")

    if args.dry_run:
        _print_preview(events_df, limit=args.limit)
        return

    agent.persist_events(events_df)


if __name__ == "__main__":
    main()
