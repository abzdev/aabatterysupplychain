"""
T7 Penalty agent: compute interpretable empirical penalty indices from filtered
chargebacks and estimate per-event expected penalty cost from penalty history.

This agent does not create events and does not collapse the indices into an
arbitrary composite score. It returns per-event payloads for downstream Claude
orchestrator use and can optionally persist `expected_penalty_cost` onto events.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

ALL_DCS: tuple[str, ...] = ("SF", "NJ", "LA")
LOCATION_CODE_TO_DC = {1: "SF", 2: "NJ", 3: "LA"}
CHARGEBACK_CAUSE_CODES = {"CRED11-F", "CRED11-O", "CRED08", "CRED12"}
GLOBAL_PENALTY_BASELINE = 680.0


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
    rows = response.data or []
    if not rows:
        return None
    return rows[0].get(column)


def _parse_date(value: Any) -> pd.Timestamp | pd.NaTType:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return pd.NaT
    return pd.to_datetime(value, errors="coerce")


def _weighted_lookup_average(
    lookup: dict[Any, float],
    weights: pd.Series,
    *,
    fallback: float,
) -> float:
    if weights.empty:
        return float(fallback)
    weighted_total = 0.0
    total_weight = 0.0
    for key, weight in weights.items():
        if key not in lookup or pd.isna(lookup[key]):
            continue
        weighted_total += float(lookup[key]) * float(weight)
        total_weight += float(weight)
    if total_weight == 0:
        return float(fallback)
    return weighted_total / total_weight


@dataclass
class PenaltyAgentConfig:
    sales_window_days: int = 90
    page_size: int = 1000


class PenaltyAgent:
    def __init__(self, client: Client, config: PenaltyAgentConfig | None = None) -> None:
        self.client = client
        self.config = config or PenaltyAgentConfig()

    def _latest_sales_date(self) -> date | None:
        raw = _scalar(self.client, "sales_history", "doc_date")
        if raw is None:
            return None
        if isinstance(raw, str):
            return datetime.fromisoformat(raw).date()
        if isinstance(raw, datetime):
            return raw.date()
        return pd.to_datetime(raw).date()

    def _load_events(self, event_ids: list[int] | None = None) -> pd.DataFrame:
        rows = _fetch_all_rows(
            self.client,
            "events",
            select="id,event_key,sku_id,source_dc,dest_dc,days_of_supply,stockout_date,state",
            page_size=self.config.page_size,
        )
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        if event_ids is not None:
            wanted_ids = {int(event_id) for event_id in event_ids}
            df = df[df["id"].isin(wanted_ids)].copy()
            print(
                f"penalty_agent: narrowed event set to {len(df)} rows using {len(wanted_ids)} requested event ids."
            )
            if df.empty:
                return df
        df["sku_id"] = df["sku_id"].astype("string").str.strip()
        df["source_dc"] = df["source_dc"].astype("string").str.strip()
        return df[df["sku_id"].notna() & df["source_dc"].isin(ALL_DCS)].copy()

    @staticmethod
    def _risk_score_from_indexes(
        *,
        channel_penalty_index: float,
        customer_penalty_index: float,
        dc_penalty_index: float,
        penalty_type_index: float,
        expected_penalty_cost: float,
        global_penalty_avg: float,
    ) -> float:
        baseline = max(float(global_penalty_avg), 1.0)
        blended_exposure = (
            float(channel_penalty_index)
            + float(customer_penalty_index)
            + float(dc_penalty_index)
            + float(penalty_type_index)
            + float(expected_penalty_cost)
        ) / 5.0
        return round(min(max(blended_exposure / (baseline * 2.0), 0.0), 1.0), 4)

    @staticmethod
    def _risk_level_from_score(score: float) -> str:
        if score >= 0.67:
            return "HIGH"
        if score >= 0.34:
            return "MEDIUM"
        return "LOW"

    def _load_customer_dc_mapping(self) -> pd.DataFrame:
        rows = _fetch_all_rows(
            self.client,
            "customer_dc_mapping",
            select="customer_number,primary_dc,customer_type",
            page_size=self.config.page_size,
        )
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["customer_number"] = df["customer_number"].astype("string").str.strip()
        df["primary_dc"] = df["primary_dc"].astype("string").str.strip()
        df["customer_type"] = df["customer_type"].astype("string").str.strip().fillna("UNKNOWN")
        return df

    def _load_chargebacks(self, customer_map: pd.DataFrame) -> pd.DataFrame:
        rows = _fetch_all_rows(
            self.client,
            "chargebacks",
            select="customer_number,location_code,cause_code,item_description,penalty_category,extended_price",
            page_size=self.config.page_size,
        )
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["customer_number"] = df["customer_number"].astype("string").str.strip()
        df["cause_code"] = df["cause_code"].astype("string").str.strip()
        df = df[df["cause_code"].isin(CHARGEBACK_CAUSE_CODES)].copy()
        df["location_code"] = pd.to_numeric(df["location_code"], errors="coerce").astype("Int64")
        df["dc"] = df["location_code"].map(LOCATION_CODE_TO_DC)
        df["extended_price"] = pd.to_numeric(df["extended_price"], errors="coerce")
        df["penalty_category"] = (
            df["penalty_category"].astype("string").str.strip().fillna("OTHER")
        )
        df = df.merge(customer_map[["customer_number", "customer_type"]], on="customer_number", how="left")
        df["customer_type"] = df["customer_type"].astype("string").str.strip().fillna("UNKNOWN")
        return df[df["extended_price"].notna()].copy()

    def _load_penalty_history(self, customer_map: pd.DataFrame) -> pd.DataFrame:
        rows = _fetch_all_rows(
            self.client,
            "penalty_history",
            select="customer_number,sku_id,extended_price",
            page_size=self.config.page_size,
        )
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["customer_number"] = df["customer_number"].astype("string").str.strip()
        df["sku_id"] = df["sku_id"].astype("string").str.strip()
        df["extended_price"] = pd.to_numeric(df["extended_price"], errors="coerce")
        df = df.merge(
            customer_map[["customer_number", "primary_dc"]].rename(columns={"primary_dc": "dc"}),
            on="customer_number",
            how="left",
        )
        return df[df["sku_id"].notna() & df["extended_price"].notna()].copy()

    def _load_sales_window(self, end_date: date | None) -> pd.DataFrame:
        if end_date is None:
            return pd.DataFrame(columns=["sku_id", "dc", "customer_number", "customer_type", "doc_date"])
        start_date = end_date - timedelta(days=self.config.sales_window_days - 1)
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            response = (
                self.client.table("sales_history")
                .select("sku_id,dc,customer_number,customer_type,doc_date")
                .gte("doc_date", start_date.isoformat())
                .lte("doc_date", end_date.isoformat())
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
        df["customer_number"] = df["customer_number"].astype("string").str.strip()
        df["customer_type"] = df["customer_type"].astype("string").str.strip().fillna("UNKNOWN")
        df["doc_date"] = df["doc_date"].map(_parse_date)
        return df[
            df["sku_id"].notna()
            & df["dc"].isin(ALL_DCS)
            & df["customer_number"].notna()
            & df["doc_date"].notna()
        ].copy()

    def _build_penalty_indexes(
        self, chargebacks: pd.DataFrame
    ) -> tuple[dict[Any, float], dict[Any, float], dict[Any, float], dict[Any, float], float]:
        channel_lookup = chargebacks.groupby("customer_type")["extended_price"].mean().to_dict()
        customer_lookup = chargebacks.groupby("customer_number")["extended_price"].mean().to_dict()
        dc_lookup = chargebacks.groupby("dc")["extended_price"].mean().to_dict()
        penalty_type_lookup = chargebacks.groupby("penalty_category")["extended_price"].mean().to_dict()
        global_avg = float(chargebacks["extended_price"].mean()) if not chargebacks.empty else GLOBAL_PENALTY_BASELINE
        return channel_lookup, customer_lookup, dc_lookup, penalty_type_lookup, global_avg

    def _expected_penalty_cost(
        self,
        penalty_history: pd.DataFrame,
        *,
        sku_id: str,
        dest_dc: str,
        global_fallback: float,
    ) -> float:
        exact = penalty_history[(penalty_history["sku_id"] == sku_id) & (penalty_history["dc"] == dest_dc)]
        if not exact.empty:
            return float(exact["extended_price"].mean())

        sku_only = penalty_history[penalty_history["sku_id"] == sku_id]
        if not sku_only.empty:
            return float(sku_only["extended_price"].mean())

        dc_only = penalty_history[penalty_history["dc"] == dest_dc]
        if not dc_only.empty:
            return float(dc_only["extended_price"].mean())

        return float(global_fallback)

    def build_event_penalty_payloads(self, event_ids: list[int] | None = None) -> pd.DataFrame:
        events = self._load_events(event_ids=event_ids)
        if events.empty:
            return pd.DataFrame()

        customer_map = self._load_customer_dc_mapping()
        chargebacks = self._load_chargebacks(customer_map)
        penalty_history = self._load_penalty_history(customer_map)
        sales = self._load_sales_window(self._latest_sales_date())

        (
            channel_lookup,
            customer_lookup,
            dc_lookup,
            penalty_type_lookup,
            global_penalty_avg,
        ) = self._build_penalty_indexes(chargebacks)

        payloads: list[dict[str, Any]] = []
        for event in events.itertuples(index=False):
            sku_id = event.sku_id
            source_dc = event.source_dc
            dest_dc = event.dest_dc
            sales_subset = sales[(sales["sku_id"] == sku_id) & (sales["dc"] == dest_dc)].copy()

            customer_weights = (
                sales_subset.groupby("customer_number").size() if not sales_subset.empty else pd.Series(dtype="int64")
            )
            channel_weights = (
                sales_subset.groupby("customer_type").size() if not sales_subset.empty else pd.Series(dtype="int64")
            )

            relevant_chargebacks = chargebacks[chargebacks["dc"] == dest_dc].copy()
            if not customer_weights.empty:
                relevant_chargebacks = relevant_chargebacks[
                    relevant_chargebacks["customer_number"].isin(customer_weights.index)
                ].copy()
            if relevant_chargebacks.empty:
                relevant_chargebacks = chargebacks[chargebacks["dc"] == dest_dc].copy()
            if relevant_chargebacks.empty:
                relevant_chargebacks = chargebacks.copy()

            penalty_type_weights = (
                relevant_chargebacks.groupby("penalty_category").size()
                if not relevant_chargebacks.empty
                else pd.Series(dtype="int64")
            )

            channel_penalty_index = _weighted_lookup_average(
                channel_lookup, channel_weights, fallback=global_penalty_avg
            )
            customer_penalty_index = _weighted_lookup_average(
                customer_lookup, customer_weights, fallback=global_penalty_avg
            )
            dc_penalty_index = float(dc_lookup.get(dest_dc, global_penalty_avg))
            penalty_type_index = _weighted_lookup_average(
                penalty_type_lookup, penalty_type_weights, fallback=global_penalty_avg
            )
            expected_penalty_cost = self._expected_penalty_cost(
                penalty_history,
                sku_id=sku_id,
                dest_dc=dest_dc,
                global_fallback=global_penalty_avg,
            )
            penalty_risk_score = self._risk_score_from_indexes(
                channel_penalty_index=channel_penalty_index,
                customer_penalty_index=customer_penalty_index,
                dc_penalty_index=dc_penalty_index,
                penalty_type_index=penalty_type_index,
                expected_penalty_cost=expected_penalty_cost,
                global_penalty_avg=global_penalty_avg,
            )
            penalty_risk_level = self._risk_level_from_score(penalty_risk_score)

            payloads.append(
                {
                    "event_id": int(event.id),
                    "event_key": event.event_key,
                    "sku_id": sku_id,
                    "source_dc": source_dc,
                    "channel_penalty_index": round(channel_penalty_index, 2),
                    "customer_penalty_index": round(customer_penalty_index, 2),
                    "dc_penalty_index": round(dc_penalty_index, 2),
                    "penalty_type_index": round(penalty_type_index, 2),
                    "penalty_risk_score": penalty_risk_score,
                    "penalty_risk_level": penalty_risk_level,
                    "expected_penalty_cost": round(expected_penalty_cost, 2),
                }
            )

        payload_df = pd.DataFrame(payloads)
        if payload_df.empty:
            return payload_df
        return payload_df.sort_values(by=["event_id"], kind="mergesort").reset_index(drop=True)

    def persist_expected_penalty_costs(self, payload_df: pd.DataFrame) -> None:
        if payload_df.empty:
            print("penalty_agent: no event payloads to persist.")
            return
        for row in payload_df.itertuples(index=False):
            print(
                "penalty_agent: updating "
                f"event {row.event_id} expected_penalty_cost={row.expected_penalty_cost} "
                f"penalty_risk_score={row.penalty_risk_score} penalty_risk_level={row.penalty_risk_level}"
            )
            (
                self.client.table("events")
                .update(
                    {
                        "expected_penalty_cost": float(row.expected_penalty_cost),
                        "penalty_risk_score": float(row.penalty_risk_score),
                        "penalty_risk_level": str(row.penalty_risk_level),
                    }
                )
                .eq("id", int(row.event_id))
                .execute()
            )
        print(f"penalty_agent: updated expected_penalty_cost for {len(payload_df)} events.")


def _print_preview(payload_df: pd.DataFrame, *, limit: int) -> None:
    if payload_df.empty:
        print("penalty_agent: no payloads produced.")
        return
    preview = payload_df.head(limit)
    print(preview.to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Penalty agent: compute empirical penalty indices for existing events."
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute and print payloads without DB updates.")
    parser.add_argument(
        "--persist-expected-cost",
        action="store_true",
        help="Persist expected_penalty_cost back onto existing events.",
    )
    parser.add_argument("--limit", type=int, default=25, help="Rows to print in dry-run preview.")
    parser.add_argument("--sales-window-days", type=int, default=90, help="Recent sales lookback for customer/channel exposure.")
    parser.add_argument("--page-size", type=int, default=1000, help="Supabase pagination page size.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_KEY"):
        raise SystemExit("SUPABASE_URL and SUPABASE_KEY must be set (e.g. via .env) to run the penalty agent.")

    agent = PenaltyAgent(
        _create_client(),
        config=PenaltyAgentConfig(
            sales_window_days=args.sales_window_days,
            page_size=args.page_size,
        ),
    )
    payload_df = agent.build_event_penalty_payloads()
    print(f"penalty_agent: prepared {len(payload_df)} event penalty payloads.")

    if args.dry_run or not args.persist_expected_cost:
        _print_preview(payload_df, limit=args.limit)
        return

    agent.persist_expected_penalty_costs(payload_df)


if __name__ == "__main__":
    main()
