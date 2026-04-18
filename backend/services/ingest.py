import argparse
import hashlib
import os
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd
from dotenv import load_dotenv
from postgrest.exceptions import APIError
from supabase import create_client

load_dotenv()

PROJECTS_DIR = Path(__file__).resolve().parents[3]
INVENTORY_PATH = PROJECTS_DIR / "POP_InventorySnapshot.xlsx"
SALES_PATH = PROJECTS_DIR / "POP_SalesTransactionHistory.csv"
PO_PATH = PROJECTS_DIR / "POP_PurchaseOrderHistory.XLSX"
CHARGEBACKS_PATH = PROJECTS_DIR / "POP_ChargeBack_Deductions_Penalties_Freight.xlsx"

INVENTORY_SHEET_MAP = {
    "Site 1 - SF": "SF",
    "Site 2 - NJ": "NJ",
    "Site 3 - LA": "LA",
}
LOCNCODE_TO_DC = {"1": "SF", "2": "NJ", "3": "LA"}
SHIP_TO_DC = {"LIVERMORE": "SF", "NEW JERSEY": "NJ", "LOS ANGELES": "LA"}
TRANSFER_ACCOUNT_TO_DC = {
    "SF - COGS - Transfer": "SF",
    "NJ - COGS - Transfer": "NJ",
    "LA - COGS - Transfer": "LA",
}
CHARGEBACK_CAUSE_CODES = {"CRED11-F", "CRED11-O", "CRED08", "CRED12"}
HASH_KEYED_TABLES = {
    "sales_history",
    "po_history",
    "chargebacks",
    "transfer_cost_history",
    "penalty_history",
}
ALL_TARGETS = [
    "inventory_snapshots",
    "sales_history",
    "po_history",
    "chargebacks",
    "penalty_history",
    "transfer_cost_history",
    "transfer_cost_lookup",
    "lead_time_lookup",
    "customer_dc_mapping",
]


def _strip_text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def _null_if_too_long(series: pd.Series, max_length: int) -> pd.Series:
    return series.where(series.str.len().le(max_length) | series.isna(), None)


def _normalize_state(series: pd.Series) -> pd.Series:
    cleaned = _strip_text(series).str.upper()
    return _null_if_too_long(cleaned, 2)


def _to_nullable_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _to_nullable_float(series: pd.Series, decimals: int | None = None) -> pd.Series:
    out = pd.to_numeric(series, errors="coerce")
    if decimals is not None:
        out = out.round(decimals)
    return out


def _to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _to_iso_date(series: pd.Series) -> pd.Series:
    dt = _to_datetime(series)
    out = dt.dt.strftime("%Y-%m-%d")
    return out.where(dt.notna(), None)


def _series_to_hashable_strings(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return series.dt.strftime("%Y-%m-%dT%H:%M:%S").where(series.notna(), "")
    return series.astype("string").fillna("")


def _add_source_row_hash(
    df: pd.DataFrame,
    key_columns: Iterable[str] | None = None,
    *,
    dedupe_label: str | None = None,
) -> pd.DataFrame:
    columns = list(key_columns or df.columns)
    hash_input = pd.DataFrame({column: _series_to_hashable_strings(df[column]) for column in columns})
    row_strings = hash_input.agg("\x1f".join, axis=1)
    out = df.copy()
    out["source_row_hash"] = row_strings.map(lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest())
    dup_mask = out["source_row_hash"].duplicated(keep=False)
    duplicate_row_count = int(dup_mask.sum())
    before_len = len(out)
    unique_dup_hashes = int(out.loc[dup_mask, "source_row_hash"].nunique()) if duplicate_row_count else 0
    out = out.drop_duplicates(subset=["source_row_hash"]).copy()
    if duplicate_row_count:
        prefix = f"{dedupe_label}: " if dedupe_label else ""
        print(
            f"{prefix}dedupe: {duplicate_row_count} rows share {unique_dup_hashes} duplicate "
            f"source_row_hash value(s); kept first occurrence per hash ({before_len} -> {len(out)} rows)."
        )
    return out


def _prepare_records(df: pd.DataFrame) -> list[dict]:
    cleaned = df.astype(object).where(pd.notna(df), None)
    return cleaned.to_dict(orient="records")


def _create_client():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


def _is_duplicate_unique_constraint_error(err: APIError) -> bool:
    message = ""
    if err.args and isinstance(err.args[0], dict):
        message = str(err.args[0].get("message", "") or "")
    if not message:
        message = str(getattr(err, "message", "") or "")
    return message.startswith("duplicate key value violates unique constraint")


def _write_batches(
    table_name: str,
    records: list[dict],
    *,
    conflict_columns: Iterable[str] | None = None,
    batch_size: int = 1000,
) -> None:
    if not records:
        print(f"No rows to upload for '{table_name}'.")
        return

    client = _create_client()
    on_conflict = ",".join(conflict_columns) if conflict_columns else None
    conflict_cols = on_conflict.split(",") if on_conflict else []

    for start in range(0, len(records), batch_size):
        batch = records[start : start + batch_size]
        print(f"{table_name}: uploading rows {start}..{start + len(batch) - 1}")
        if on_conflict:
            key_counts: dict[tuple, int] = {}
            for record in batch:
                key = tuple(record.get(column) for column in conflict_cols)
                key_counts[key] = key_counts.get(key, 0) + 1
            dup_keys = sum(1 for _key, count in key_counts.items() if count > 1)
            if dup_keys:
                print(
                    f"{table_name}: warning: batch at offset {start} contains {dup_keys} "
                    f"duplicate upsert key(s) on ({on_conflict}); same batch may error in Postgres."
                )
        try:
            if on_conflict:
                client.table(table_name).upsert(batch, on_conflict=on_conflict).execute()
            else:
                client.table(table_name).insert(batch).execute()
        except APIError as err:
            if on_conflict and _is_duplicate_unique_constraint_error(err):
                print(
                    f"{table_name}: duplicate-key conflict while upserting batch at offset {start} "
                    f"on ({on_conflict}); treating as idempotent skip. Detail: {err}"
                )
                continue
            raise

    print(f"Uploaded {len(records)} rows to '{table_name}'.")


def upload_table(
    table_name: str,
    df: pd.DataFrame,
    *,
    conflict_columns: Iterable[str] | None = None,
) -> None:
    _write_batches(table_name, _prepare_records(df), conflict_columns=conflict_columns)


def insert_table(table_name: str, df: pd.DataFrame) -> None:
    _write_batches(table_name, _prepare_records(df))


def load_inventory_snapshots(path: Path = INVENTORY_PATH) -> pd.DataFrame:
    sheets = pd.read_excel(path, sheet_name=list(INVENTORY_SHEET_MAP.keys()), dtype=object)
    frames = []
    for sheet_name, dc_value in INVENTORY_SHEET_MAP.items():
        df = sheets[sheet_name].copy()
        df = df.rename(
            columns={
                "Item Number": "sku_id",
                "Description": "description",
                "Available": "available",
                "On Hand": "on_hand",
            }
        )
        df["sku_id"] = _strip_text(df["sku_id"])
        df["description"] = _strip_text(df["description"])
        df["available"] = _to_nullable_int(df["available"])
        df["on_hand"] = _to_nullable_int(df["on_hand"])
        df["dc"] = dc_value
        df["snapshot_date"] = date.today().isoformat()
        frames.append(df[["sku_id", "description", "available", "on_hand", "dc", "snapshot_date"]])
    return pd.concat(frames, ignore_index=True)


def load_sales_history(path: Path = SALES_PATH) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["DOCDATE"], dtype={"LOCNCODE": "string"}, low_memory=False)
    df["LOCNCODE"] = _strip_text(df["LOCNCODE"])
    df["SOP TYPE"] = _strip_text(df["SOP TYPE"])
    df = df[df["LOCNCODE"].isin(LOCNCODE_TO_DC) & (df["SOP TYPE"] == "Invoice")].copy()

    out = pd.DataFrame(
        {
            "dc": df["LOCNCODE"].map(LOCNCODE_TO_DC),
            "salesperson_id": _strip_text(df["SLPRSNID"]),
            "customer_number": _strip_text(df["CUSTNMBR"]),
            "city": _strip_text(df["CITY"]),
            "state": _normalize_state(df["STATE"]),
            "sop_number": _strip_text(df["SOPNUMBE"]),
            "doc_date": _to_iso_date(df["DOCDATE"]),
            "sku_id": _strip_text(df["ITEMNMBR"]),
            "item_desc": _strip_text(df["ITEMDESC"]),
            "quantity_adj": _to_nullable_int(df["QUANTITY_adj"]),
            "uom": _strip_text(df["UOFM"]),
            "qty_base_uom": _to_nullable_int(df["QTYBSUOM"]),
            "ext_price_adj": _to_nullable_float(df["XTNDPRCE_adj"], 2),
            "ext_cost_adj": _to_nullable_float(df["EXTDCOST_adj"], 2),
            "customer_type": _strip_text(df["Customer Type"]),
            "product_type": _strip_text(df["Product Type"]),
            "gross_profit": _to_nullable_float(df["Gross_Profit_adj"], 2),
            "margin_pct": _to_nullable_float(df["Margin_Pct_adj"], 4),
            "unit_price_adj": _to_nullable_float(df["Unit_Price_adj"], 4),
        }
    )
    return _add_source_row_hash(out, dedupe_label="sales_history")


def _load_po_source(path: Path = PO_PATH) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=object)
    out = pd.DataFrame(
        {
            "po_number": _to_nullable_int(df["PO Number"]),
            "po_date": _to_datetime(df["PO Date"]),
            "required_date": _to_datetime(df["Required Date"]),
            "promised_ship_date": _to_datetime(df["Promised Ship Date"]),
            "receipt_date": _to_datetime(df["Receipt Date"]),
            "pop_receipt_number": _to_nullable_int(df["POP Receipt Number"]),
            "sku_id": _strip_text(df["Item Number"]),
            "item_description": _strip_text(df["Item Description"]),
            "qty_shipped": _to_nullable_int(df["QTY Shipped"]),
            "qty_invoiced": _to_nullable_int(df["QTY Invoiced"]),
            "unit_cost": _to_nullable_float(df["Unit Cost"], 4),
            "extended_cost": _to_nullable_float(df["Extended Cost"], 2),
            "vendor_id": _strip_text(df["Vendor ID"]),
            "location_code": _to_nullable_int(df["Location Code"]),
            "dc": _strip_text(df["Primary Ship To Address"]).map(SHIP_TO_DC),
            "ship_to_address": _strip_text(df["Primary Ship To Address"]),
            "shipping_method": _strip_text(df["Shipping Method"]),
        }
    )
    return out[out["dc"].notna()].copy()


def load_po_history(path: Path = PO_PATH) -> pd.DataFrame:
    out = _load_po_source(path)

    for column in ["po_date", "required_date", "promised_ship_date", "receipt_date"]:
        out[column] = _to_iso_date(out[column])

    return _add_source_row_hash(
        out[
            [
                "po_number",
                "po_date",
                "required_date",
                "promised_ship_date",
                "receipt_date",
                "pop_receipt_number",
                "sku_id",
                "item_description",
                "qty_shipped",
                "qty_invoiced",
                "unit_cost",
                "extended_cost",
                "vendor_id",
                "location_code",
                "dc",
                "ship_to_address",
                "shipping_method",
            ]
        ],
        dedupe_label="po_history",
    )


def load_chargebacks(path: Path = CHARGEBACKS_PATH) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Data - Deductions & Cause Code", dtype=object)
    df["Cause Code"] = _strip_text(df["Cause Code"])
    df = df[df["Cause Code"].isin(CHARGEBACK_CAUSE_CODES)].copy()

    out = pd.DataFrame(
        {
            "location_code": _to_nullable_int(df["Location Code"]),
            "salesperson_id": _strip_text(df["Salesperson ID"]),
            "customer_number": _strip_text(df["Customer Number"]),
            "city": _strip_text(df["City from Sales Transaction"]),
            "state": _normalize_state(df["State from Sales Transaction"]),
            "sop_type": _strip_text(df["SOP Type"]),
            "sop_number": _strip_text(df["SOP Number"]),
            "customer_po_number": _strip_text(df["Customer PO Number"]),
            "doc_date": _to_iso_date(df["Document Date"]),
            "cause_code": df["Cause Code"],
            "cause_code_desc": _strip_text(df["Cause Code Desc"]),
            "item_description": _strip_text(df["Item Description"]),
            "extended_price": _to_nullable_float(df["Extended Price"], 2),
        }
    )
    return _add_source_row_hash(out, dedupe_label="chargebacks")


def load_transfer_cost_history(path: Path = CHARGEBACKS_PATH) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Data-Transfer Cost", dtype=object)
    out = pd.DataFrame(
        {
            "journal_entry": _to_nullable_int(df["Journal Entry"]),
            "trx_date": _to_iso_date(df["TRX Date"]),
            "account_number": _strip_text(df["Account Number"]),
            "account_description": _strip_text(df["Account Description"]),
            "dc": _strip_text(df["Account Description"]).map(TRANSFER_ACCOUNT_TO_DC),
            "amount": _to_nullable_float(df["Amount"], 2),
            "originating_master_name": _strip_text(df["Originating Master Name"]),
            "reference": _strip_text(df["Reference"]),
        }
    )
    return _add_source_row_hash(out, dedupe_label="transfer_cost_history")


def load_penalty_history(path: Path = CHARGEBACKS_PATH) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Data-Penalty", dtype=object)
    out = pd.DataFrame(
        {
            "salesperson_id": _strip_text(df["Salesperson ID"]),
            "customer_number": _strip_text(df["Customer Number"]),
            "customer_name": _strip_text(df["Customer Name"]),
            "city": _strip_text(df["City from Sales Transaction"]),
            "state": _normalize_state(df["State from Sales Transaction"]),
            "sop_number": _strip_text(df["SOP Number"]),
            "doc_date": _to_iso_date(df["Document Date"]),
            "sku_id": _strip_text(df["Item Number"]),
            "item_description": _strip_text(df["Item Description"]),
            "qty": _to_nullable_float(df["QTY"], 2),
            "uom": _strip_text(df["U Of M"]),
            "extended_price": _to_nullable_float(df["Extended Price"], 2),
            "market": _strip_text(df["MARKET"]),
        }
    )
    return _add_source_row_hash(out, dedupe_label="penalty_history")


def derive_transfer_cost_lookup(history_df: pd.DataFrame) -> pd.DataFrame:
    df = history_df.copy()
    df = df[df["dc"].notna() & df["amount"].notna()].copy()
    # Reversal rows can be negative; use cost magnitude for route-level lookup stats.
    df["cost_magnitude"] = df["amount"].abs()

    grouped = (
        df.groupby("dc", as_index=False)["cost_magnitude"]
        .agg(avg_cost="mean", min_cost="min", max_cost="max", sample_size="count")
        .sort_values("dc")
        .rename(columns={"dc": "dest_dc"})
    )
    for column in ["avg_cost", "min_cost", "max_cost"]:
        grouped[column] = grouped[column].round(2)
    grouped["sample_size"] = grouped["sample_size"].astype("Int64")
    return grouped[["dest_dc", "avg_cost", "min_cost", "max_cost", "sample_size"]]


def derive_lead_time_lookup(path: Path = PO_PATH) -> pd.DataFrame:
    df = _load_po_source(path)
    df = df[df["po_date"].notna() & df["receipt_date"].notna()].copy()
    df["lead_days"] = (df["receipt_date"] - df["po_date"]).dt.days
    df = df[df["lead_days"].between(0, 200, inclusive="both")].copy()

    grouped = (
        df.groupby("dc", as_index=False)["lead_days"]
        .agg(median_days="median", avg_days="mean", sample_size="count")
        .sort_values("dc")
    )
    grouped["median_days"] = grouped["median_days"].round(1)
    grouped["avg_days"] = grouped["avg_days"].round(1)
    grouped["sample_size"] = grouped["sample_size"].astype("Int64")
    return grouped[["dc", "median_days", "avg_days", "sample_size"]]


def derive_customer_dc_mapping(sales_df: pd.DataFrame) -> pd.DataFrame:
    counts = (
        sales_df.groupby(["customer_number", "dc"], as_index=False)
        .size()
        .rename(columns={"size": "order_count"})
    )
    winners = (
        counts.sort_values(["customer_number", "order_count", "dc"], ascending=[True, False, True])
        .drop_duplicates(subset=["customer_number"], keep="first")
        .rename(columns={"dc": "primary_dc"})
    )

    customer_type = (
        sales_df.groupby("customer_number")["customer_type"]
        .agg(lambda values: values.dropna().mode().iloc[0] if not values.dropna().empty else None)
        .reset_index()
    )

    out = winners.merge(customer_type, on="customer_number", how="left")
    out["order_count"] = out["order_count"].astype("Int64")
    return out[["customer_number", "primary_dc", "customer_type", "order_count"]]


def build_datasets(targets: set[str]) -> dict[str, pd.DataFrame]:
    datasets: dict[str, pd.DataFrame] = {}

    if "inventory_snapshots" in targets:
        datasets["inventory_snapshots"] = load_inventory_snapshots()

    sales_df: pd.DataFrame | None = None
    if {"sales_history", "customer_dc_mapping"} & targets:
        sales_df = load_sales_history()
        if "sales_history" in targets:
            datasets["sales_history"] = sales_df
        if "customer_dc_mapping" in targets:
            datasets["customer_dc_mapping"] = derive_customer_dc_mapping(sales_df)

    if "chargebacks" in targets:
        datasets["chargebacks"] = load_chargebacks()

    if "penalty_history" in targets:
        datasets["penalty_history"] = load_penalty_history()

    transfer_cost_history_df: pd.DataFrame | None = None
    if {"transfer_cost_history", "transfer_cost_lookup"} & targets:
        transfer_cost_history_df = load_transfer_cost_history()
        if "transfer_cost_history" in targets:
            datasets["transfer_cost_history"] = transfer_cost_history_df
        if "transfer_cost_lookup" in targets:
            datasets["transfer_cost_lookup"] = derive_transfer_cost_lookup(transfer_cost_history_df)

    if {"po_history", "lead_time_lookup"} & targets:
        if "po_history" in targets:
            datasets["po_history"] = load_po_history()
        if "lead_time_lookup" in targets:
            datasets["lead_time_lookup"] = derive_lead_time_lookup()

    return datasets


def _count_open_po_rows(path: Path = PO_PATH) -> int:
    """Match open_po_history view: receipt_date is null or strictly after today."""
    df = _load_po_source(path)
    receipt = df["receipt_date"]
    today = pd.Timestamp(date.today())
    mask = receipt.isna() | (receipt > today)
    return int(mask.sum())


def write_dataset(table_name: str, df: pd.DataFrame) -> None:
    if table_name == "inventory_snapshots":
        upload_table(table_name, df, conflict_columns=["sku_id", "dc", "snapshot_date"])
    elif table_name in HASH_KEYED_TABLES:
        upload_table(table_name, df, conflict_columns=["source_row_hash"])
    elif table_name == "transfer_cost_lookup":
        upload_table(table_name, df, conflict_columns=["dest_dc"])
    elif table_name == "lead_time_lookup":
        upload_table(table_name, df, conflict_columns=["dc"])
    elif table_name == "customer_dc_mapping":
        upload_table(table_name, df, conflict_columns=["customer_number"])
    else:
        insert_table(table_name, df)


def run_targets(targets: Iterable[str], *, dry_run: bool = False) -> None:
    target_set = set(targets)
    datasets = build_datasets(target_set)

    if "po_history" in target_set:
        open_po_count = _count_open_po_rows()
        print(
            "open_po_history is a database view over po_history (not ingested directly). "
            f"Rows that would appear as open today: {open_po_count}."
        )

    for table_name in ALL_TARGETS:
        if table_name not in target_set:
            continue
        df = datasets[table_name]
        print(f"{table_name}: prepared {len(df)} rows")
        if dry_run:
            continue
        write_dataset(table_name, df)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load POP source files into Supabase tables.")
    parser.add_argument(
        "targets",
        nargs="*",
        choices=ALL_TARGETS + ["all"],
        default=["all"],
        help="Specific tables/lookups to load. Defaults to all targets.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build datasets and print row counts without uploading to Supabase.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    targets = ALL_TARGETS if not args.targets or "all" in args.targets else args.targets
    run_targets(targets, dry_run=args.dry_run)


if __name__ == "__main__":
    main()