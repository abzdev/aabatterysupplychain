import pandas as pd
from supabase import create_client
import os
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
TABLE_NAME = "inventory_snapshots"
# getting the path to the excel file (test only)
XLSX_PATH = Path(__file__).resolve().parent.parent / "data" / "POP_InventorySnapshot.xlsx"

SHEET_MAP = {
    "Site 1 - SF": "SF",
    "Site 2 - NJ": "NJ",
    "Site 3 - LA": "LA",
}

def load_inventory_snapshots(path: str) -> pd.DataFrame:
    sheets = pd.read_excel(path, sheet_name=list(SHEET_MAP.keys()), dtype=str)
    frames = []
    for sheet_name, dc_value in SHEET_MAP.items():
        df = sheets[sheet_name].copy()
        first_col = df.columns[0]
        df[first_col] = df[first_col].str.strip()
        df["dc"] = dc_value
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    out = out.rename(columns={"Item Number": "sku_id",
                              "Description": "description", 
                              "Available": "available", 
                              "On Hand": "on_hand"
    })
    out["snapshot_date"] = date.today().isoformat()
    return out



def upload_to_supabase(df: pd.DataFrame):
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    # Cast to object first so missing values can become real Python None.
    cleaned_df = df.astype(object).where(pd.notna(df), None)
    records = cleaned_df.to_dict(orient="records")
    # Upsert in batches to stay within Supabase request limits
    batch_size = 70
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        print("batch number: ", i)
        print("batch size: ", len(batch))
        print("--------------------------------")
        client.table(TABLE_NAME).upsert(batch).execute()
    print(f"Uploaded {len(records)} rows to '{TABLE_NAME}'.")

if __name__ == "__main__":
    df = load_inventory_snapshots(XLSX_PATH)
    print(f"Inventory Snapshots Loaded: {len(df)} rows across {df['dc'].nunique()} sites.")
    upload_to_supabase(df)