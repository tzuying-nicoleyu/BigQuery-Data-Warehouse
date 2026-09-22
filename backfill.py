"""
backfill.py — Full historical loads for transaction and transactionline.

Run manually (one-time or disaster recovery):
    python backfill.py --job backfill_transaction
    python backfill.py --job backfill_transactionline
    python backfill.py --job backfill_transactionline --years 2025 2026

Do NOT schedule these in GitHub Actions — they are not incremental refreshes.
"""

from google.cloud import bigquery
from extract_from_netsuite import pull_data_by_sql
import pandas as pd
import queue, concurrent.futures
import time
from datetime import date, timedelta

# ── Constants ──────────────────────────────────────────────────────────────────
PROJECT_ID = "clean-pilot-456915-t0"
DATASET_ID = "NetSuite"

# ── Transaction ────────────────────────────────────────────────────────────────
TRANSACTION_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.transaction"
TRANSACTION_SCHEMA = [
    bigquery.SchemaField("id",                "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("tranid",            "STRING"),
    bigquery.SchemaField("transactionnumber", "STRING"),
    bigquery.SchemaField("status",            "STRING"),
    bigquery.SchemaField("type",              "STRING"),
    bigquery.SchemaField("trandate",          "DATE"),
    bigquery.SchemaField("createddate",       "DATE"),
    bigquery.SchemaField("closedate",         "DATE"),
    bigquery.SchemaField("entity",            "INTEGER"),
    bigquery.SchemaField("employee",          "INTEGER"),
    bigquery.SchemaField("lastmodifieddate",  "DATE"),
    bigquery.SchemaField("_loaded_at",        "TIMESTAMP"),
]

def _cast_transaction_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["id"] = pd.to_numeric(df["id"], errors="raise").astype("Int64")
    for col in ["entity", "employee"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["tranid", "transactionnumber", "status", "type"]:
        df[col] = df[col].astype("string")
    for col in ["trandate", "createddate", "closedate", "lastmodifieddate"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    df["_loaded_at"] = pd.Timestamp.now(tz="UTC")
    return df


def backfill_transaction(chunk_size: int = 5_000):
    """Pull ALL transactions from NetSuite and WRITE_TRUNCATE into BigQuery."""
    print("[backfill_transaction] Starting full load...")
    client = bigquery.Client(project=PROJECT_ID)
    all_chunks = []
    last_id = 0
    t0 = time.time()

    while True:
        query = f"""
            SELECT id, tranid, transactionnumber, status, type, entity, employee,
                   tranDate, createdDate, closeDate, lastmodifieddate
            FROM transaction
            WHERE id > {last_id}
            ORDER BY id
            FETCH NEXT {chunk_size} ROWS ONLY
        """
        df = pull_data_by_sql(query=query, return_df=True)
        if df is None or df.empty:
            break
        all_chunks.append(df)
        last_id = int(df["id"].astype("int64").max())
        print(f"  Pulled up to id {last_id:,} ({len(df)} rows this page)")
        if len(df) < chunk_size:
            break

    full_df = pd.concat(all_chunks, ignore_index=True)
    full_df = _cast_transaction_df(full_df)
    print(f"  Total rows fetched: {len(full_df):,}")

    job = client.load_table_from_dataframe(
        full_df,
        TRANSACTION_TABLE_REF,
        job_config=bigquery.LoadJobConfig(
            schema=TRANSACTION_SCHEMA,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH, field="trandate"
            ),
            clustering_fields=["type"],
        ),
    )
    job.result()
    elapsed = time.time() - t0
    print(f"[backfill_transaction] Done. {job.output_rows:,} rows loaded in {elapsed:.0f}s")


# ── Transactionline ────────────────────────────────────────────────────────────
TL_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.transactionline"
TL_CLUSTERING_FIELDS = ["mainline", "taxline", "transaction"]
TL_SELECT_COLUMNS = [
    "uniquekey", "transaction", "item", "itemtype", "accountinglinetype",
    "expenseaccount", "inventorylocation", "netamount", "costestimate",
    "rate", "price", "quantity", "quantitybackordered", "createdfrom",
    "memo", "mainline", "taxline", "custcolfree_goods_checkbox",
    "linecreateddate", "linelastmodifieddate",
]
TL_SCHEMA = [
    bigquery.SchemaField("uniquekey",                 "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("transaction",               "INTEGER"),
    bigquery.SchemaField("item",                      "INTEGER"),
    bigquery.SchemaField("itemtype",                  "STRING"),
    bigquery.SchemaField("accountinglinetype",        "STRING"),
    bigquery.SchemaField("expenseaccount",            "INTEGER"),
    bigquery.SchemaField("inventorylocation",         "INTEGER"),
    bigquery.SchemaField("netamount",                 "FLOAT"),
    bigquery.SchemaField("costestimate",              "FLOAT"),
    bigquery.SchemaField("rate",                      "FLOAT"),
    bigquery.SchemaField("price",                     "FLOAT"),
    bigquery.SchemaField("quantity",                  "FLOAT"),
    bigquery.SchemaField("quantitybackordered",       "FLOAT"),
    bigquery.SchemaField("createdfrom",               "INTEGER"),
    bigquery.SchemaField("memo",                      "STRING"),
    bigquery.SchemaField("mainline",                  "BOOLEAN"),
    bigquery.SchemaField("taxline",                   "BOOLEAN"),
    bigquery.SchemaField("custcolfree_goods_checkbox","BOOLEAN"),
    bigquery.SchemaField("linecreateddate",           "DATE"),
    bigquery.SchemaField("linelastmodifieddate",      "DATE"),
    bigquery.SchemaField("_loaded_at",                "TIMESTAMP"),
]
ALL_YEARS = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]


def _cast_tl_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["uniquekey"] = pd.to_numeric(df["uniquekey"], errors="raise").astype("Int64")
    for col in ["transaction", "item", "expenseaccount", "inventorylocation", "createdfrom"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["itemtype", "accountinglinetype", "memo"]:
        df[col] = df[col].astype("string")
    for col in ["netamount", "costestimate", "rate", "price", "quantity", "quantitybackordered"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["mainline", "taxline", "custcolfree_goods_checkbox"]:
        df[col] = df[col].map({"T": True, "F": False, True: True, False: False})
    for col in ["linecreateddate", "linelastmodifieddate"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    df["_loaded_at"] = pd.Timestamp.now(tz="UTC")
    return df


def create_transactionline_table():
    """Create the transactionline table in BigQuery. Run ONCE before the first year load."""
    client = bigquery.Client(project=PROJECT_ID)
    client.delete_table(TL_TABLE_REF, not_found_ok=True)
    table = bigquery.Table(TL_TABLE_REF, schema=TL_SCHEMA)
    table.clustering_fields = TL_CLUSTERING_FIELDS
    client.create_table(table)
    print(f"Created {TL_TABLE_REF}")


def _year_filter(year: int) -> str:
    return (
        f"linecreateddate >= TO_DATE('{year}-01-01', 'YYYY-MM-DD') "
        f"AND linecreateddate <  TO_DATE('{year + 1}-01-01', 'YYYY-MM-DD')"
    )


def _get_year_range(year: int, attempts: int = 4):
    q = (
        f"SELECT MIN(uniquekey) AS mn, MAX(uniquekey) AS mx, COUNT(*) AS n "
        f"FROM transactionline WHERE {_year_filter(year)}"
    )
    for _ in range(attempts):
        df = pull_data_by_sql(query=q, return_df=True)
        if df is not None and not df.empty and "mn" in df.columns:
            return int(df["mn"].iloc[0]), int(df["mx"].iloc[0]), int(df["n"].iloc[0])
    raise RuntimeError(f"Could not get uniquekey range for {year}")


def _year_worker(
    slice_queue: queue.Queue,
    year: int,
    worker_id: int,
    chunk_size: int = 5_000,
    batch_flush_every: int = 4,
    max_slice_attempts: int = 3,
) -> int:
    client = bigquery.Client(project=PROJECT_ID)
    yf = _year_filter(year)
    total_loaded = 0
    attempts: dict = {}

    while True:
        try:
            s_start, s_end = slice_queue.get_nowait()
        except queue.Empty:
            break

        key = (s_start, s_end)
        attempts[key] = attempts.get(key, 0) + 1
        last_key = s_start
        last_flushed = s_start
        buffer = []

        try:
            while True:
                query = f"""
                    SELECT {", ".join(TL_SELECT_COLUMNS)}
                    FROM transactionline
                    WHERE {yf}
                      AND uniquekey >  {last_key}
                      AND uniquekey <= {s_end}
                    ORDER BY uniquekey
                    FETCH NEXT {chunk_size} ROWS ONLY
                """
                df = pull_data_by_sql(query=query, return_df=True)
                if df is None or df.empty:
                    break

                df.columns = df.columns.str.lower()
                for col in TL_SELECT_COLUMNS:
                    if col not in df.columns:
                        df[col] = pd.NA
                df = df[TL_SELECT_COLUMNS]
                buffer.append(df)
                last_key = int(df["uniquekey"].astype("int64").max())

                if len(buffer) >= batch_flush_every or len(df) < chunk_size:
                    batch_df = _cast_tl_df(pd.concat(buffer, ignore_index=True))
                    client.load_table_from_dataframe(
                        batch_df,
                        TL_TABLE_REF,
                        job_config=bigquery.LoadJobConfig(
                            schema=TL_SCHEMA,
                            write_disposition="WRITE_APPEND",
                            create_disposition="CREATE_IF_NEEDED",
                            clustering_fields=TL_CLUSTERING_FIELDS,
                        ),
                    ).result()
                    total_loaded += len(batch_df)
                    last_flushed = last_key
                    buffer = []
                    print(f"  [{year} w{worker_id}] flushed to {last_flushed:,} (total {total_loaded:,})")

                if len(df) < chunk_size:
                    break

        except Exception as e:
            if attempts[key] < max_slice_attempts and last_flushed < s_end:
                slice_queue.put((last_flushed, s_end))
                print(f"  [{year} w{worker_id}] requeued ({last_flushed}, {s_end}): {e}")
            else:
                print(f"  [{year} w{worker_id}] ABANDONED ({last_flushed}, {s_end}): {e}")

    return total_loaded


def load_year(year: int, n_workers: int = 3, chunk_size: int = 5_000, slice_size: int = 100_000):
    """Load one year of transactionline into BigQuery (WRITE_APPEND). Verifies row count."""
    mn, mx, expected = _get_year_range(year)
    print(f"[{year}] uniquekey {mn:,}..{mx:,}, NetSuite reports {expected:,} rows")

    slice_q: queue.Queue = queue.Queue()
    s = mn - 1
    while s < mx:
        e = min(s + slice_size, mx)
        slice_q.put((s, e))
        s = e
    print(f"[{year}] {slice_q.qsize()} slices, {n_workers} workers")

    loaded = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as pool:
        futs = [pool.submit(_year_worker, slice_q, year, i, chunk_size) for i in range(n_workers)]
        for f in concurrent.futures.as_completed(futs):
            loaded += f.result() or 0

    client = bigquery.Client(project=PROJECT_ID)
    in_bq = list(client.query(
        f"SELECT COUNT(*) AS n FROM `{TL_TABLE_REF}` "
        f"WHERE linecreateddate >= DATE '{year}-01-01' "
        f"  AND linecreateddate <  DATE '{year + 1}-01-01'"
    ).result())[0].n

    ok = in_bq == expected
    status = "OK" if ok else "MISMATCH"
    print(f"[{year}] loaded {loaded:,} | BigQuery {in_bq:,} | expected {expected:,} → {status}")
    return ok


def backfill_transactionline(years: list[int] | None = None, n_workers: int = 3):
    """Full historical load: create table then load each year in sequence."""
    if years is None:
        years = ALL_YEARS

    print(f"[backfill_transactionline] Years: {years}")
    print("[backfill_transactionline] Creating table...")
    create_transactionline_table()

    t0 = time.time()
    mismatches = []
    for year in years:
        year_start = time.time()
        ok = load_year(year, n_workers=n_workers)
        elapsed = time.time() - year_start
        print(f"[{year}] Finished in {elapsed:.0f}s")
        if not ok:
            mismatches.append(year)

    total = time.time() - t0
    print(f"\n[backfill_transactionline] Complete in {total:.0f}s")
    if mismatches:
        print(f"  MISMATCHED YEARS (re-run needed): {mismatches}")
    else:
        print("  All years verified OK.")


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    import argparse

    parser = argparse.ArgumentParser(description="NetSuite -> BigQuery full backfill")
    parser.add_argument(
        "--job",
        required=True,
        choices=["backfill_transaction", "backfill_transactionline"],
        help="Which backfill to run",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=None,
        help="Years to load for transactionline (default: all years)",
    )
    args = parser.parse_args()

    if args.job == "backfill_transaction":
        backfill_transaction()
    elif args.job == "backfill_transactionline":
        backfill_transactionline(years=args.years)


if __name__ == "__main__":
    main()
