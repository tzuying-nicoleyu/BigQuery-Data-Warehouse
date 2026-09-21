## Libarary ################################
from google.cloud import bigquery
from extract_from_netsuite import pull_data_by_sql
import pandas as pd
import pandas_gbq
from decimal import Decimal
import queue, concurrent.futures
import traceback
import time
import threading
from datetime import date, timedelta
from wrapper import *

## Common Parameters ###############################
PROJECT_ID = "clean-pilot-456915-t0"
DATASET_ID = "NetSuite"

## Transaction Table Refresh ##########################
TRANSACTION_STAGING_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.transaction_staging"
TRANSACTION_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.transaction"
TRANSACTION_SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("tranid", "STRING"),
    bigquery.SchemaField("transactionnumber", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("trandate", "DATE"),
    bigquery.SchemaField("createddate", "DATE"),
    bigquery.SchemaField("closedate", "DATE"),
    bigquery.SchemaField("entity", "INTEGER"),
    bigquery.SchemaField("employee", "INTEGER"),
    bigquery.SchemaField("lastmodifieddate", "DATE"),
    bigquery.SchemaField("_loaded_at", "TIMESTAMP"),
]

def _cast_transaction_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["id"] = pd.to_numeric(df["id"], errors="raise").astype("Int64")

    int_cols = ["entity", "employee"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    string_cols = ["tranid", "transactionnumber", "status", "type"]
    for col in string_cols:
        df[col] = df[col].astype("string")

    date_cols = ["trandate", "createddate", "closedate", "lastmodifieddate"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    df["_loaded_at"] = pd.Timestamp.now(tz="UTC")

    return df


def _pull_transaction_period(trandate_gte, trandate_lt, worker_id, chunk_size=5000):
    """Pull one date period using keyset pagination on id. Returns a DataFrame."""
    chunks = []
    last_id = 0

    # configure date condition for the pull syntax
    date_filter = f"trandate >= TO_DATE('{trandate_gte}', 'YYYY-MM-DD')"
    if trandate_lt:
        date_filter += f" AND trandate < TO_DATE('{trandate_lt}', 'YYYY-MM-DD')"

    while True:
        query = f"""
            SELECT id, tranid, transactionnumber, status, type, entity, employee,
                   tranDate, createdDate, closeDate, lastmodifieddate
            FROM transaction
            WHERE {date_filter}
              AND id > {last_id}
            ORDER BY id
            FETCH NEXT {chunk_size} ROWS ONLY
        """
        df = pull_data_by_sql(query=query, return_df=True)

        if df is None or df.empty:
            break

        chunks.append(df)
        last_id = int(df["id"].astype("int64").max())
        print(f"[w{worker_id} {trandate_gte[:7]}] pulled to id {last_id} ({len(df)} rows)")

        if len(df) < chunk_size:
            break

    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

@timeit
def refresh_transaction(chunk_size=5000):
    cutoff_date = date.today() - timedelta(days=365 * 2)
    cutoff_str  = cutoff_date.strftime("%Y-%m-%d")
    today       = date.today()
    print(f"Refresh window: trandate >= {cutoff_str}")

    # Build year-based periods within the 2-year window, using cutoff_date.year and today.year
    periods = []
    yr = cutoff_date.year
    while yr <= today.year:
        gte = cutoff_str          if yr == cutoff_date.year else f"{yr}-01-01" # lower bound (greater than)
        lt  = f"{yr + 1}-01-01"   if yr < today.year        else None  # no upper bound on current year (less than)
        periods.append((gte, lt))
        yr += 1

    # ── Step 1: Pull all periods concurrently ─────────────────────────────────
    print(f"Pulling {len(periods)} periods concurrently...")
    all_chunks = []
    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(periods)) as ex:
        futures = {
            ex.submit(_pull_transaction_period, gte, lt, i, chunk_size): (gte, lt)
            for i, (gte, lt) in enumerate(periods)
        }
        for future in concurrent.futures.as_completed(futures):
            gte, lt = futures[future]
            df = future.result()  # raises immediately if the period failed
            if not df.empty:
                all_chunks.append(df)
                print(f"Period {gte} → {lt or 'now'}: {len(df)} rows collected")
    elapsed = time.time() - start
    print(f"NetSuite concurrent pull done in {elapsed:.1f}s")
    
    full_df = pd.concat(all_chunks, ignore_index=True)
    full_df = _cast_transaction_df(full_df)
    print(f"Total rows to stage: {len(full_df)}")

    # ── Step 2: Stage (single load job) ───────────────────────────────────────
    client = bigquery.Client(project=PROJECT_ID)
    staging_job = client.load_table_from_dataframe(
        full_df,
        TRANSACTION_STAGING_TABLE_REF,
        job_config=bigquery.LoadJobConfig(
            schema=TRANSACTION_SCHEMA,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        ),
    )
    staging_job.result()
    print(f"Staged {staging_job.output_rows} rows into {TRANSACTION_STAGING_TABLE_REF}")

    # ── Step 3: MERGE staging → target ────────────────────────────────────────
    merge_sql = f"""
        MERGE `{TRANSACTION_TABLE_REF}` AS T
        USING `{TRANSACTION_STAGING_TABLE_REF}` AS S
        ON T.id = S.id

        WHEN MATCHED THEN UPDATE SET
            T.tranid            = S.tranid,
            T.transactionnumber = S.transactionnumber,
            T.status            = S.status,
            T.type              = S.type,
            T.trandate          = S.trandate,
            T.createddate       = S.createddate,
            T.closedate         = S.closedate,
            T.entity            = S.entity,
            T.employee          = S.employee,
            T.lastmodifieddate  = S.lastmodifieddate,
            T._loaded_at        = S._loaded_at

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (id, tranid, transactionnumber, status, type, trandate,
                    createddate, closedate, entity, employee, lastmodifieddate, _loaded_at)
            VALUES (S.id, S.tranid, S.transactionnumber, S.status, S.type, S.trandate,
                    S.createddate, S.closedate, S.entity, S.employee, S.lastmodifieddate, S._loaded_at)

        WHEN NOT MATCHED BY SOURCE
             AND T.trandate >= DATE '{cutoff_str}' THEN DELETE
    """
    merge_job = client.query(merge_sql)
    merge_job.result()
    print(f"MERGE complete.")
    print(f"  Inserted: {merge_job.dml_stats.inserted_row_count}")
    print(f"  Updated:  {merge_job.dml_stats.updated_row_count}")
    print(f"  Deleted:  {merge_job.dml_stats.deleted_row_count}")


# Transactionline Refresh strategy ##################################

TL_TABLE_REF   = f"{PROJECT_ID}.{DATASET_ID}.transactionline"
TL_STAGING_REF = f"{PROJECT_ID}.{DATASET_ID}.transactionline_staging"

TL_SELECT_COLUMNS = [
    "uniquekey", "transaction", "item", "expenseaccount", "inventorylocation",
    "createdfrom", "itemtype", "accountinglinetype", "memo",
    "netamount", "costestimate", "rate", "price", "quantity", "quantitybackordered",
    "mainline", "taxline", "custcolfree_goods_checkbox",
    "linecreateddate", "linelastmodifieddate",
]

TL_SCHEMA = [
    bigquery.SchemaField("uniquekey",                  "INTEGER"),
    bigquery.SchemaField("transaction",                "INTEGER"),
    bigquery.SchemaField("item",                       "INTEGER"),
    bigquery.SchemaField("expenseaccount",             "INTEGER"),
    bigquery.SchemaField("inventorylocation",          "INTEGER"),
    bigquery.SchemaField("createdfrom",                "INTEGER"),
    bigquery.SchemaField("itemtype",                   "STRING"),
    bigquery.SchemaField("accountinglinetype",         "STRING"),
    bigquery.SchemaField("memo",                       "STRING"),
    bigquery.SchemaField("netamount",                  "FLOAT"),
    bigquery.SchemaField("costestimate",               "FLOAT"),
    bigquery.SchemaField("rate",                       "FLOAT"),
    bigquery.SchemaField("price",                      "FLOAT"),
    bigquery.SchemaField("quantity",                   "FLOAT"),
    bigquery.SchemaField("quantitybackordered",        "FLOAT"),
    bigquery.SchemaField("mainline",                   "BOOLEAN"),
    bigquery.SchemaField("taxline",                    "BOOLEAN"),
    bigquery.SchemaField("custcolfree_goods_checkbox", "BOOLEAN"),
    bigquery.SchemaField("linecreateddate",            "DATE"),
    bigquery.SchemaField("linelastmodifieddate",       "DATE"),
    bigquery.SchemaField("_loaded_at",                 "TIMESTAMP"),
]


def cast_transactionline_df(df: pd.DataFrame) -> pd.DataFrame:
    from datetime import datetime, timezone

    int_cols = ["uniquekey", "transaction", "item", "expenseaccount",
                "inventorylocation", "createdfrom"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    float_cols = ["netamount", "costestimate", "rate", "price",
                  "quantity", "quantitybackordered"]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    bool_cols = ["mainline", "taxline", "custcolfree_goods_checkbox"]
    for col in bool_cols:
        df[col] = df[col].map({"T": True, "F": False, True: True, False: False})
    date_cols = ["linecreateddate", "linelastmodifieddate"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    df["_loaded_at"] = datetime.now(timezone.utc)
    return df


def _flush_tl_batch(chunks: list, client, lock, counter: list, max_retries: int = 6):
    """Cast, concat and WRITE_APPEND a batch of chunks to the staging table.

    Retries with exponential backoff on BigQuery 429 rate-limit errors.
    Three concurrent workers flushing frequently can exceed BQ's per-table
    write rate; backing off and retrying is safer than abandoning the slice.
    """
    from google.api_core.exceptions import TooManyRequests

    batch = pd.concat(chunks, ignore_index=True)
    batch = cast_transactionline_df(batch)

    for attempt in range(max_retries):
        try:
            job = client.load_table_from_dataframe(
                batch, TL_STAGING_REF,
                job_config=bigquery.LoadJobConfig(
                    schema=TL_SCHEMA,
                    write_disposition="WRITE_APPEND",
                    create_disposition="CREATE_IF_NEEDED",
                ),
            )
            job.result()
            with lock:
                counter[0] += job.output_rows
            return job.output_rows
        except TooManyRequests:
            if attempt == max_retries - 1:
                raise
            wait = 15 * (2 ** attempt)   # 15s, 30s, 60s, 120s, 240s ...
            print(f"  [flush] BQ 429 rate limit — waiting {wait}s "
                  f"(attempt {attempt + 1}/{max_retries})...")
            time.sleep(wait)



def _tl_cutoff() -> str:
    """Return the 2-year rolling cutoff as a YYYY-MM-DD string."""
    return (date.today() - timedelta(days=365 * 2)).strftime("%Y-%m-%d")


def _get_tl_key_range(where_clause: str, attempts: int = 4):
    """Return (min_uniquekey, max_uniquekey, row_count) for a given WHERE clause."""
    q = f"""
        SELECT MIN(uniquekey) AS mn, MAX(uniquekey) AS mx, COUNT(*) AS n
        FROM transactionline
        WHERE {where_clause}
    """
    for attempt in range(attempts):
        df = pull_data_by_sql(query=q, return_df=True)
        if df is not None and not df.empty and pd.notna(df["mn"].iloc[0]):
            return int(df["mn"].iloc[0]), int(df["mx"].iloc[0]), int(df["n"].iloc[0])
        print(f"  _get_tl_key_range attempt {attempt + 1}/{attempts} returned empty — retrying...")
    raise RuntimeError(f"Could not get uniquekey range after {attempts} attempts")


# ── Pass 1 ────────────────────────────────────────────────────────────────────

def stage_transactionline_pass1(cutoff_str: str |None = None,
                                chunk_size: int = 5000,
                                n_workers: int = 3,
                                slice_size: int = 100_000,
                                flush_every: int = 10):
    """Pull lines created within the 2-year window into the staging table.

    Clears the staging table first, then uses bounded-slice concurrent workers
    (same strategy as load_year()) for fast NetSuite index range scans.
    Call this before stage_transactionline_pass2().
    """
    if cutoff_str is None:
        cutoff_str = _tl_cutoff()

    t0 = time.time()
    print(f"[Pass 1] cutoff: {cutoff_str}")

    client = bigquery.Client(project=PROJECT_ID)
    client.delete_table(TL_STAGING_REF, not_found_ok=True)
    print("[Pass 1] Staging table cleared.")

    lock         = threading.Lock()
    staged_count = [0]

    where_clause = f"linecreateddate >= TO_DATE('{cutoff_str}', 'YYYY-MM-DD')"

    mn, mx, expected = _get_tl_key_range(where_clause)
    print(f"[Pass 1] uniquekey {mn:,}..{mx:,}  NetSuite expects {expected:,} rows")

    sq = queue.Queue()
    start = mn - 1
    while start < mx:
        end = min(start + slice_size, mx)
        sq.put((start, end))
        start = end
    print(f"[Pass 1] {sq.qsize()} slices → {n_workers} workers")

    def slice_worker(worker_id, max_slice_attempts=3):
        worker_client = bigquery.Client(project=PROJECT_ID)
        total_loaded  = 0
        attempts      = {}

        while True:
            try:
                s_start, s_end = sq.get_nowait()
            except queue.Empty:
                break

            key           = (s_start, s_end)
            attempts[key] = attempts.get(key, 0) + 1
            last_key      = s_start
            last_flushed  = s_start
            chunks        = []

            try:
                while True:
                    query = f"""
                        SELECT {', '.join(TL_SELECT_COLUMNS)}
                        FROM transactionline
                        WHERE {where_clause}
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

                    chunks.append(df)
                    last_key = int(df["uniquekey"].astype("int64").max())

                    if len(chunks) >= flush_every or len(df) < chunk_size:
                        flushed = _flush_tl_batch(chunks, worker_client, lock, staged_count)
                        print(f"[Pass 1 w{worker_id}] flushed {flushed:,} rows  "
                              f"key→{last_key}  total: {staged_count[0]:,}")
                        total_loaded += flushed
                        last_flushed  = last_key
                        chunks        = []

                    if len(df) < chunk_size:
                        break

                if chunks:
                    flushed = _flush_tl_batch(chunks, worker_client, lock, staged_count)
                    print(f"[Pass 1 w{worker_id}] final flush {flushed:,} rows")
                    total_loaded += flushed

            except Exception as e:
                traceback.print_exc()
                if attempts[key] < max_slice_attempts and last_flushed < s_end:
                    sq.put((last_flushed, s_end))
                    print(f"[Pass 1 w{worker_id}] requeued ({last_flushed}, {s_end}): {e}")
                else:
                    print(f"[Pass 1 w{worker_id}] ABANDONED ({last_flushed}, {s_end}): {e}")

        return total_loaded

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as ex:
        futures = [ex.submit(slice_worker, i) for i in range(n_workers)]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    print(f"[Pass 1] Done. Staged: {staged_count[0]:,} rows  ({time.time()-t0:.0f}s)")
    return staged_count[0]


# ── Pass 2 ────────────────────────────────────────────────────────────────────

def stage_transactionline_pass2(cutoff_str: str | None = None, chunk_size: int = 5000):
    """Pull lines created BEFORE the 2-year window but edited WITHIN it.

    These rows (~5K) are sparse across the full uniquekey range, so a simple
    serial keyset loop is used. All rows are accumulated in memory and flushed
    in a single load job. Appends to the staging table (does NOT clear it).
    Call this after stage_transactionline_pass1().
    """
    if cutoff_str is None:
        cutoff_str = _tl_cutoff()

    t0 = time.time()
    print(f"[Pass 2] cutoff: {cutoff_str}")

    client       = bigquery.Client(project=PROJECT_ID)
    lock         = threading.Lock()
    staged_count = [0]

    where_clause = (
        f"linelastmodifieddate >= TO_DATE('{cutoff_str}', 'YYYY-MM-DD') "
        f"AND linecreateddate  <  TO_DATE('{cutoff_str}', 'YYYY-MM-DD')"
    )

    buffer   = []
    last_key = 0

    while True:
        query = f"""
            SELECT {', '.join(TL_SELECT_COLUMNS)}
            FROM transactionline
            WHERE {where_clause}
              AND uniquekey > {last_key}
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
        print(f"[Pass 2] pulled to key {last_key}  ({len(df)} rows this page)")

        if len(df) < chunk_size:
            break

    if buffer:
        flushed = _flush_tl_batch(buffer, client, lock, staged_count)
        print(f"[Pass 2] Done. Staged {flushed:,} rows  ({time.time()-t0:.0f}s)")
        return flushed
    else:
        print("[Pass 2] No edited-old lines found.")
        return 0


# ── MERGE ─────────────────────────────────────────────────────────────────────

def merge_transactionline(cutoff_str: str | None = None):
    """MERGE transactionline_staging → transactionline (target).

    WHEN MATCHED          → UPDATE all columns
    WHEN NOT MATCHED      → INSERT new rows
    WHEN NOT MATCHED BY SOURCE AND linecreateddate >= cutoff → DELETE
        (only rows in the refresh window are eligible for deletion;
        rows created before cutoff that were deleted in NetSuite are
        NOT caught — accepted limitation, same as refresh_transaction).

    WARNING: if staging is incomplete (pass 1 or pass 2 failed partway),
    the DELETE guard will remove target rows that are missing from staging.
    Verify staged row count before running this.
    """
    if cutoff_str is None:
        cutoff_str = _tl_cutoff()

    t0     = time.time()
    client = bigquery.Client(project=PROJECT_ID)

    set_cols      = [c for c in TL_SELECT_COLUMNS if c != "uniquekey"]
    update_clause = ",\n            ".join(f"T.{c} = S.{c}" for c in set_cols + ["_loaded_at"])
    insert_cols   = ", ".join(TL_SELECT_COLUMNS + ["_loaded_at"])
    insert_vals   = ", ".join(f"S.{c}" for c in TL_SELECT_COLUMNS + ["_loaded_at"])

    merge_sql = f"""
        MERGE `{TL_TABLE_REF}` AS T
        USING (
            SELECT * EXCEPT (rn)
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY uniquekey
                           ORDER BY _loaded_at DESC
                       ) AS rn
                FROM `{TL_STAGING_REF}`
            )
            WHERE rn = 1
        ) AS S
        ON T.uniquekey = S.uniquekey

        WHEN MATCHED THEN UPDATE SET
            {update_clause}

        WHEN NOT MATCHED BY TARGET THEN 
            INSERT ({insert_cols})
            VALUES ({insert_vals})

        WHEN NOT MATCHED BY SOURCE
             AND T.linecreateddate >= DATE '{cutoff_str}' THEN DELETE
    """
    print(f"[MERGE] cutoff: {cutoff_str}")
    print("[MERGE] Running...")
    merge_job = client.query(merge_sql)
    merge_job.result()
    print(f"[MERGE] Done  ({time.time()-t0:.0f}s)")
    print(f"  Inserted: {merge_job.dml_stats.inserted_row_count:,}")
    print(f"  Updated:  {merge_job.dml_stats.updated_row_count:,}")
    print(f"  Deleted:  {merge_job.dml_stats.deleted_row_count:,}")


# ── Orchestrator ──────────────────────────────────────────────────────────────

@timeit
def refresh_transactionline(chunk_size: int = 5000,
                            n_workers: int = 3,
                            slice_size: int = 100_000,
                            flush_every: int = 10):
    """Run Pass 1 → Pass 2 → MERGE in sequence.

    Call the individual functions directly to test each step in isolation:
        stage_transactionline_pass1()
        stage_transactionline_pass2()
        merge_transactionline()
    """
    cutoff_str = _tl_cutoff()
    t0 = time.time()

    stage_transactionline_pass1(cutoff_str, chunk_size, n_workers, slice_size, flush_every)
    stage_transactionline_pass2(cutoff_str, chunk_size)
    merge_transactionline(cutoff_str)

    print(f"refresh_transactionline complete  ({time.time()-t0:.0f}s total)")



# ── Classification ────────────────────────────────────────────────────────────

CLASSIFICATION_SCHEMA = [
    bigquery.SchemaField("id",               "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name",             "STRING"),
    bigquery.SchemaField("parent",           "INTEGER"),
    bigquery.SchemaField("fullname",         "STRING"),
    bigquery.SchemaField("isinactive",       "BOOLEAN"),
    bigquery.SchemaField("subsidiary",       "STRING"),
    bigquery.SchemaField("lastmodifieddate", "DATE"),
    bigquery.SchemaField("_loaded_at",       "TIMESTAMP"),
]

CLASSIFICATION_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.classification"

@timeit
def load_classification():
    """Full pull → WRITE_TRUNCATE. Small reference table (~178 rows), low change frequency.
    Safe to truncate-reload on every run; auto-handles deletes.
    """

    print("[classification] Pulling from NetSuite...")

    q = """
        SELECT id, name, parent, fullname, isinactive, subsidiary, lastmodifieddate
        FROM classification
    """
    df = pull_data_by_sql(query=q, return_df=True)
    if df is None or df.empty:
        print("[classification] No rows returned")
        return
    
    print(f"[classification] {len(df):,} rows pulled")
   
    # Cast
    df["id"]               = pd.to_numeric(df["id"], errors="raise").astype("Int64")
    df["name"]             = df["name"].astype("string")
    df["parent"]           = pd.to_numeric(df["parent"], errors="coerce").astype("Int64")
    df["fullname"]         = df["fullname"].astype("string")
    df["subsidiary"]       = df["subsidiary"].astype("string")  # may be multi-value comma-separated IDs
    df["isinactive"]       = df["isinactive"].map({"T": True, "F": False, True: True, False: False})
    df["lastmodifieddate"] = pd.to_datetime(df["lastmodifieddate"]).dt.date
    df["_loaded_at"]       = pd.Timestamp.now(tz="UTC")

    # Load
    client = bigquery.Client(project=PROJECT_ID)
    job = client.load_table_from_dataframe(
        df, CLASSIFICATION_TABLE_REF,
        job_config=bigquery.LoadJobConfig(
            schema=CLASSIFICATION_SCHEMA,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        ),
    )
    job.result()
    print(f"[classification] Loaded {job.output_rows:,} rows → {CLASSIFICATION_TABLE_REF} ")


# ── Subsidiary ────────────────────────────────────────────────────────────────

SUBSIDIARY_SCHEMA = [
    bigquery.SchemaField("id",                            "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name",                          "STRING"),
    bigquery.SchemaField("fullname",                      "STRING"),
    bigquery.SchemaField("parent",                        "INTEGER"),
    bigquery.SchemaField("legalname",                     "STRING"),
    bigquery.SchemaField("custrecordcustom_practice_code","STRING"),
    bigquery.SchemaField("isinactive",                    "BOOLEAN"),
    bigquery.SchemaField("dropdownstate",                 "STRING"),
    bigquery.SchemaField("mainaddress",                   "INTEGER"),
    bigquery.SchemaField("mainaddress_text",              "STRING"),
    bigquery.SchemaField("lastmodifieddate",              "DATE"),
    bigquery.SchemaField("_loaded_at",                    "TIMESTAMP"),
]

SUBSIDIARY_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.subsidiary"

@timeit
def load_subsidiary():
    """Full pull → WRITE_TRUNCATE. Small reference table (~70 rows), monthly/quarterly cadence."""
    print("[subsidiary] Pulling from NetSuite...")

    q = """
        SELECT id, name, fullname, parent, legalname,
               custrecordcustom_practice_code, isinactive, dropdownstate,
               mainaddress, BUILTIN.DF(mainaddress) AS mainaddress_text,
               lastmodifieddate
        FROM subsidiary
    """
    df = pull_data_by_sql(query=q, return_df=True)
    if df is None or df.empty:
        print("[subsidiary] No rows returned")
        return
    print(f"[subsidiary] {len(df):,} rows pulled")

    df["id"]                             = pd.to_numeric(df["id"], errors="raise").astype("Int64")
    df["parent"]                         = pd.to_numeric(df["parent"], errors="coerce").astype("Int64")
    df["mainaddress"]                    = pd.to_numeric(df["mainaddress"], errors="coerce").astype("Int64")
    df["name"]                           = df["name"].astype("string")
    df["fullname"]                       = df["fullname"].astype("string")
    df["legalname"]                      = df["legalname"].astype("string")
    df["custrecordcustom_practice_code"] = df["custrecordcustom_practice_code"].astype("string")
    df["dropdownstate"]                  = df["dropdownstate"].astype("string")
    df["mainaddress_text"]               = df["mainaddress_text"].astype("string")
    df["isinactive"]                     = df["isinactive"].map({"T": True, "F": False, True: True, False: False})
    df["lastmodifieddate"]               = pd.to_datetime(df["lastmodifieddate"]).dt.date
    df["_loaded_at"]                     = pd.Timestamp.now(tz="UTC")

    client = bigquery.Client(project=PROJECT_ID)
    job = client.load_table_from_dataframe(
        df, SUBSIDIARY_TABLE_REF,
        job_config=bigquery.LoadJobConfig(
            schema=SUBSIDIARY_SCHEMA,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        ),
    )
    job.result()
    print(f"[subsidiary] Loaded {job.output_rows:,} rows → {SUBSIDIARY_TABLE_REF}  ")


# ── Item ──────────────────────────────────────────────────────────────────────

ITEM_SCHEMA = [
    bigquery.SchemaField("id",                         "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("fullname",                   "STRING"),
    bigquery.SchemaField("itemtype",                   "STRING"),
    bigquery.SchemaField("upccode",                    "STRING"),
    bigquery.SchemaField("mpn",                        "STRING"),
    bigquery.SchemaField("displayname",                "STRING"),
    bigquery.SchemaField("vendorname",                 "STRING"),
    bigquery.SchemaField("purchaseunit",               "INTEGER"),
    bigquery.SchemaField("parent",                     "INTEGER"),
    bigquery.SchemaField("class",                      "INTEGER"),
    bigquery.SchemaField("subsidiary",                 "STRING"),
    bigquery.SchemaField("custitemcustitem_dnd_brand", "STRING"),
    bigquery.SchemaField("manufacturer",               "STRING"),
    bigquery.SchemaField("averagecost",                "FLOAT"),   # FLOAT not NUMERIC — avoids pyarrow rescaling errors
    bigquery.SchemaField("cost",                       "FLOAT"),
    bigquery.SchemaField("lastpurchaseprice",          "FLOAT"),
    bigquery.SchemaField("costingmethoddisplay",       "STRING"),
    bigquery.SchemaField("autoleadtime",               "BOOLEAN"),
    bigquery.SchemaField("autoreorderpoint",           "BOOLEAN"),
    bigquery.SchemaField("autopreferredstocklevel",    "BOOLEAN"),
    bigquery.SchemaField("lastmodifieddate",           "DATE"),
    bigquery.SchemaField("_loaded_at",                 "TIMESTAMP"),
]

ITEM_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.item"


def load_item():
    """Full pull → WRITE_TRUNCATE. ~34K rows, twice-daily cadence (new items added frequently).
    Uses generic 'item' table (union of all item types) so transactionline FKs resolve correctly.
    """
    print("[item] Pulling from NetSuite...")

    q = """
        SELECT id, fullname, itemtype, upccode, mpn, displayname, vendorname,
               purchaseunit, parent, class, subsidiary, custitemcustitem_dnd_brand,
               manufacturer, averagecost, cost, lastpurchaseprice, costingmethoddisplay,
               autoLeadTime, autoReorderPoint, autoPreferredStockLevel, lastmodifieddate
        FROM item
        ORDER BY id, itemtype
    """
    df = pull_data_by_sql(query=q, return_df=True)
    if df is None or df.empty:
        print("[item] No rows returned")
        return
    df.columns = df.columns.str.lower()
    print(f"[item] {len(df):,} rows pulled")

    df["id"] = pd.to_numeric(df["id"], errors="raise").astype("Int64")

    for col in ["purchaseunit", "parent", "class"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["fullname", "itemtype", "upccode", "mpn", "displayname", "vendorname",
                "subsidiary", "custitemcustitem_dnd_brand", "manufacturer", "costingmethoddisplay"]:
        df[col] = df[col].astype("string")

    for col in ["averagecost", "cost", "lastpurchaseprice"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["autoleadtime", "autoreorderpoint", "autopreferredstocklevel"]:
        df[col] = df[col].map({"T": True, "F": False, True: True, False: False})

    df["lastmodifieddate"] = pd.to_datetime(df["lastmodifieddate"]).dt.date
    df["_loaded_at"]       = pd.Timestamp.now(tz="UTC")

    client = bigquery.Client(project=PROJECT_ID)
    job = client.load_table_from_dataframe(
        df, ITEM_TABLE_REF,
        job_config=bigquery.LoadJobConfig(
            schema=ITEM_SCHEMA,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        ),
    )
    job.result()
    print(f"[item] Loaded {job.output_rows:,} rows → {ITEM_TABLE_REF} ")


# ── Customer ──────────────────────────────────────────────────────────────────

CUSTOMER_SCHEMA = [
    bigquery.SchemaField("id",               "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("fullname",         "STRING"),
    bigquery.SchemaField("category",         "INTEGER"),
    bigquery.SchemaField("currency",         "INTEGER"),
    bigquery.SchemaField("email",            "STRING"),
    bigquery.SchemaField("entityid",         "STRING"),
    bigquery.SchemaField("entitystatus",     "INTEGER"),
    bigquery.SchemaField("isinactive",       "BOOLEAN"),
    bigquery.SchemaField("entitytitle",      "STRING"),
    bigquery.SchemaField("datecreated",      "DATE"),
    bigquery.SchemaField("lastmodifieddate", "DATE"),
    bigquery.SchemaField("_loaded_at",       "TIMESTAMP"),
]

CUSTOMER_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.customer"


def load_customer():
    """Full pull → WRITE_TRUNCATE. ~29K rows, twice-daily cadence.
    Uses 'customer' subtype (not generic 'entity') — transaction.entity joins to customer.id.
    """
    t0 = time.time()
    print("[customer] Pulling from NetSuite...")

    q = """
        SELECT id, fullname, category, currency, email,
               entityid, entitystatus, isinactive, entitytitle,
               datecreated, lastmodifieddate
        FROM customer
    """
    df = pull_data_by_sql(query=q, return_df=True)
    if df is None or df.empty:
        print("[customer] No rows returned")
        return
    print(f"[customer] {len(df):,} rows pulled")

    df["id"] = pd.to_numeric(df["id"], errors="raise").astype("Int64")

    for col in ["category", "currency", "entitystatus"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["fullname", "email", "entityid", "entitytitle"]:
        df[col] = df[col].astype("string")
    df["isinactive"]  = df["isinactive"].map({"T": True, "F": False, True: True, False: False})
    df["datecreated"]  = pd.to_datetime(df["datecreated"]).dt.date
    df["lastmodifieddate"] = pd.to_datetime(df["lastmodifieddate"]).dt.date
    df["_loaded_at"]  = pd.Timestamp.now(tz="UTC")

    client = bigquery.Client(project=PROJECT_ID)
    job = client.load_table_from_dataframe(
        df, CUSTOMER_TABLE_REF,
        job_config=bigquery.LoadJobConfig(
            schema=CUSTOMER_SCHEMA,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        ),
    )
    job.result()
    print(f"[customer] Loaded {job.output_rows:,} rows → {CUSTOMER_TABLE_REF}  ({time.time()-t0:.0f}s)")


# ── Transaction Status ────────────────────────────────────────────────────────

TRANSACTIONSTATUS_SCHEMA = [
    bigquery.SchemaField("id",          "STRING", mode="REQUIRED"),
    bigquery.SchemaField("trantype",    "STRING", mode="REQUIRED"),
    bigquery.SchemaField("name",        "STRING"),
    bigquery.SchemaField("fullname",    "STRING"),
    bigquery.SchemaField("friendlykey", "STRING"),
    bigquery.SchemaField("_loaded_at",  "TIMESTAMP"),
]

TRANSACTIONSTATUS_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.transactionstatus"


def load_transactionstatus():
    """Full pull → WRITE_TRUNCATE. Small lookup table, monthly/quarterly cadence."""
    t0 = time.time()
    print("[transactionstatus] Pulling from NetSuite...")

    q = """
        SELECT id, trantype, name, fullname, friendlykey
        FROM transactionstatus
    """
    df = pull_data_by_sql(query=q, return_df=True)
    if df is None or df.empty:
        print("[transactionstatus] No rows returned")
        return
           
    print(f"[transactionstatus] {len(df):,} rows pulled")

    for col in ["id", "trantype", "name", "fullname", "friendlykey"]:
        if col not in df.columns:
            df[col] = None
        df[col] = df[col].astype("string")
    df["_loaded_at"] = pd.Timestamp.now(tz="UTC")

    client = bigquery.Client(project=PROJECT_ID)
    job = client.load_table_from_dataframe(
        df, TRANSACTIONSTATUS_TABLE_REF,
        job_config=bigquery.LoadJobConfig(
            schema=TRANSACTIONSTATUS_SCHEMA,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        ),
    )
    job.result()
    print(f"[transactionstatus] Loaded {job.output_rows:,} rows → {TRANSACTIONSTATUS_TABLE_REF}  ({time.time()-t0:.0f}s)")



# ── Aggregate Item Location ───────────────────────────────────────────────────

AGGREGATE_ITEM_LOCATION_SCHEMA = [
    bigquery.SchemaField("item",                        "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("location",                    "INTEGER"),
    bigquery.SchemaField("quantityavailable",           "FLOAT"),
    bigquery.SchemaField("quantityonhand",              "FLOAT"),
    bigquery.SchemaField("quantityintransit",           "FLOAT"),
    bigquery.SchemaField("quantitycommitted",           "FLOAT"),
    bigquery.SchemaField("quantitybackordered",         "FLOAT"),
    bigquery.SchemaField("quantityonorder",             "FLOAT"),
    bigquery.SchemaField("averagecostmli",              "FLOAT"),
    bigquery.SchemaField("lastpurchasepricemli",        "FLOAT"),
    bigquery.SchemaField("onhandvaluemli",              "FLOAT"),
    bigquery.SchemaField("reorderpoint",                "FLOAT"),
    bigquery.SchemaField("safetystocklevel",            "FLOAT"),
    bigquery.SchemaField("preferredstocklevel",         "FLOAT"),
    bigquery.SchemaField("leadtime",                    "INTEGER"),
    bigquery.SchemaField("lastquantityavailablechange", "DATE"),
    bigquery.SchemaField("lastmodifieddate",            "DATE"),
    bigquery.SchemaField("_loaded_at",                  "TIMESTAMP"),
]

AGGREGATE_ITEM_LOCATION_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.aggregateItemLocation"


@timeit
def load_aggregate_item_location(chunk_size: int = 5000):
    """Full pull via keyset pagination -> WRITE_TRUNCATE.
    Clustered on location.  Call on-demand before reorder-point jobs."""
    print("[aggregateItemLocation] Pulling from NetSuite...")

    client        = bigquery.Client(project=PROJECT_ID)
    all_chunks    = []
    last_item, last_location = 0, 0

    while True:
        query = f"""
            SELECT item, location,
                   quantityavailable, quantityonhand, quantityintransit,
                   quantitycommitted, quantitybackordered, quantityonorder,
                   averagecostmli, lastpurchasepricemli, onhandvaluemli,
                   reorderpoint, safetystocklevel, preferredstocklevel,
                   leadtime, lastquantityavailablechange, lastmodifieddate
            FROM aggregateItemLocation
            WHERE item > {last_item}
               OR (item = {last_item} AND location > {last_location})
            ORDER BY item, location
            FETCH NEXT {chunk_size} ROWS ONLY
        """
        df = pull_data_by_sql(query=query, return_df=True)

        if df is None or df.empty:
            print("[aggregateItemLocation] Load complete (empty page).")
            break

        all_chunks.append(df)
        last_row      = df.iloc[-1]
        last_item     = int(last_row["item"])
        last_location = int(last_row["location"]) if pd.notna(last_row["location"]) else last_location
        print(f"[aggregateItemLocation] ... item={last_item}, loc={last_location}  ({len(df)} rows)")

        if len(df) < chunk_size:
            print("[aggregateItemLocation] Load complete.")
            break

    full_df = pd.concat(all_chunks, ignore_index=True)
    print(f"[aggregateItemLocation] {len(full_df):,} rows total -- casting types...")

    full_df["item"]     = pd.to_numeric(full_df["item"],     errors="raise").astype("Int64")
    full_df["location"] = pd.to_numeric(full_df["location"], errors="coerce").astype("Int64")
    full_df["leadtime"] = pd.to_numeric(full_df["leadtime"], errors="coerce").astype("Int64")

    float_cols = [
        "quantityavailable", "quantityonhand", "quantityintransit", "quantitycommitted",
        "quantitybackordered", "quantityonorder", "averagecostmli", "lastpurchasepricemli",
        "onhandvaluemli", "reorderpoint", "safetystocklevel", "preferredstocklevel",
    ]
    for col in float_cols:
        full_df[col] = pd.to_numeric(full_df[col], errors="coerce")

    full_df["lastquantityavailablechange"] = (
        pd.to_datetime(full_df["lastquantityavailablechange"], errors="coerce").dt.date
    )
    full_df["lastmodifieddate"] = (
        pd.to_datetime(full_df["lastmodifieddate"], errors="coerce").dt.date
    )
    full_df["_loaded_at"] = pd.Timestamp.now(tz="UTC")

    job = client.load_table_from_dataframe(
        full_df, AGGREGATE_ITEM_LOCATION_TABLE_REF,
        job_config=bigquery.LoadJobConfig(
            schema=AGGREGATE_ITEM_LOCATION_SCHEMA,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            clustering_fields=["location"],
        ),
    )
    job.result()
    print(
        f"[aggregateItemLocation] Loaded {job.output_rows:,} rows -> "
        f"{AGGREGATE_ITEM_LOCATION_TABLE_REF}"
    )


def main():
    import argparse

    JOBS = {
        "transactionline":         refresh_transactionline,
        "transaction":             refresh_transaction,
        "item":                    load_item,
        "customer":                load_customer,
        "classification":          load_classification,
        "subsidiary":              load_subsidiary,
        "transactionstatus":       load_transactionstatus,
        "aggregate_item_location": load_aggregate_item_location,
    }

    parser = argparse.ArgumentParser(description="NetSuite -> BigQuery refresh")
    parser.add_argument(
        "--job",
        required=True,
        choices=list(JOBS),
        help="Which table/job to refresh",
    )
    args = parser.parse_args()
    JOBS[args.job]()


if __name__ == "__main__":
    main()
