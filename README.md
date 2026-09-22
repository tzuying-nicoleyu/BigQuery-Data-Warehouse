# NetSuite → BigQuery Pipeline

**Updated:** 2026-09-22 | **Status:** Complete — moving to downstream analytics

---

## File Structure

```
BigQuery Pipeline/
├── refresh.py               # All incremental refresh functions + orchestrators
├── backfill.py              # Full historical loads (run manually / disaster recovery)
├── extract_from_netsuite.py # NetSuite OAuth + SuiteQL client
├── wrapper.py               # @timeit decorator and shared utilities
├── requirements.txt
└── .github/workflows/
    ├── refresh-transaction-transactionline.yml  # daily 02:00 UTC
    ├── refresh-item-customer.yml                # twice daily 06:00 + 18:00 UTC
    ├── refresh-reference.yml                    # weekly Sunday 00:00 UTC
    ├── refresh-transactionline-full.yml         # monthly 1st 02:00 UTC
    └── refresh-on-demand.yml                    # manual trigger via GitHub UI
```

> `bigquery.ipynb` has been retired — all functions are now in `refresh.py` or `backfill.py`.

---

## Table Status & Refresh Schedule

| Table | Rows | Cadence | Function |
|---|---|---|---|
| classification | ~178 | weekly | `load_classification()` |
| subsidiary | ~70 | weekly | `load_subsidiary()` |
| transactionstatus | ~324 | weekly | `load_transactionstatus()` |
| item | ~34K | twice daily | `load_item()` |
| customer | ~29K | twice daily | `load_customer()` |
| aggregateItemLocation | ~348K | on-demand | `load_aggregate_item_location()` |
| transaction | ~1.26M | daily | `refresh_transaction()` |
| transactionline | ~7.9M total / ~2.6M window | daily + monthly full | `refresh_transactionline()` |

---

## GitHub Actions Schedule

| Workflow | Cron | When | Jobs |
|---|---|---|---|
| refresh-transaction-transactionline | `0 2 * * *` | daily 02:00 UTC | transaction + transactionline |
| refresh-item-customer | `0 6,18 * * *` | twice daily 06:00 + 18:00 | item, customer |
| refresh-reference | `0 0 * * 0` | weekly Sunday 00:00 UTC | classification, subsidiary, transactionstatus |
| refresh-transactionline-full | `0 2 1 * *` | monthly — 1st 02:00 UTC | transactionline_full (Pass 1 + Pass 2 + merge) |
| refresh-on-demand | workflow_dispatch | manual (GitHub UI) | any single job via dropdown |

**Required secrets:** `REALM`, `CONSUMER_KEY`, `CONSUMER_SECRET`, `TOKEN_ID`, `TOKEN_SECRET`, `ACCOUNT_HOST`, `GCP_SERVICE_ACCOUNT_KEY`

---

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Pagination | Bounded-slice keyset (`uniquekey > start AND ≤ end`) | Two-sided bound = O(slice) index scan; open upper bound = O(table) full scan |
| Concurrency | Worker threads drain a shared slice queue | Parallelises NetSuite fetching; each worker owns its own BigQuery client |
| TL daily vs monthly | daily = Pass 1 + merge; monthly = Pass 1 + Pass 2 + merge | Pass 2 (~19K sparse rows) runs ~35s/call; monthly cadence acceptable |
| Pass 2 strategy | Serial keyset, NOT concurrent slices | Sparse data (19K rows across 13M key range) → hundreds of tiny BQ load jobs → 429 rate limit |
| aggregateItemLocation | Bounded-slice, 3 workers, 50K item slices | Dense data; concurrent reduced runtime from 263s to ~3 min |
| transaction refresh | Rolling 2-year window MERGE | Catches edits + deletes within window; quota-safe (bounded partition touches) |
| Full backfill | Separate `backfill.py`, run manually | Not scheduled — used for initial load and disaster recovery only |

---

## transactionline — Two-Pass Strategy

**Why two passes?**
A single `linelastmodifieddate >= cutoff` filter misses rows created before the cutoff that were never modified since. Without Pass 2, those rows vanish from staging and the MERGE DELETE guard removes them from BigQuery.

### Pass 1 — New lines (~2.6M rows · dense · daily)

```sql
linecreateddate >= cutoff
```

Concurrent bounded-slice workers. Upper bound mandatory for O(slice) index scan. Runs every day before the MERGE.

### Pass 2 — Edited-old lines (~19K rows · sparse · monthly)

```sql
linelastmodifieddate >= cutoff AND linecreateddate < cutoff
```

Serial keyset loop. ~4 API calls total. Concurrent slices trigger 429 (19K rows across 13M key range → ~2 rows/slice).

### MERGE

Deduplication subquery required — rows where both dates ≥ cutoff appear in both passes.
DELETE guard scoped to `linecreateddate >= cutoff` to limit blast radius.

### argparse jobs

```python
JOBS = {
    "transactionline":      lambda: refresh_transactionline(full=False),  # daily
    "transactionline_full": lambda: refresh_transactionline(full=True),   # monthly
    ...
}
```

---

## Backfill Commands

All backfill functions live in `backfill.py` and are run manually — not scheduled.

```bash
# Full WRITE_TRUNCATE — transaction
python backfill.py --job backfill_transaction

# All years — transactionline (2018–2026, ~7.9M rows)
python backfill.py --job backfill_transactionline

# Specific years — disaster recovery
python backfill.py --job backfill_transactionline --years 2025 2026
```

**Historical row counts — transactionline:**

| Year | Rows |
|---|---|
| 2018 | 182K |
| 2019 | 833K |
| 2020 | 1.01M |
| 2021 | 907K |
| 2022 | 864K |
| 2023 | 948K |
| 2024 | 1.06M |
| 2025 | 1.14M |
| 2026 | 972K |

---

## Next Steps

- **Downstream analytics** — top products analysis (query BigQuery, not NetSuite)
- **Vendor table** — `transaction.entity` for non-sales transactions is currently an unjoined ID; needs a vendor dimension
