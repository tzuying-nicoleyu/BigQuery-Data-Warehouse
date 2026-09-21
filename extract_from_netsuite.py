import pandas as pd
import os
import re
import time 

from pathlib import Path
from dotenv import load_dotenv
from requests_oauthlib import OAuth1Session

import json


def _parse_select_columns(query: str) -> list[str] | None:
    """
    Best-effort parser: return column names/aliases in the order they appear
    in the SELECT clause. Returns None if we can't confidently parse the
    query (e.g. `SELECT *`, complex expressions without aliases).
    """
    q = re.sub(r"--[^\n]*", "", query)
    q = re.sub(r"/\*.*?\*/", "", q, flags=re.S)

    m = re.search(r"\bselect\b(.*?)\bfrom\b", q, flags=re.I | re.S)
    if not m:
        return None
    body = m.group(1).strip()
    body = re.sub(r"^distinct\s+(on\s*\([^)]*\)\s+)?", "", body, flags=re.I)

    parts, buf, depth = [], [], 0
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append("".join(buf).strip())

    columns = []
    for part in parts:
        if not part or part == "*" or part.endswith(".*"):
            return None
        alias_m = re.search(r"\s+as\s+([a-zA-Z_][\w$]*)\s*$", part, flags=re.I)
        if alias_m:
            columns.append(alias_m.group(1).lower())
            continue
        tokens = part.split()
        last = tokens[-1]
        if (
            len(tokens) > 1
            and re.fullmatch(r"[a-zA-Z_][\w$]*", last)
            and last.lower() not in {"asc", "desc"}
        ):
            columns.append(last.lower())
            continue
        tail = re.search(r"([a-zA-Z_][\w$]*)\s*$", part)
        if tail:
            columns.append(tail.group(1).lower())
        else:
            return None
    return columns

def _load_env():
    """
    Walk up from this file looking for a .env, warn if not found.
    """
    curr = Path(__file__).resolve().parent
    for path in (curr/ ".env", curr.parent/".env"):
        if path.exists():
            load_dotenv(path, override=True)
            return 
    # No .env found — fall back to environment variables (e.g. GitHub Actions / Cloud Run).

def _get_credentials(sandbox: bool) -> dict:
    """
    Return the sandbox credentials if sanbox is True, else return production credentials.
    """
    if sandbox:
        prefix = "SB_"
    else:
        prefix = ""
    credentials = {
        "realm":           os.getenv(f"{prefix}REALM"),
        "consumer_key":    os.getenv(f"{prefix}CONSUMER_KEY"),
        "consumer_secret": os.getenv(f"{prefix}CONSUMER_SECRET"),
        "token_id":        os.getenv(f"{prefix}TOKEN_ID"),
        "token_secret":    os.getenv(f"{prefix}TOKEN_SECRET"),
        "account_host":    os.getenv(f"{prefix}ACCOUNT_HOST")}
    missing = [k for k, v in credentials.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing env vars for {'sandbox' if sandbox else 'production'}: "
            f"{[f'{prefix}{k.upper()}' for k in missing]}"
        )
    return credentials

def _build_oauth_session(creds:dict) -> tuple[OAuth1Session, str]:
    """
    Return (oauth_session, suiteql_url)
    """
    oauth = OAuth1Session(
        client_key=creds["consumer_key"],
        client_secret=creds["consumer_secret"],
        resource_owner_key=creds["token_id"],
        resource_owner_secret=creds["token_secret"],
        realm=creds["realm"], # type: ignore
        signature_method="HMAC-SHA256",
    )
    url = f"https://{creds['account_host']}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    return oauth, url

def _post_with_retry(oauth, url: str, headers: dict, query: str, max_retries: int = 3, report=print):
    """
    POST to url, retrying up to max_retries times on 429.
    """
    for attempt in range(max_retries):
        response = oauth.post(url, headers = headers, json = {"q": query})
        if response.status_code == 200:
            return response
        wait = 2 ** attempt
        report(f"got {response.status_code}, waiting {wait}s (attempt {attempt + 1}/{max_retries})...")
        time.sleep(wait)
    error = json.loads(response.text)
    detail = error.get("o:errorDetails", [{}])[0].get("detail", response.text)
    raise RuntimeError(f"Max retries exceeded. Last response: {response.status_code}\n{detail}")
        
        
def _fetch_all_pages(oauth, url: str, headers: dict, query: str, report=print) -> list:
    """
    Fetch all paginated results and return as a flat list.
    """
    start = time.time()
    response_json = _post_with_retry(oauth, url, headers, query, report=report).json()
    results = response_json.get("items", [])
    report(f"Total results: {response_json.get('totalResults', 'unknown')}")

    while response_json.get("hasMore"):
        next_link = next(
            (link["href"] for link in response_json.get("links", []) if link.get("rel") == "next"),
            None,
        )
        if not next_link:
            break

        page_start = time.time()
        response_json = _post_with_retry(oauth, next_link, headers, query, report=report).json()
        results.extend(response_json.get("items", []))
        report(f"Fetched: {len(results)} | Page time: {time.time() - page_start:.2f}s")

    report(f"Final count: {len(results)} | Total time: {time.time() - start:.2f}s")
    return results

def _to_dataframe(results:list, column_order: list[str] | None = None, report=print) ->pd.DataFrame:
    """
    Convert result to a DataFrame, dropping the NetSuite default 'links' column.
    If column_order is given, reorder columns to match it (case-insensitive);
    any unmatched columns are appended at the end.
    """
    df = pd.DataFrame(results)
    if df.empty:
        report("No rows returned.")
        return df
    if "links" in df.columns:
        df = df.drop(columns=["links"])
    if column_order:
        lower_to_actual = {c.lower(): c for c in df.columns}
        ordered = [lower_to_actual[c] for c in column_order if c in lower_to_actual]
        remaining = [c for c in df.columns if c not in ordered]
        df = df[ordered + remaining]
    return df

def _to_csv(df: pd.DataFrame, file_name:str, report=print):
    folder = "output_csv_files"
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, file_name)
    df.to_csv(file_path, index=False)
    report(f"Saved to {file_path}")

# ── main entry point ─────────────────────────────────────────────────────────

def pull_data_by_sql(sandbox=False, query="", file_name="output.csv", return_df=False, on_progress=None)-> pd.DataFrame|None :
    """
    Run a SuiteQL query.

    on_progress: optional callable(str) that receives progress messages.
                 Defaults to print, so notebook/CLI behaviour is unchanged.
    """
    report = on_progress or print

    _load_env()

    creds = _get_credentials(sandbox)
    oauth, suiteql_url = _build_oauth_session(creds)
    headers = {"Content-Type": "application/json", "Prefer": "transient"}

    try:
        results = _fetch_all_pages(oauth, suiteql_url, headers, query, report=report)
        df = _to_dataframe(results, column_order=_parse_select_columns(query), report=report)

        if return_df:
            return df
        _to_csv(df, file_name, report=report)

    except Exception as e:
        report(f"Exception type: {type(e).__name__}")
        report(f"Exception message: {e}")
        raise