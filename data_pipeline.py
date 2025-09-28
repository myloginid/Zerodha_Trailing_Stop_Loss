"""
Data pipeline helpers for holdings and funds persistence and date logic.

Centralizes:
- IST date utilities and snapshot date selection
- Path helpers (JSONL/Parquet) for holdings and funds
- Existence checks and Parquet backfill from JSONL
- Normalize + persist functions for holdings and funds
- Missing-data checks per account and login necessity
"""


import os
import json
from datetime import datetime as _dt, timedelta
import pytz


# -------- IST date utilities --------

def ist_today_str() -> str:
    tz = pytz.timezone("Asia/Kolkata")
    return _dt.now(tz).strftime("%Y-%m-%d")


def ist_last_business_date_str() -> str:
    tz = pytz.timezone("Asia/Kolkata")
    dt = _dt.now(tz)
    wd = dt.weekday()  # Mon=0..Sun=6
    if wd == 5:  # Sat -> Fri
        dt = dt - timedelta(days=1)
    elif wd == 6:  # Sun -> Fri
        dt = dt - timedelta(days=2)
    return dt.strftime("%Y-%m-%d")


# -------- Path helpers (holdings) --------

def holdings_jsonl_path(base_dir: str, account: str, date_str: str) -> str:
    return os.path.join(base_dir, "data", "holdings_jsonl", account, f"{date_str}.jsonl")


def holdings_parquet_path(base_dir: str, account: str, date_str: str) -> str:
    return os.path.join(base_dir, "data", "holdings_parquet", f"account={account}", f"date={date_str}", "holdings.parquet")


def holdings_already_persisted(base_dir: str, account: str, date_str: str) -> bool:
    p = holdings_jsonl_path(base_dir, account, date_str)
    return os.path.exists(p) and os.path.getsize(p) > 0


def ensure_holdings_parquet_from_jsonl(base_dir: str, account: str, date_str: str):
    jsonl = holdings_jsonl_path(base_dir, account, date_str)
    parquet = holdings_parquet_path(base_dir, account, date_str)
    if os.path.exists(parquet) and os.path.getsize(parquet) > 0:
        return
    if not os.path.exists(jsonl):
        return
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        os.makedirs(os.path.dirname(parquet), exist_ok=True)
        rows = []
        with open(jsonl, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
        if not rows:
            return
        pq.write_table(pa.Table.from_pylist(rows), parquet)
    except Exception:
        pass


# -------- Path helpers (funds) --------

def funds_jsonl_path(base_dir: str, account: str, date_str: str) -> str:
    return os.path.join(base_dir, "data", "funds_jsonl", account, f"{date_str}.jsonl")


def funds_parquet_path(base_dir: str, account: str, date_str: str) -> str:
    return os.path.join(base_dir, "data", "funds_parquet", f"account={account}", f"date={date_str}", "funds.parquet")


def funds_already_persisted(base_dir: str, account: str, date_str: str) -> bool:
    p = funds_jsonl_path(base_dir, account, date_str)
    return os.path.exists(p) and os.path.getsize(p) > 0


def ensure_funds_parquet_from_jsonl(base_dir: str, account: str, date_str: str):
    jsonl = funds_jsonl_path(base_dir, account, date_str)
    parquet = funds_parquet_path(base_dir, account, date_str)
    if os.path.exists(parquet) and os.path.getsize(parquet) > 0:
        return
    if not os.path.exists(jsonl):
        return
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        rows = []
        with open(jsonl, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
        if not rows:
            return
        os.makedirs(os.path.dirname(parquet), exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), parquet)
    except Exception:
        pass


# -------- Normalize + persist --------

def normalize_holdings(holdings_list: list[dict], account: str, as_of_date: str, as_of_ts: str | None = None) -> list[dict]:
    if as_of_ts is None:
        tz = pytz.timezone("Asia/Kolkata")
        as_of_ts = _dt.now(tz).strftime("%Y-%m-%dT%H:%M:%S")
    records = []
    for h in holdings_list or []:
        records.append({
            "account": account,
            "as_of_date": as_of_date,
            "as_of_ts": as_of_ts,
            "tradingsymbol": h.get("tradingsymbol"),
            "exchange": h.get("exchange"),
            "instrument_token": h.get("instrument_token"),
            "isin": h.get("isin"),
            "product": h.get("product"),
            "quantity": h.get("quantity"),
            "used_quantity": h.get("used_quantity"),
            "t1_quantity": h.get("t1_quantity"),
            "realised_quantity": h.get("realised_quantity"),
            "opening_quantity": h.get("opening_quantity"),
            "short_quantity": h.get("short_quantity"),
            "collateral_quantity": h.get("collateral_quantity"),
            "average_price": h.get("average_price"),
            "last_price": h.get("last_price"),
            "close_price": h.get("close_price"),
            "pnl": h.get("pnl"),
            "day_change": h.get("day_change"),
            "day_change_percentage": h.get("day_change_percentage"),
        })
    return records


def persist_holdings(base_dir: str, account: str, holdings_list: list[dict], date_str: str) -> dict:
    recs = normalize_holdings(holdings_list, account, date_str)
    if not recs:
        return {"jsonl": None, "parquet": None}
    jsonl_path = holdings_jsonl_path(base_dir, account, date_str)
    os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)
    with open(jsonl_path, "w") as f:
        for row in recs:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
    parquet_path = holdings_parquet_path(base_dir, account, date_str)
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
        pq.write_table(pa.Table.from_pylist(recs), parquet_path)
        wrote_parquet = True
    except Exception:
        wrote_parquet = False
    return {"jsonl": jsonl_path, "parquet": parquet_path if wrote_parquet else None}


def normalize_funds(funds: dict, account: str, as_of_date: str) -> list[dict]:
    tz = pytz.timezone("Asia/Kolkata")
    as_of_ts = _dt.now(tz).strftime("%Y-%m-%dT%H:%M:%S")
    records = []
    for segment in ("equity", "commodity"):
        seg = (funds or {}).get(segment)
        if not seg:
            continue
        available = seg.get("available") or {}
        records.append({
            "account": account,
            "segment": segment,
            "as_of_date": as_of_date,
            "as_of_ts": as_of_ts,
            "available_cash": available.get("cash", 0.0),
            "net": seg.get("net", 0.0),
            "available_collateral": available.get("collateral", 0.0),
        })
    return records


def persist_funds(base_dir: str, account: str, funds: dict, date_str: str) -> dict:
    recs = normalize_funds(funds, account, date_str)
    if not recs:
        return {"jsonl": None, "parquet": None}
    jsonl_path = funds_jsonl_path(base_dir, account, date_str)
    os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)
    with open(jsonl_path, "w") as f:
        for row in recs:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
    parquet_path = funds_parquet_path(base_dir, account, date_str)
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
        pq.write_table(pa.Table.from_pylist(recs), parquet_path)
        wrote_parquet = True
    except Exception:
        wrote_parquet = False
    return {"jsonl": jsonl_path, "parquet": parquet_path if wrote_parquet else None}


# -------- Missing checks and login planner --------

def compute_missing_maps(base_dir: str, accounts: list[str], date_str: str) -> tuple[dict, dict]:
    missing_h = {n: not holdings_already_persisted(base_dir, n, date_str) for n in accounts}
    missing_f = {n: not funds_already_persisted(base_dir, n, date_str) for n in accounts}
    return missing_h, missing_f


def load_token_for_account(tokens_dir: str, account: str, acceptable_dates: list[str]) -> str | None:
    token_file = os.path.join(tokens_dir, f"{account}_access_token.json")
    try:
        if os.path.exists(token_file):
            with open(token_file, "r") as f:
                data = json.load(f)
            if data.get("date") in acceptable_dates:
                return data.get("access_token")
    except Exception:
        pass
    return None


def compute_accounts_to_login(base_dir: str, tokens_dir: str, accounts: list[str], date_str: str) -> list[str]:
    missing_h, missing_f = compute_missing_maps(base_dir, accounts, date_str)
    acceptable = [date_str, ist_today_str()]
    needs = []
    for n in accounts:
        if missing_h[n]:
            needs.append(n)
            continue
        if missing_f[n] and not load_token_for_account(tokens_dir, n, acceptable):
            needs.append(n)
    return needs
