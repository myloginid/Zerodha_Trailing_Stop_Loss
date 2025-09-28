"""
Trailing Stop Loss (TSL) logic and analytics helpers.

Provides:
- Parquet glob + DuckDB view creation over holdings history
- TSL evaluation per account and consolidated across accounts
- Snapshot queries and printing utilities
"""

import os

# Symbols to exclude from TSL analytics (e.g., cash/money-market proxies)
EXCLUDED_SYMBOLS = {"LIQUIDCASE"}


def holdings_parquet_glob(base_dir: str = None) -> str:
    base = base_dir or os.path.dirname(__file__)
    return os.path.join(base, 'data', 'holdings_parquet', 'account=*', 'date=*', 'holdings.parquet')


def duckdb_connect_with_holdings_view(base_dir: str = None):
    try:
        import duckdb
    except Exception as e:
        print(f"duckdb not available: {e}")
        return None

    con = duckdb.connect(database=':memory:')
    glob = holdings_parquet_glob(base_dir)
    glob_sql = glob.replace("'", "''")
    con.execute(f"CREATE VIEW holdings_all AS SELECT * FROM read_parquet('{glob_sql}');")
    return con


def _eval_tsl_action(avg_cost: float, last_price: float, peak_price: float | None, quantity: int):
    if not avg_cost or not last_price or quantity is None:
        return {'action': 'hold', 'exit_fraction': 0.0, 'exit_qty': 0,
                'pnl_pct': None, 'drawdown_pct': None, 'loss_pct': None}

    pnl_pct = (last_price - avg_cost) / avg_cost * 100.0
    loss_pct = max(0.0, (avg_cost - last_price) / avg_cost * 100.0)
    drawdown_pct = None
    if peak_price and peak_price > 0:
        drawdown_pct = max(0.0, (peak_price - last_price) / peak_price * 100.0)

    action = 'hold'
    exit_fraction = 0.0

    # Hard loss stops
    if loss_pct >= 20.0:
        action, exit_fraction = 'exit_all', 1.0
    elif loss_pct >= 15.0:
        action, exit_fraction = 'trim_50', 0.5
    else:
        if pnl_pct < 5.0:
            if pnl_pct < 1.5:
                action, exit_fraction = 'exit_all', 1.0
        else:
            if drawdown_pct is not None:
                if drawdown_pct >= 25.0:
                    action, exit_fraction = 'exit_all', 1.0
                elif drawdown_pct >= 15.0:
                    action, exit_fraction = 'trim_50', 0.5

    exit_qty = int(quantity * exit_fraction)
    if exit_fraction > 0 and quantity > 0 and exit_qty == 0:
        exit_qty = 1

    return {
        'action': action,
        'exit_fraction': exit_fraction,
        'exit_qty': exit_qty,
        'pnl_pct': pnl_pct,
        'drawdown_pct': drawdown_pct,
        'loss_pct': loss_pct,
    }


def compute_trailing_stop_signals(con, for_date: str | None = None):
    if for_date is None:
        cur = con.execute("SELECT max(as_of_date) AS latest FROM holdings_all")
        latest = cur.fetchone()[0]
    else:
        latest = for_date
    if latest is None:
        return {'as_of_date': None, 'per_account': [], 'consolidated': []}

    sql_per = """
    WITH snap AS (
      SELECT account, tradingsymbol,
             SUM(quantity)::BIGINT AS quantity,
             SUM(quantity * average_price) / NULLIF(SUM(quantity), 0) AS avg_cost,
             MAX(last_price) AS last_price
      FROM holdings_all
      WHERE as_of_date = ? AND tradingsymbol <> 'LIQUIDCASE'
      GROUP BY account, tradingsymbol
    ),
    peak AS (
      SELECT account, tradingsymbol, MAX(last_price) AS peak_price
      FROM holdings_all
      WHERE as_of_date <= ? AND tradingsymbol <> 'LIQUIDCASE'
      GROUP BY account, tradingsymbol
    )
    SELECT s.account, s.tradingsymbol, s.quantity, s.avg_cost, s.last_price, p.peak_price
    FROM snap s LEFT JOIN peak p USING(account, tradingsymbol)
    ORDER BY s.account, s.tradingsymbol
    """
    cur = con.execute(sql_per, [latest, latest])
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    per_account_rows = [dict(zip(cols, r)) for r in rows]

    sql_con = """
    WITH snap AS (
      SELECT tradingsymbol,
             SUM(quantity)::BIGINT AS quantity,
             SUM(quantity * average_price) / NULLIF(SUM(quantity), 0) AS avg_cost,
             MAX(last_price) AS last_price
      FROM holdings_all
      WHERE as_of_date = ? AND tradingsymbol <> 'LIQUIDCASE'
      GROUP BY tradingsymbol
    ),
    peak AS (
      SELECT tradingsymbol, MAX(last_price) AS peak_price
      FROM holdings_all
      WHERE as_of_date <= ? AND tradingsymbol <> 'LIQUIDCASE'
      GROUP BY tradingsymbol
    )
    SELECT s.tradingsymbol, s.quantity, s.avg_cost, s.last_price, p.peak_price
    FROM snap s LEFT JOIN peak p USING(tradingsymbol)
    ORDER BY s.tradingsymbol
    """
    cur = con.execute(sql_con, [latest, latest])
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    consolidated_rows = [dict(zip(cols, r)) for r in rows]

    per_signals = []
    for r in per_account_rows:
        qty = r['quantity'] or 0
        last = r['last_price'] or 0.0
        avg = r['avg_cost'] or 0.0
        cur_value = qty * last
        pnl_amount = qty * (last - avg)
        m = _eval_tsl_action(r['avg_cost'], r['last_price'], r.get('peak_price'), r['quantity'])
        per_signals.append({
            'account': r['account'],
            'tradingsymbol': r['tradingsymbol'],
            'quantity': r['quantity'],
            'avg_cost': r['avg_cost'],
            'last_price': r['last_price'],
            'peak_price': r.get('peak_price'),
            'value': cur_value,
            'pnl_amount': pnl_amount,
            **m,
        })

    acc_map = {}
    for r in per_account_rows:
        acc_map.setdefault(r['tradingsymbol'], []).append({'account': r['account'], 'quantity': r['quantity']})

    cons_signals = []
    for r in consolidated_rows:
        qty = r['quantity'] or 0
        last = r['last_price'] or 0.0
        avg = r['avg_cost'] or 0.0
        cur_value = qty * last
        pnl_amount = qty * (last - avg)
        m = _eval_tsl_action(r['avg_cost'], r['last_price'], r.get('peak_price'), r['quantity'])
        allocation = []
        if m['exit_qty'] > 0 and r['quantity'] and r['tradingsymbol'] in acc_map:
            total_qty = r['quantity']
            for acc in acc_map[r['tradingsymbol']]:
                q = acc['quantity']
                alloc = int(round(m['exit_qty'] * (q / total_qty)))
                if alloc > q:
                    alloc = q
                allocation.append({'account': acc['account'], 'qty': alloc})
            diff = m['exit_qty'] - sum(a['qty'] for a in allocation)
            i = 0
            while diff != 0 and i < len(allocation):
                if diff > 0 and allocation[i]['qty'] < acc_map[r['tradingsymbol']][i]['quantity']:
                    allocation[i]['qty'] += 1
                    diff -= 1
                elif diff < 0 and allocation[i]['qty'] > 0:
                    allocation[i]['qty'] -= 1
                    diff += 1
                i = (i + 1) % max(1, len(allocation))

        cons_signals.append({
            'tradingsymbol': r['tradingsymbol'],
            'total_quantity': r['quantity'],
            'avg_cost': r['avg_cost'],
            'last_price': r['last_price'],
            'peak_price': r.get('peak_price'),
            'value': cur_value,
            'pnl_amount': pnl_amount,
            **m,
            'allocations': allocation,
        })

    return {'as_of_date': latest, 'per_account': per_signals, 'consolidated': cons_signals}


def print_trailing_stop_summary(signals, max_rows_per_section: int = 20):
    as_of = signals.get('as_of_date')
    print("\n" + "-"*60)
    print(f"Trailing Stop Suggestions as of {as_of}")
    print("-"*60)

    print("Per-account:")
    shown = 0
    per_sorted = sorted(
        [s for s in signals.get('per_account', []) if s.get('action') != 'hold'],
        key=lambda x: (x.get('value') or 0.0, x.get('pnl_amount') or 0.0),
        reverse=True,
    )
    for s in per_sorted:
        if s['action'] == 'hold':
            continue
        val = int(round(s.get('value', 0) or 0))
        pnl_amt = int(round(s.get('pnl_amount', 0) or 0))
        print(
            f"  [{s['account']}] {s['tradingsymbol']}: {s['action']} "
            f"qty={s['exit_qty']} | value={val} pnl_amt={pnl_amt} "
            f"pnl={s['pnl_pct']:.2f}% loss={s['loss_pct']:.2f}% dd={s['drawdown_pct'] if s['drawdown_pct'] is not None else 0:.2f}%"
        )
        shown += 1
        if shown >= max_rows_per_section:
            break
    if shown == 0:
        print("  (no actions)")

    print("\nConsolidated across accounts:")
    shown = 0
    cons_sorted = sorted(
        [s for s in signals.get('consolidated', []) if s.get('action') != 'hold'],
        key=lambda x: (x.get('value') or 0.0, x.get('pnl_amount') or 0.0),
        reverse=True,
    )
    for s in cons_sorted:
        if s['action'] == 'hold':
            continue
        alloc_str = ", ".join(f"{a['account']}:{a['qty']}" for a in s.get('allocations', []) if a['qty'])
        val = int(round(s.get('value', 0) or 0))
        pnl_amt = int(round(s.get('pnl_amount', 0) or 0))
        print(
            f"  {s['tradingsymbol']}: {s['action']} qty={s['exit_qty']} "
            f"| value={val} pnl_amt={pnl_amt} "
            f"pnl={s['pnl_pct']:.2f}% loss={s['loss_pct']:.2f}% dd={s['drawdown_pct'] if s['drawdown_pct'] is not None else 0:.2f}% "
            f"| alloc: {alloc_str}"
        )
        shown += 1
        if shown >= max_rows_per_section:
            break
    if shown == 0:
        print("  (no actions)")


def _query_latest_snapshots(con, for_date: str | None = None):
    if for_date is None:
        cur = con.execute("SELECT max(as_of_date) AS latest FROM holdings_all")
        latest = cur.fetchone()[0]
    else:
        latest = for_date
    if latest is None:
        return None, [], []

    sql_per = """
    WITH snap AS (
      SELECT account, tradingsymbol,
             SUM(quantity)::BIGINT AS quantity,
             SUM(quantity * average_price) / NULLIF(SUM(quantity), 0) AS avg_cost,
             MAX(last_price) AS last_price
      FROM holdings_all
      WHERE as_of_date = ? AND tradingsymbol <> 'LIQUIDCASE'
      GROUP BY account, tradingsymbol
    )
    SELECT * FROM snap ORDER BY account, tradingsymbol
    """
    c1 = con.execute(sql_per, [latest])
    per_rows = [dict(zip([d[0] for d in c1.description], r)) for r in c1.fetchall()]

    sql_con = """
    WITH snap AS (
      SELECT tradingsymbol,
             SUM(quantity)::BIGINT AS quantity,
             SUM(quantity * average_price) / NULLIF(SUM(quantity), 0) AS avg_cost,
             MAX(last_price) AS last_price
      FROM holdings_all
      WHERE as_of_date = ? AND tradingsymbol <> 'LIQUIDCASE'
      GROUP BY tradingsymbol
    )
    SELECT * FROM snap ORDER BY tradingsymbol
    """
    c2 = con.execute(sql_con, [latest])
    cons_rows = [dict(zip([d[0] for d in c2.description], r)) for r in c2.fetchall()]
    return latest, per_rows, cons_rows
