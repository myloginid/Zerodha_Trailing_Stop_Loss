"""
Email report generation and Gmail sender.

This module is deliberately config-agnostic: callers pass SMTP settings
explicitly so configuration remains centralized in the main application.
"""
import smtplib
from email.message import EmailMessage

from tsl import _query_latest_snapshots


def _fmt_money(x):
    try:
        return f"₹{float(x):,.2f}"
    except Exception:
        return "-"


def _fmt_pct(x):
    try:
        return f"{float(x):.2f}%"
    except Exception:
        return "-"


def generate_daily_html_report(con, signals, for_date: str | None = None):
    """Build the daily HTML report.

    Sections:
    - Account-level holdings per account
    - Across-account consolidated holdings
    - Recommendations (per-account and consolidated) based on signals

    Args:
        con: DuckDB connection with a `holdings_all` view.
        signals: Output from `tsl.compute_trailing_stop_signals`.

    Returns:
        str: Complete HTML document as a string.
    """
    latest, per_rows, cons_rows = _query_latest_snapshots(con, for_date=for_date)
    as_of = latest or "n/a"

    # Derive P&L metrics
    for r in per_rows:
        q = r.get('quantity') or 0
        avg = r.get('avg_cost') or 0.0
        last = r.get('last_price') or 0.0
        r['value'] = q * (last or 0.0)
        r['pnl_value'] = q * ((last or 0.0) - (avg or 0.0))
        r['pnl_pct'] = ((last - avg) / avg * 100.0) if avg else 0.0
    for r in cons_rows:
        q = r.get('quantity') or 0
        avg = r.get('avg_cost') or 0.0
        last = r.get('last_price') or 0.0
        r['value'] = q * (last or 0.0)
        r['pnl_value'] = q * ((last or 0.0) - (avg or 0.0))
        r['pnl_pct'] = ((last - avg) / avg * 100.0) if avg else 0.0

    # Group per account
    accounts = {}
    for r in per_rows:
        accounts.setdefault(r['account'], []).append(r)

    # Sort holdings by current value desc
    for acc in accounts:
        accounts[acc].sort(key=lambda x: x.get('value', 0.0), reverse=True)
    cons_rows.sort(key=lambda x: x.get('value', 0.0), reverse=True)

    style = """
    <style>
      body { font-family: Arial, sans-serif; color: #222; }
      h2 { margin-top: 1.2em; }
      table { border-collapse: collapse; width: 100%; margin: 8px 0 16px; }
      th, td { border: 1px solid #ddd; padding: 6px 8px; font-size: 13px; }
      th { background: #f4f4f4; text-align: left; }
      .muted { color: #666; }
      .pos { color: #0a7; }
      .neg { color: #c33; }
      .section { border: 1px solid #eee; padding: 10px; border-radius: 6px; }
    </style>
    """

    def _fmt_inr0(x):
        """Format integer rupee amounts with Indian digit grouping (e.g., ₹1,54,20,229)."""
        try:
            n = int(round(float(x)))
        except Exception:
            return "-"
        neg = n < 0
        s = str(abs(n))
        if len(s) <= 3:
            grp = s
        else:
            last3 = s[-3:]
            rest = s[:-3]
            parts = []
            while len(rest) > 2:
                parts.insert(0, rest[-2:])
                rest = rest[:-2]
            if rest:
                parts.insert(0, rest)
            grp = ",".join(parts + [last3])
        return ("-" if neg else "") + "₹" + grp

    def holdings_table(rows, include_account=False):
        head = (
            "<tr>"
            + ("<th>Account</th>" if include_account else "")
            + "<th>Symbol</th><th>Qty</th><th>Avg</th><th>Last</th><th>Value</th><th>PnL</th><th>PnL%</th></tr>"
        )
        trs = []
        # Running totals
        tot_qty = 0
        tot_value = 0.0
        tot_pnl = 0.0
        tot_cost = 0.0
        for r in rows:
            pnl_cls = 'pos' if r['pnl_value'] >= 0 else 'neg'
            tds = []
            if include_account:
                tds.append(f"<td>{r.get('account','')}</td>")
            tds.extend([
                f"<td>{r['tradingsymbol']}</td>",
                f"<td>{r['quantity']}</td>",
                f"<td>{_fmt_money(r['avg_cost'])}</td>",
                f"<td>{_fmt_money(r['last_price'])}</td>",
                f"<td>{_fmt_inr0(r['value'])}</td>",
                f"<td class='{pnl_cls}'>{_fmt_inr0(r['pnl_value'])}</td>",
                f"<td class='{pnl_cls}'>{_fmt_pct(r['pnl_pct'])}</td>",
            ])
            trs.append("<tr>" + "".join(tds) + "</tr>")
            # Update totals
            q = int(r.get('quantity') or 0)
            avg = float(r.get('avg_cost') or 0.0)
            last = float(r.get('last_price') or 0.0)
            val = float(r.get('value') or (q * last))
            pnlv = float(r.get('pnl_value') or (q * (last - avg)))
            tot_qty += q
            tot_value += val
            tot_pnl += pnlv
            tot_cost += q * avg
        # Totals row
        tot_pct = (tot_pnl / tot_cost * 100.0) if tot_cost > 0 else 0.0
        total_cells = []
        if include_account:
            total_cells.append("<td></td>")  # Account column empty
        total_cells.extend([
            "<td><b>Total</b></td>",
            f"<td><b>{tot_qty}</b></td>",
            "<td>-</td>",
            "<td>-</td>",
            f"<td><b>{_fmt_inr0(tot_value)}</b></td>",
            f"<td><b>{_fmt_inr0(tot_pnl)}</b></td>",
            f"<td><b>{_fmt_pct(tot_pct)}</b></td>",
        ])
        trs.append("<tr>" + "".join(total_cells) + "</tr>")
        return "<table>" + head + "".join(trs) + "</table>"

    def recs_table(rows, include_account=False):
        head = (
            "<tr>"
            + ("<th>Account</th>" if include_account else "")
            + "<th>Symbol</th><th>Action</th><th>Exit Qty</th><th>Value</th>"
            + "<th>PnL Amt</th><th>PnL%</th><th>Loss%</th><th>Drawdown%</th></tr>"
        )
        trs = []
        # Sort by value then pnl amount desc
        rows_sorted = sorted(rows, key=lambda x: (x.get('value', 0.0), x.get('pnl_amount', 0.0)), reverse=True)
        for r in rows_sorted:
            if r.get('action') == 'hold':
                continue
            tds = []
            if include_account:
                tds.append(f"<td>{r.get('account','')}</td>")
            tds.extend([
                f"<td>{r['tradingsymbol']}</td>",
                f"<td>{r['action']}</td>",
                f"<td>{r['exit_qty']}</td>",
                f"<td>{_fmt_inr0(r.get('value'))}</td>",
                f"<td>{_fmt_inr0(r.get('pnl_amount'))}</td>",
                f"<td>{_fmt_pct(r.get('pnl_pct'))}</td>",
                f"<td>{_fmt_pct(r.get('loss_pct'))}</td>",
                f"<td>{_fmt_pct(r.get('drawdown_pct') or 0)}</td>",
            ])
            trs.append("<tr>" + "".join(tds) + "</tr>")
        body = "".join(trs) or "<tr><td class='muted' colspan='7'>(no actions)</td></tr>"
        return "<table>" + head + body + "</table>"

    html = [f"<html><head>{style}</head><body>", f"<h1>Daily Holdings & Recommendations</h1>", f"<div class='muted'>As of {as_of}</div>"]

    # Funds summary (cash at hand per account)
    try:
        import os as _os
        base_dir = _os.path.dirname(__file__)
        funds_glob = _os.path.join(base_dir, 'data', 'funds_parquet', 'account=*', f'date={as_of}', 'funds.parquet')
        try:
            rows = con.execute(
                "SELECT account, SUM(available_cash) AS cash "
                "FROM read_parquet(?) WHERE segment='equity' "
                "GROUP BY account ORDER BY cash DESC",
                [funds_glob],
            ).fetchall()
        except Exception:
            fpath = funds_glob.replace("'", "''")
            rows = con.execute(
                "SELECT account, SUM(available_cash) AS cash "
                f"FROM read_parquet('{fpath}') WHERE segment='equity' "
                "GROUP BY account ORDER BY cash DESC",
            ).fetchall()
        html.append("<h2>Funds (Cash at hand)</h2>")
        if rows:
            tot = sum(r[1] or 0.0 for r in rows)
            html.append("<table><tr><th>Account</th><th>Cash</th></tr>")
            for acc, cash in rows:
                html.append(f"<tr><td>{acc}</td><td>{_fmt_inr0(cash)}</td></tr>")
            html.append(f"<tr><td><b>Total</b></td><td><b>{_fmt_inr0(tot)}</b></td></tr></table>")
        else:
            html.append("<div class='muted'>(no funds data)</div>")
    except Exception as e:
        html.append(f"<div class='muted'>Funds section unavailable: {e}</div>")


    html.append("<h2>Account-Level Holdings</h2>")
    for acc, rows in accounts.items():
        html.append(f"<div class='section'><h3>{acc}</h3>{holdings_table(rows)}</div>")

    html.append("<h2>Across-Account Holdings</h2>")
    html.append("<div class='section'>" + holdings_table(cons_rows, include_account=False) + "</div>")

    html.append("<h2>Recommendations</h2>")
    html.append("<h3>Per Account</h3>")
    html.append("<div class='section'>" + recs_table(signals.get('per_account', []), include_account=True) + "</div>")
    html.append("<h3>Consolidated</h3>")
    html.append("<div class='section'>" + recs_table(signals.get('consolidated', []), include_account=False) + "</div>")

    html.append("</body></html>")
    return "".join(html)


def send_email_via_gmail(
    subject: str,
    html_body: str,
    to_addrs: list[str],
    *,
    smtp_user: str,
    smtp_pass: str,
    smtp_from: str | None = None,
    smtp_host: str = 'smtp.gmail.com',
    smtp_port: int = 587,
):
    """Send an HTML email via Gmail SMTP (STARTTLS).

    Args:
        subject: Email subject line.
        html_body: Full HTML body.
        to_addrs: List of recipient email addresses.
        smtp_user: SMTP auth username (Gmail address).
        smtp_pass: SMTP auth password (Gmail App Password recommended).
        smtp_from: Optional From address (defaults to smtp_user).
        smtp_host: SMTP server host (default: smtp.gmail.com).
        smtp_port: SMTP server port (default: 587).

    Returns:
        bool: True on success, False on failure.
    """
    if not smtp_user or not smtp_pass:
        print("Email not sent: missing smtp_user or smtp_pass.")
        return False
    if not to_addrs:
        print("Email not sent: empty recipient list.")
        return False
    from_addr = smtp_from or smtp_user

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = ", ".join(to_addrs)
    msg.set_content("This email contains HTML content. If you see this, switch to HTML view.")
    msg.add_alternative(html_body, subtype='html')

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(smtp_user, smtp_pass)
            smtp.send_message(msg)
        print(f"Email sent to: {', '.join(to_addrs)}")
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False
