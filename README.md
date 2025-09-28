# Zerodha Multi-Account Daily Runner (Holdings, Funds, TSL, Email)

This project logs into multiple Zerodha (Kite Connect) accounts once per day, persists holdings and funds snapshots, computes trailing-stop signals, and emails a concise HTML report. It is designed to be resilient (idempotent) and easy to schedule on a laptop or small VM.

## High-Level Flow

```mermaid
flowchart TD
  A[accounts.json + app_config.json + email_config.json] --> B[kiteConnect.py]
  B --> C{Need login?}
  C -- yes --> D[web_login.py<br/>Selenium login + 2FA]
  C -- no --> E
  D --> E[Persist data<br/>(data_pipeline.py)]
  E --> F[[Holdings Parquet]]
  E --> G[[Funds Parquet]]
  F & G --> H[(DuckDB View)]
  H --> I[tsl.py<br/>Compute TSL signals]
  H --> J[email_report.py<br/>Build HTML report]
  I --> J
  J --> K[Send Email]
```

## Key Modules

- [tmp/Algo-Trade/kiteConnect.py](tmp/Algo-Trade/kiteConnect.py)
  - Orchestrator for the daily run (multi-account).
  - Chooses a single target snapshot date (today IST or Friday on weekends).
  - Checks what data is missing per account and fetches only what’s needed.
  - Ensures Parquet is present for the target date, then computes TSL and emails the report.

- [tmp/Algo-Trade/data_pipeline.py](tmp/Algo-Trade/data_pipeline.py)
  - Centralized persistence utilities and date logic:
    - IST date helpers and “last business day” logic.
    - JSONL/Parquet paths for holdings and funds.
    - Existence checks and JSONL → Parquet backfill.
    - Normalize + persist functions for holdings and funds.
    - Token loader and “which accounts need login” planner.

- [tmp/Algo-Trade/web_login.py](tmp/Algo-Trade/web_login.py)
  - Selenium-based login + 2FA (TOTP) + request_token capture.
  - Supports headless mode and fixed `chromedriver_path` (via app_config).
  - Uses small randomized waits and restarts driver per account.

- [tmp/Algo-Trade/tsl.py](tmp/Algo-Trade/tsl.py)
  - Builds a DuckDB view over all holdings Parquet files.
  - Computes trailing-stop signals per account and consolidated across accounts.
  - Excludes cash-like symbols (e.g., LIQUIDCASE) from analytics.
  - Adds value/pnl_amount fields and sorts recommendations.

- [tmp/Algo-Trade/email_report.py](tmp/Algo-Trade/email_report.py)
  - Generates an HTML report for the chosen snapshot date.
  - Sections: Funds (cash at hand), Account-Level Holdings (with totals), Across-Account Holdings (with totals), and Recommendations (per-account and consolidated).
  - Uses Indian-style currency formatting and 2-decimal percentages.
  - Sends mail via Gmail SMTP (config-only; credentials passed in by the main).

## Configuration

- `tmp/Algo-Trade/accounts.json`: per-account API credentials and TOTP secret.
- `tmp/Algo-Trade/app_config.json`: runner settings (chromedriver path, headless toggle, selected accounts, email config path, subject).
- `tmp/Algo-Trade/email_config.json`: SMTP auth and recipients (Gmail App Password recommended).

Snapshots are written under `tmp/Algo-Trade/data/`:
- Holdings: `holdings_jsonl/<account>/<YYYY-MM-DD>.jsonl` and `holdings_parquet/account=<account>/date=<YYYY-MM-DD>/holdings.parquet`
- Funds: `funds_jsonl/<account>/<YYYY-MM-DD>.jsonl` and `funds_parquet/account=<account>/date=<YYYY-MM-DD>/funds.parquet`

## Idempotency

- The code computes a single target date (today or Friday on weekends) and fetches only missing data. If snapshots exist for that date, it skips to TSL + email.
- Safe to run multiple times a day — it won’t duplicate snapshots for the same date.

## One-Click Daily Runner

- Script: [tmp/Algo-Trade/run_daily.sh](tmp/Algo-Trade/run_daily.sh)
  - Runs once per IST day, logs to `logs/daily.log`.
  - Activates the `kiteconnect` virtualenv (mkvirtualenv) if present; falls back to local `.venv`.
  - Checks network before running.

- macOS launchd sample: [tmp/Algo-Trade/com.algotrade.daily.plist.sample](tmp/Algo-Trade/com.algotrade.daily.plist.sample)
  - Triggers at login and on network availability.
  - Uses the state file in `.state/last_run_date` to avoid multiple runs per day.

## Quick Start

1) Install deps in your venv (preferably `kiteconnect`):
```
pip install -r tmp/Algo-Trade/requirements.txt
```

2) Create and edit configs:
```
cp tmp/Algo-Trade/accounts.sample.json tmp/Algo-Trade/accounts.json
cp tmp/Algo-Trade/app_config.sample.json tmp/Algo-Trade/app_config.json
cp tmp/Algo-Trade/email_config.sample.json tmp/Algo-Trade/email_config.json
```

3) Test a manual run:
```
python tmp/Algo-Trade/kiteConnect.py
```

4) Make the daily runner executable and try it:
```
chmod +x tmp/Algo-Trade/run_daily.sh
bash tmp/Algo-Trade/run_daily.sh
```

5) (macOS) Load the launch agent:
```
cp tmp/Algo-Trade/com.algotrade.daily.plist.sample \
  ~/Library/LaunchAgents/com.algotrade.daily.plist
launchctl load ~/Library/LaunchAgents/com.algotrade.daily.plist
launchctl kickstart -k gui/$UID/com.algotrade.daily
```

## Notes

- Use a fixed `chromedriver_path` in `app_config.json` to avoid driver downloads, and keep headless mode enabled.
- For more reliability, run on a small VM with cron/systemd. The code is daily-idempotent, so a second pass after failures is harmless.
- The TSL logic excludes LIQUIDCASE, computes drawdown/loss/trim actions, and sorts by value then PnL amount.

