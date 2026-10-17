# Entropy Pilot

**Two-sided market making, inventory-backed grids, and a local browser dashboard for Entropy.**

[简体中文](../../README.md) · English · [繁體中文](README.zh-TW.md) · [日本語](README.ja.md) · [한국어](README.ko.md)

Entropy Pilot runs on your own computer and supports `io:SNDK` and `io:ANTH`. Wallet setup, risk budgets, strategy controls, charts, and logs share one local dashboard. **The dashboard currently uses Chinese. These translations cover documentation, not the interface.**

> Live-start buttons send real orders. Public CLI configurations default to paper mode. Market making, grids, and referral benefits do not guarantee a profit.

> **Inventory-backed grids are experimental and have not been tested in live trading. Only code implementation and code-level tests have been completed; live-trading readiness has not been verified.**

## Features

- **Two-sided market making:** ALO/Maker quotes, inventory adjustments, and position/order limits.
- **Inventory-backed grids (experimental):** long or short inventory; closing orders reduce existing inventory. Matched grid profit and account P&L are reported separately. Implemented and tested at code level only; not tested in live trading.
- **Multiple wallets and markets:** separate processes and persistent order, fill, and recovery records.
- **Local controls:** configure wallets, save fixed budgets, start or safely stop strategies, and inspect charts and logs.
- **Uncertain-order handling:** preserve `UNKNOWN` and reconcile the original order before retrying.
- **Nine-step guide:** highlights controls without saving settings or submitting orders for you.

## Quick start on Windows

Install [Git](https://git-scm.com/downloads) and [Python 3.11+](https://www.python.org/downloads/), including the Windows Python Launcher (`py`). Installation, tests, and browser workflows were verified on **Windows with Python 3.13**. Paper mode needs no private key; live use requires a funded exchange account and a separately authorized API wallet.

You can install Git and Python 3.13 in PowerShell:

```powershell
winget install --id Git.Git -e --source winget
winget install --id Python.Python.3.13 -e --source winget
```

If `winget` is unavailable, use the official Git and Python download links above. After installation, **close and reopen PowerShell**, then check the versions and set up the project:

```powershell
git --version
py -3.13 --version
git clone https://github.com/AICoder-xx/entropy-pilot.git
cd entropy-pilot
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe dashboard.py
```

Open [http://127.0.0.1:8765/](http://127.0.0.1:8765/). Calling the virtual environment's Python directly avoids `Activate.ps1`; no PowerShell execution-policy change is needed. Alternatively, extract the downloaded source, double-click `install.bat`, then `run_dashboard.bat`.

Start the backend with `run_dashboard.bat` or `dashboard.py`, then open the address above. Double-clicking `dashboard/index.html` cannot connect to the backend; this is not a read-only lock. Use the normal startup entry point instead.

Normal mode allows configuration changes. A fresh installation starts stopped, but **an existing directory may resume from persisted control state**. Check its status first. Closing a browser tab does not stop the service or trading processes.

## Your first session in nine steps

1. **Inspect the page and guide.** Open `新手指南` (Beginner guide), then `带我走一遍` (Walk me through). It only explains the page. Check for running/recovering strategies and read-only mode.
2. **Authorize an API wallet.** Use the exchange's official interface to authorize a separate API wallet for your funded account. Do not enter a main-wallet key. Compromised trading permissions can still cause financial loss.
3. **Fill in wallet details.** Under `钱包与配置` (Wallet and configuration), enter a wallet name, the funded main/subaccount address, and the API wallet private key. The funded-account field is **not** the API wallet address.
4. **Save locally.** Click `保存本机配置` (Save local configuration). Reusing a name updates that wallet; blank address/key fields retain existing values. Use a new name for another funded account. Saving does not prove exchange authorization; startup checks again.
5. **Set the next-run budget.** Select SNDK or ANTH in `风险预算` (Risk budget). Enter the market-making side budget, position limit, open-order limit, and session drawdown. `保存固定预算` (Save fixed budget) disables equity-based scaling for that market. Stop all strategies and recovery before saving.
6. **Choose a strategy and verify scope.** Market making starts **all configured markets for all enabled wallets**—currently SNDK and ANTH. The budget selector does not restrict that scope. A grid starts only the selected wallet/market; set its direction, levels, spacing, order size, and anchor separately. Existing inventory/history restricts direction changes. **Grids are experimental, with code implementation and code-level tests only. They have not been tested in live trading; having a start button does not establish live-trading readiness.**
7. **Check before starting live.** Review the funded account, positions, open orders, available collateral, budgets, and direction. Then decide whether to press the explicitly labeled live-start button. A submitted request does not prove that preflight or quoting succeeded.
8. **Read status, fills, and logs.** Next-run settings are separate from current/previous-run statistics. Resolve the stated error first. `UNKNOWN` means an uncertain outcome, not a failed order that can be blindly resubmitted.
9. **Stop and check remaining exposure.** Use `全部安全撤单并停止` (Cancel strategy orders and stop all), wait for confirmation, and check the exchange. **Stopping and cancelling does not close positions.** `flatten_on_halt` governs risk-triggered reduction only. Closing tabs, killing processes, or deleting records does not replace a safe stop.

## Budgets and configuration

Budgets apply **to each enabled wallet in the selected market**, not a pooled total. The side budget is the total for one market-making ladder, not an amount per level. Grid order size is separate; the other three limits also constrain that market's grid. Consider combined exposure across wallets and markets.

Public examples use **$12 per side, $30 maximum net position, $30 open orders, and $3 session drawdown**, per wallet and market. These are inspection-friendly templates, not recommended allocations or guaranteed loss limits. Exchange minimums and rounding may make them unsuitable. Automatic scaling, paid action-credit purchases, and projected unreceived rebates are disabled by default.

| File | Purpose |
|---|---|
| `config.json` | Default CLI and market-making paper configuration; contains no private key |
| `config.grid.json` | Grid paper-trading configuration |
| `config.live.example.json` | Live template read by the dashboard; credentials are blank, so configure a local wallet first |
| `dashboard.config.json` | Local dashboard budgets, written when saving |
| `dashboard.config.example.json` | Small defaults when the local file is absent |
| `wallet.local.json` | Locally saved credentials; see `wallet.example.json` |
| `.env` | Optional single-wallet CLI credentials; read only if `live.env_file` explicitly names it |
| `state/`, `logs/` | Persistent orders, fills, inventory, and runtime records |

Budget saves update next-run settings immediately without reopening the page; they do not start trading or rewrite history. Stop and restart the relevant processes after manually editing other files. Missing both budget files, or a damaged local file, produces an error instead of a hidden large-budget fallback.

Named wallets use their own JSON credentials. For `primary`, process environment variables override an explicit `.env`, which overrides local JSON. Existing environment variables can therefore affect `primary`; the page displays a note. Distinct names such as `wallet1` are recommended.

## Paper, live, and read-only modes

```powershell
# Market data only
.\.venv\Scripts\python.exe main.py --config config.json --check

# Paper trading; no real orders
.\.venv\Scripts\python.exe main.py --config config.json
.\.venv\Scripts\python.exe main.py --config config.grid.json

# Read-only dashboard: no saving, trading controls, or recovery
.\.venv\Scripts\python.exe dashboard.py --port 8767 --read-only
```

Windows also provides `check_market.bat`, `run_market_maker.bat`, and `run_grid.bat`. Paper fills do not reproduce real queue position, full slippage, or profitability. **Normal dashboard start buttons remain live controls even when CLI defaults are paper.**

Live execution support remains Windows. POSIX locking is implemented, but Linux/macOS live trading and long unattended operation have not been accepted.

## Credentials, recovery, and statistics

- Windows GUI saves now encrypt the entire wallet file with current-user DPAPI. Legacy plaintext is readable and is upgraded only on an explicit GUI save; `.env` remains plaintext. Non-Windows writes fail closed, and encrypted files are not portable across users or machines. Never publish credentials or account diagnostics. Signing locks now have Windows/POSIX implementations; only Windows has local acceptance evidence. Do not mix old and new running versions. See [engineering boundaries](../ENGINEERING_QUALITY.md).
- Stop safely and back up your configuration, credentials, and state before updating. Do not import someone else's ledger or delete records to bypass `UNKNOWN`, inventory conflicts, or late fills. Reconcile original exchange orders and positions first.
- Grid pair profit is not account net profit. Realized price P&L minus fees excludes other components such as funding, unrealized results, transfers, and unpaid rewards. A 30-second markout measures post-fill price movement, not final loss. Missing data is not zero.

```powershell
.\.venv\Scripts\python.exe analyze_efficiency.py --state-dir state --format table
.\.venv\Scripts\python.exe audit_local_data.py --output reports/local-data.json
```

## Optional referral link

[Open Entropy with traderx](https://entropy.io/?r=traderx)

The author confirms a **75% benefit calculated on Entropy's share of trading fees**, not 75% of all trading fees. Eligibility, the actual rate, and settlement follow the official account page and current rules. This statement does not change trading fee rates or add unpaid rewards to actual P&L.

The link is optional. The program does not sign referral bindings, replace existing referrals, or add builder fees. Referral and personal-tier benefits follow the official higher-benefit rule rather than stacking.

## License

Licensed under [MIT](../../LICENSE); see [third-party notices](../../THIRD_PARTY_NOTICES.md) for upstream attribution and licenses.
