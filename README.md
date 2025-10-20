# fractals_orderflow_algo

## Purpose
`bot.py` is a locally-run trading research bot that detects support & resistance zones using fractal extrema and confirms trade signals with orderflow (delta) from Binance aggregated trades. It can simulate fills locally or place real futures market orders via ccxt when enabled.

## Key features
- Builds a warmup OHLCV candle history (configurable timeframe / candle count).
- Aggregates live `aggTrade` websocket data from Binance to compute buy/sell volumes and delta per candle.
- Dynamically detects support/resistance zones (fractal-based) and attaches zone positions for deduping.
- Signal detection uses normalized delta (or raw delta) with adaptive thresholds (stddev × multiplier) and confirmation candle rules.
- Simulated or real order placement + per-trade monitoring using intrabar ticks to detect TP/SL.
- Lightweight Flask endpoints for diagnostics and data inspection.

## Endpoints
- `GET /health` — basic health check
- `GET /diag` — internal diagnostics (listener status, recent agg buckets, df tail, recent signals)
- `GET /signals` — list of recent open/closed signals
- `GET /sr` — current SR zones and recent candle tail

## Quick start (Windows)
1. Create & activate a virtualenv:
   - python -m venv venv
   - .\venv\Scripts\Activate.ps1
2. Install deps:
   - pip install -r requirements.txt
3. Configure environment variables (use `.env` or export in shell). Important vars:
   - BINANCE_API_KEY, BINANCE_API_SECRET (leave blank for simulation)
   - EXECUTE_TRADES (False for simulation)
   - SYMBOL, TIMEFRAME, TARGET_CANDLES, PORT
   - GOOGLE_SERVICE_ACCOUNT_PATH / GOOGLE_SERVICE_ACCOUNT_JSON and GOOGLE_SHEET_ID (optional)
4. Run:
   - .\run_bot.ps1
   - or: python bot.py
5. Open diagnostics:
   - http://127.0.0.1:8080/diag

## Tuning / notes
- Detection thresholds are adaptive: `delta_threshold = std(delta) * DELTA_REL_MULTIPLIER`. Increase multiplier → fewer/higher-quality signals. `CONFIRM_MULT` controls required follow-through in the confirmation candle.
- Watchdog thread automatically restarts the websocket listener if it dies.
- Use `USE_LOCAL_LOG=True` to persist signals to `signals_log.csv`/`signals_log.xlsx` instead of Google Sheets.

## Files of interest
- `bot.py` — main bot implementation
- `requirements.txt` — Python dependencies
- `Dockerfile` — containerization instructions
- `run_bot.ps1` — convenience script to load `.env` and run the bot on Windows

## License
See repository license (if any). Use keys and trading live funds with caution.
