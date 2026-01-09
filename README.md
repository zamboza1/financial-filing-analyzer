# Financial Filing Analyzer

Automated SEC filing analysis extracting standardized metrics (Gross revenue, EPS, P/E, P/S) by ticker with audit-ready traceability; built React frontend and Python backend enabling repeatable valuation workflows.

## The Project
I built this to automate extracting and comparing financial metrics from public company filings.

It connects to SEC EDGAR, pulls 10-Q and 10-K filings, extracts key numbers, fetches market data, and calculates valuation ratios while showing source text evidence. Historical prices use the filing date for accurate point-in-time analysis.

## How It Works
**Backend:** Python FastAPI handling the data pipeline. Parses SEC EDGAR filings, extracts KPIs (Revenue, Net Income, EPS, EBITDA, margins), fetches stock prices (current or historical as of filing date), and calculates P/E, P/S, EV/EBITDA ratios.

**Frontend:** React + Tailwind CSS (CDN-based for lightweight deployment). Dark-mode interface with comparison tables and source text evidence panels.

## Engineering Standards
**Testing:** Comprehensive pytest suite located in `/tests` covering SEC ingestion, KPI extraction, and edge cases.

**Architecture:** Separated frontend/backend for independent scaling.

**Type Safety:** Full Python type hinting and Pydantic models.

**Caching:** Local file-based caching for SEC API efficiency and offline development.

## Run It Yourself
```bash
./run.sh
```
Then access the app at: http://localhost:3000

Alternatively:
```bash
pip install -e .
uvicorn backend.api:app --reload --port 8001
python3 -m http.server 3000 --directory frontend/public
```

**License:** MIT

www.linkedin.com/in/willis-yorick/
