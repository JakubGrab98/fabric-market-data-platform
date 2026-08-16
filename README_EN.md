# Investment Data Analytics Platform (Microsoft Fabric)

> A data engineering project: automated ingestion, processing, and modeling of market and macroeconomic data on Microsoft Fabric, powering investment analysis and Power BI reporting.

## Table of Contents
- [Overview](#overview)
- [Goals](#goals)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Sources](#data-sources)
- [Data Model](#data-model-gold-layer)
- [Data Quality](#data-quality)
- [Status & Roadmap](#status--roadmap)
- [Setup / Getting Started](#setup--getting-started)
- [Disclaimer](#disclaimer)

## Overview

This platform aggregates investment data (equity and ETF prices, company fundamentals, FX rates, macroeconomic indicators) from public APIs using a **hybrid architecture** on Microsoft Fabric: data with a naturally periodic cadence (fundamentals, macro, FX) flows through a classic medallion pipeline (Bronze → Silver → Gold), while price quotes — the only data with genuine potential for frequent updates — are handled through an event-driven path via Fabric Real-Time Intelligence. The Gold layer feeds a Power BI semantic model (Direct Lake) for historical and comparative analysis, while the event-driven path feeds a real-time dashboard and threshold-based alerts.

> **Why hybrid, not fully streaming?** Fundamental and macroeconomic data (FMP, NBP, GUS/Eurostat) has an inherently periodic update cadence — a daily/quarterly cycle doesn't justify building an event stream. Streaming is applied where it genuinely makes sense: Finnhub provides a free real-time WebSocket trade feed, making it the only part of the platform that actually "flows" at high frequency.

## Goals

- **Learning** — deepen investment knowledge by building a consistent, self-owned market data foundation instead of manually collecting data from scattered sources.
- **Portfolio** — demonstrate practical data engineering skills: API integration, pipeline orchestration, dimensional modeling, lakehouse architecture, and data quality testing.

## Architecture

Full diagram: [`architecture.mermaid`](./architecture.mermaid)

| Layer | Content | Technology |
|---|---|---|
| **Bronze (batch)** | Raw fundamentals/macro/FX data, 1:1 with source | Fabric Notebook (Python) → Delta in OneLake |
| **Bronze (stream)** | Raw price events | Eventstream → Delta in OneLake |
| **Real-time path** | Live quotes, threshold alerts | Eventstream → Eventhouse (KQL) → Activator |
| **Silver** | Cleaned & merged data (batch + stream) — standardized currencies/dates/units | PySpark in Fabric Notebook |
| **Gold** | Star schema ready for analysis and BI | PySpark → Delta tables |
| **Serving** | Historical reports / live quote view | Power BI (Direct Lake) / Real-Time Dashboard |
| **Orchestration** | Scheduling and monitoring of the batch path | Data Factory Pipeline |

Full split between batch and event-driven sources: see [`architecture.mermaid`](./architecture.mermaid).

## Tech Stack

- **Microsoft Fabric**: OneLake, Lakehouse, Notebooks (PySpark/Python), Data Factory Pipelines, Power BI (Direct Lake)
- **Microsoft Fabric Real-Time Intelligence**: Eventstream (event routing, SQL operator), Eventhouse/KQL Database (querying fresh data), Activator (no-code threshold alerts)
- **Python**: `requests`/`httpx` (API integration), `pandas` (local validation before write)
- **Delta Lake**: table format across all layers
- **Git**: version control for notebook code and pipeline definitions (exported as `.py`/`.ipynb`)

## Data Sources

| Source | Data Scope | Notes |
|---|---|---|
| **Finnhub** | Real-time trades (WebSocket), news, basic fundamentals, SEC filings | Free tier: 60 requests/min, WebSocket for up to 50 tickers — powers the streaming path |
| **Financial Modeling Prep (FMP)** | Financial statements (balance sheet, income statement, cash flow), fundamental metrics | Free tier: 250 requests/day — powers the batch (fundamentals) path |
| **NBP API** | FX rates (Polish National Bank) | Free, no rate limit |
| **GUS / Eurostat** | Macroeconomic data (Poland/EU) | Periodic updates (monthly/quarterly) |
| **Stooq** | Historical price data, including GPW (Warsaw Stock Exchange) | Free, no API key — supplementary data, used for cross-validation |

Every source is documented in a source log (retrieval date, API version, licensing constraints) — see [`docs/source-log.md`](./docs/source-log.md).

## Data Model (Gold layer)

Star schema oriented toward industry peer comparisons. Full column/key/grain
reference: [`docs/data-model.md`](./docs/data-model.md) — that's the source
of truth; physical Gold table names match
[`architecture.mermaid`](./architecture.mermaid):

- `dim_company` — ticker, name, listing currency
- `dim_date` — calendar (day, week, month, quarter, year, GPW trading day flag)
- `fact_prices` — daily OHLCV per company
- `fact_fundamentals` — quarterly/annual fundamental metrics per company (long/EAV format)
- `fact_macro` — macroeconomic indicators per country/period (long/EAV format)

## Data Quality

- Schema validation on entry into the Bronze layer (types, value ranges)
- Deduplication and date-reconciliation rules in the Silver layer
- Cross-layer consistency checks (row counts, date ranges) between Bronze/Silver/Gold —
  `notebooks/quality/reconciliation/`
- Source and retrieval-timestamp log for full reproducibility

## Status & Roadmap

- [x] Phase 1 — Batch ingestion: notebooks pulling fundamentals/macro/FX into Bronze (NBP, Stooq, FMP, GUS BDL done; Eurostat deliberately deferred — see `docs/next-steps.md`)
- [x] Phase 2 — Transformation: cleaning and standardization in Silver (all four sources: fx_rates, prices, fundamentals, macro)
- [x] Phase 3 — Modeling: star schema in Gold (`dim_company`, `dim_date`, `fact_prices`, `fact_fundamentals`, `fact_macro`); first Power BI report still open
- [ ] Phase 4 — Automation: scheduling and monitoring in Data Factory
- [ ] Phase 5 — Streaming path: WS Bridge (Finnhub) → Eventstream → Eventhouse → Activator (alerts)
- [ ] Phase 6 — Data quality: tests, documentation, source log, cross-layer reconciliation (all now exist; schema-drift/null-rate and referential-integrity checks still open — see `docs/next-steps.md`)

## Setup / Getting Started

1. Create a Microsoft Fabric workspace and Lakehouse(s) (`bronze`, `silver`, `gold` as separate schemas/folders or separate Lakehouses).
2. Store API keys (Finnhub, FMP) as Fabric secrets (Key Vault / Variable Library) — never hardcoded in notebooks.
3. Run the ingestion notebook for a given source (parameters: date range, ticker list).
4. Run the Silver → Gold transformation notebook.
5. Refresh the Power BI semantic model (Direct Lake auto-refreshes as OneLake data changes).

## Disclaimer

This project is educational and portfolio-oriented. The data and any derived metrics or rankings **do not constitute investment advice**. Verify all data against the original source before making any investment decision.
