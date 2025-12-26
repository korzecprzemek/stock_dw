# Streaming Stock Data Warehouse (Thesis Project)

Small end-to-end **streaming-style data warehouse** for stock prices using:

- **PostgreSQL** as the warehouse  
- **Dagster** for orchestration & scheduling  
- **Python** for ETL  
- **Streamlit** for the dashboard  

The pipeline periodically loads stock price data into a dimensional model and the dashboard visualizes it.

---
## Launching the project
- ``psql --version``
- ``psql stock_dw``
- ``dagster dev -m etl.definitions``
- ``streamlit run app.py``
## Architecture (High Level) (outdated!!!)

- **Source:** stock market data via `yfinance` (historical, micro-batch “streaming”)  
- **Warehouse:** PostgreSQL database `stock_dw`  
  - `dim_symbol` – stock symbols  
  - `fact_price` – daily OHLCV prices  
- **Orchestration:** Dagster job `stock_prices_job`  
  - scheduled every 5 minutes (micro-batch streaming)  
- **Dashboard:** Streamlit app (`streamlit_app.py`) querying PostgreSQL
---

## Tech Stack

- Python 3.12
- PostgreSQL (via **Postgres.app** on macOS)
- Dagster (`dagster`, `dagster-webserver`)
- Streamlit
- SQLAlchemy + psycopg2
- pandas, yfinance, plotly, rich

---

## 📁 Project Structure (simplified)

```text
stock_etl
├── 001_init_schema.sql
├── app.py
├── documents
│   ├── plan.pdf
│   ├── README.md
│   ├── requirements.txt
│   └── tree.txt
├── etl
│   ├── dims
│   │   ├── __init__.py
│   │   ├── load_dim_date.py
│   │   └── load_dim_symbol.py
│   ├── facts
│   │   ├── __init__.py
│   │   ├── load_fact_dividend.py
│   │   └── load_fact_price.py
│   ├── jobs
│   │   ├── __init__.py
│   │   ├── daily_price_job.py
│   │   └── workspace.yaml
│   ├── sql
│   │   ├── dims
│   │   │   ├── load_dim_date.sql
│   │   │   └── load_dim_symbol.sql
│   │   ├── facts
│   │   │   ├── create_fact_dividend.sql
│   │   │   ├── load_fact_dividend.sql
│   │   │   └── load_fact_price.sql
│   │   └── staging
│   │       ├── create_stg_dividend.sql
│   │       ├── create_stg_price_intraday.sql
│   │       └── create_stg_price.sql
│   ├── staging
│   │   ├── __init__.py
│   │   ├── extract_dividends.py
│   │   ├── extract_prices.py
│   │   ├── load_stg_dividend.py
│   │   └── load_stg_price.py
│   └── utils
│       ├── config.py
│       ├── db.py
│       └── logging.py
├── main.py
├── migrations
└── stream_intraday.py

