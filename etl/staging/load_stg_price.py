import uuid
from sqlalchemy import text
import pandas as pd

def load_stg_price(df, engine):
    batch_id = str(uuid.uuid4())

    df = df.copy()
    df["batch_id"] = batch_id
    df["source"] = "yfinance"

    df["date_value"] = pd.to_datetime(df["date_value"]).dt.date

    records = df.to_dict(orient="records")

    sql = """
    INSERT INTO stg.stg_price (
        symbol, date_value, open, high, low, close, adj_close, volume, batch_id, source
    )
    VALUES (
        :symbol, :date_value, :open, :high, :low, :close, :adj_close, :volume, :batch_id, :source
    )
    """

    with engine.begin() as conn:
        conn.execute(text(sql), records)

    return batch_id
