import uuid
from sqlalchemy import text
import pandas as pd


def load_stg_dividend(df: pd.DataFrame, engine) -> str:
    """
    Ładuje dywidendy do stg.stg_dividend i zwraca batch_id.
    """

    batch_id = str(uuid.uuid4())

    if df.empty:
        return batch_id

    df = df.copy()
    df["batch_id"] = batch_id
    df["source"] = "yfinance"

    records = df.to_dict(orient="records")

    sql = """
    INSERT INTO stg.stg_dividend (
        symbol, ex_date, dividend, batch_id, source
    )
    VALUES (
        :symbol, :ex_date, :dividend, :batch_id, :source
    );
    """

    with engine.begin() as conn:
        conn.execute(text(sql), records)

    return batch_id
