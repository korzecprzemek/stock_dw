from confluent_kafka import Producer
import pickle
import time
from datetime import datetime, timezone
import random

from fetch_intraday import fetch_intraday
from load_staging import get_pg_connection

TOPIC = "ticks_intraday_v1"


def load_active_symbols(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("select symbol from dim_symbol;")
        return [r[0] for r in cur.fetchall()]


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")


def generate_fake_rows(symbols):
    """
    Generuje testowe 1-min ticki dla podanych symboli.
    DOPASUJ strukturę 'row' do tego, co normalnie zwraca fetch_intraday()
    i czego oczekuje load_intraday_staging().
    """
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    rows = []

    for sym in symbols:
        base = 100 + random.uniform(-5, 5)
        o = base + random.uniform(-0.5, 0.5)
        c = o + random.uniform(-0.5, 0.5)
        h = max(o, c) + random.uniform(0, 0.3)
        l = min(o, c) - random.uniform(0, 0.3)
        v = random.randint(100, 10_000)

        # jeśli normalnie używasz SŁOWNIKÓW:
        rows.append(
            {
                "symbol": sym,
                "ts_utc": now,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
            }
        )

        # jeśli używasz KROTEK, zamień powyższe na:
        # rows.append((sym, now, o, h, l, c, v))

    return rows


def main():
    producer = Producer(
        {
            "bootstrap.servers": "localhost:9092",
            "linger.ms": 100,
            "acks": "all",
        }
    )

    conn = get_pg_connection()
    symbols = load_active_symbols(conn)
    conn.close()


    last_ts_per_symbol = {}

    try:
        while True:
            rows = fetch_intraday(symbols)

            filtered_rows = []
            for symbol, ts, o, h, l, c, v in rows:
                last_ts = last_ts_per_symbol.get(symbol)
                if last_ts == ts:
                    # ten sam bar co poprzednio -> pomijamy
                    continue

                # nowy bar dla symbolu
                last_ts_per_symbol[symbol] = ts
                filtered_rows.append((symbol, ts, o, h, l, c, v))

            if not filtered_rows:
                print("Rynek zamknięty, czekam na nowe ticki...")
                time.sleep(10)
                continue  # wracamy do while True

            for row in filtered_rows:
                print("Yfinance zwraca nowe dane...", row)
                producer.produce(
                    TOPIC,
                    value=pickle.dumps(row),
                    on_delivery=delivery_report,
                )

            producer.flush()
            time.sleep(60)

    finally:
        producer.flush()



if __name__ == "__main__":
    main()
