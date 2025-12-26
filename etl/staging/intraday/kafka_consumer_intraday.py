from confluent_kafka import Consumer
import pickle

from load_staging import get_pg_connection, load_intraday_staging

TOPIC = "ticks_intraday_v1"


def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "intraday_loader",
        "auto.offset.reset": "latest",
    })

    consumer.subscribe([TOPIC])
    conn = get_pg_connection()

    batch = []
    BATCH_SIZE = 1

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"❌ Consumer error: {msg.error()}")
                continue
            print("⬇️ received message from Kafka")
            row = pickle.loads(msg.value())
            print(row)

            row = pickle.loads(msg.value())
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                load_intraday_staging(conn, batch)
                conn.commit()
                batch.clear()

    finally:
        if batch:
            load_intraday_staging(conn, batch)
            conn.commit()
        consumer.close()
        conn.close()
    


if __name__ == "__main__":
    main()
