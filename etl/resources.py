# etl/resources.py
import psycopg2
from contextlib import contextmanager
from dagster import resource


@resource
def db_resource(init_context):
    """
    Prosty resource DB używany przez wszystkie op'y.
    """
    conn_params = {
        "host": "localhost",
        "port": 5432,
        "dbname": "stock_dw",
        "user": "postgres",
        "password": "postgres",
    }

    @contextmanager
    def get_connection():
        conn = psycopg2.connect(**conn_params)
        try:
            yield conn
        finally:
            conn.close()

    # zwracamy obiekt z metodą get_connection()
    class DbWrapper:
        def get_connection(self):
            return get_connection()

    return DbWrapper()
