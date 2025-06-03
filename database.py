import pyodbc
import yaml
import os
import sys
from contextlib import contextmanager

def load_config(config_path="config.yaml"):
    if getattr(sys, 'frozen', False):
        base_path = os.path.dirname(sys.executable)
    else:
        base_path = os.path.abspath(os.path.dirname(__file__))
    full_path = os.path.join(base_path, config_path)

    try:
        with open(full_path, "r", encoding="utf-8") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"[ERROR] Configuration file '{config_path}' was not found in {base_path}")
        return None
    except yaml.YAMLError as e:
        print(f"[ERROR] Error parsing the configuration file: {e}")
        return None

@contextmanager
def get_db_connection():
    config = load_config()
    if not config:
        print("[ERROR] Configuration not loaded, cannot connect to DB.")
        yield None
        return

    db_config = config.get('database')
    if not db_config:
        print("[ERROR] 'database' section missing in config.")
        yield None
        return

    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={db_config['server']};"
        f"DATABASE={db_config['name']};"
        f"UID={db_config['username']};"
        f"PWD={db_config['password']}"
    )
    try:
        conn = pyodbc.connect(conn_str, timeout=5)
    except Exception as e:
        print(f"[ERROR] Could not connect to DB: {e}")
        yield None
        return

    try:
        yield conn
    finally:
        conn.close()

def fetch_data(date, batch_size=5000):
    config = load_config()
    if not config:
        print("[ERROR] Configuration not loaded, fetch_data cannot proceed.")
        return

    db_config = config.get('database')
    if not db_config or 'query' not in db_config:
        print("[ERROR] 'query' missing in 'database' config.")
        return

    query_template = db_config['query']
    start_date = date.strftime('%Y%m%d 00:00:00')
    end_date = date.strftime('%Y%m%d 23:59:59')
    query = query_template.format(start_date=start_date, end_date=end_date)

    with get_db_connection() as conn:
        if conn is None:
            return
        cursor = conn.cursor()
        cursor.execute(query)
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield rows, [desc[0] for desc in cursor.description]

def check_connection():
    try:
        with get_db_connection() as conn:
            if conn is None:
                return False
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return True
    except Exception:
        return False
