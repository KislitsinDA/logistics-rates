import logging
import os
import time
from pathlib import Path

import psycopg2

from etl.pipeline import run as run_etl
from ml.score import run as run_score

log = logging.getLogger("run")
ROOT = Path(__file__).resolve().parent


def connect():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "deals_bigdata"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )


def wait_db():
    last = None
    # генератор 300k на слабой машине может идти больше 10 минут
    for attempt in range(1, 241):
        conn = None
        try:
            conn = connect()
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM prices")
                n = cur.fetchone()[0]
            if not n:
                raise RuntimeError("prices ещё пустая")
            time.sleep(3)
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM prices")
                n2 = cur.fetchone()[0]
            if n2 != n:
                raise RuntimeError("база ещё догружается")
            log.info("база готова, prices=%s", n2)
            return conn
        except Exception as exc:
            last = exc
            if conn is not None:
                conn.close()
            log.info("жду базу, попытка %s: %s", attempt, exc)
            time.sleep(5)
    raise RuntimeError(f"база не поднялась: {last}")


def apply_sql(conn, path):
    text = Path(path).read_text(encoding="utf-8")
    lines = []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    parts = [p.strip() for p in "\n".join(lines).split(";") if p.strip()]
    with conn.cursor() as cur:
        for part in parts:
            cur.execute(part)
    conn.commit()
    log.info("применил %s", path.name)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = wait_db()
    apply_sql(conn, ROOT / "sql" / "dwh.sql")
    run_etl(conn)
    apply_sql(conn, ROOT / "sql" / "mart.sql")
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM mart.route_card")
        log.info("mart.route_card %s", cur.fetchone()[0])
        cur.execute("SELECT max(month) FROM mart.route_card")
        log.info("последний месяц витрины %s", cur.fetchone()[0])
    run_score(conn)
    conn.close()


if __name__ == "__main__":
    main()
