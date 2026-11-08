import logging
import os
from datetime import date

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

log = logging.getLogger("etl")

CORE_COLS = [
    "price_id",
    "loading_date",
    "loading_city_gcid",
    "unloading_city_gcid",
    "distance",
    "total_weight",
    "total_volume",
    "body_type",
    "price",
    "currency_code",
    "price_rub",
    "price_type",
    "is_fact",
    "complex",
    "outlier_mark",
    "organization_id",
    "weight_category",
    "volume_category",
]


def as_py(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def fill_weight_category(weight, category):
    # пороги после округления до 0.1 т, как в генераторе
    w = np.floor(np.asarray(weight, dtype=float) * 10 + 0.5) / 10
    calc = np.select(
        [w >= 30, w >= 10, w >= 5, w >= 2.5, w >= 0.5],
        ["G20T", "FTL", "10T", "5T", "L5T"],
        default="OTH",
    )
    cat = category.astype("object").to_numpy()
    empty = pd.isna(cat) | (cat == "")
    return pd.Series(np.where(empty, calc, cat), index=category.index)


def fill_volume_category(volume, category):
    v = np.asarray(volume, dtype=float)
    calc = np.where((v > 0) & (v <= 120), "FTL", "OTH")
    cat = category.astype("object").to_numpy()
    empty = pd.isna(cat) | (cat == "")
    return pd.Series(np.where(empty, calc, cat), index=category.index)


def read_frame(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def window():
    start = date.fromisoformat(os.environ.get("START_DATE", "2020-01-01"))
    end = date.fromisoformat(os.environ.get("END_DATE", "2027-12-31"))
    if start > end:
        raise ValueError("START_DATE больше END_DATE")
    return start, end


def quarantine_frame(df, city_ids):
    parts = []

    def add(mask, reason):
        if not mask.any():
            return
        part = df.loc[mask, ["id", "loading_date"]].copy()
        part["reason"] = reason
        parts.append(part)

    # ноль и минус — брак для любой валюты; у рублей ещё отсечка источника <= 1000
    add(df["price"] <= 0, "price_le_0")
    add((df["currency_code"] == "643") & (df["price"] <= 1000), "rub_price_le_1000")
    add(df["distance"] <= 0, "distance_le_0")
    add((df["total_weight"] < 0) | (df["total_volume"] < 0), "negative_weight_or_volume")
    known = df["loading_city_gcid"].isin(city_ids) & df["unloading_city_gcid"].isin(city_ids)
    add(~known, "city_not_found")
    # курс строго на nearest_rate_date, без подстановки loading_date
    foreign = df["currency_code"] != "643"
    add(foreign & df["rate"].isna(), "no_rate")

    if not parts:
        return pd.DataFrame(columns=["price_id", "loading_date", "reason"])
    bad = pd.concat(parts, ignore_index=True)
    bad = bad.rename(columns={"id": "price_id"})
    return bad.drop_duplicates(["price_id", "reason"])


def write_window(conn, good, bad, start, end):
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM core.prices
            WHERE price_id IN (
                SELECT id FROM prices WHERE loading_date BETWEEN %s AND %s
            )
            """,
            (start, end),
        )
        cur.execute(
            """
            DELETE FROM dq_quarantine_prices
            WHERE price_id IN (
                SELECT id FROM prices WHERE loading_date BETWEEN %s AND %s
            )
            """,
            (start, end),
        )
        if len(good):
            values = [tuple(as_py(v) for v in rec) for rec in good[CORE_COLS].itertuples(index=False, name=None)]
            execute_values(
                cur,
                f"INSERT INTO core.prices ({', '.join(CORE_COLS)}) VALUES %s",
                values,
                page_size=5000,
            )
        if len(bad):
            values = [
                tuple(as_py(v) for v in rec)
                for rec in bad[["price_id", "loading_date", "reason"]].itertuples(index=False, name=None)
            ]
            execute_values(
                cur,
                "INSERT INTO dq_quarantine_prices (price_id, loading_date, reason) VALUES %s",
                values,
                page_size=5000,
            )
    conn.commit()


def run(conn):
    start, end = window()
    log.info("окно %s .. %s", start, end)

    df = read_frame(
        conn,
        """
        SELECT id, loading_date, loading_city_gcid, unloading_city_gcid,
               distance, total_weight, total_volume, body_type,
               price, currency_code, price_type, is_fact, complex,
               outlier_mark, organization_id, weight_category, volume_category,
               nearest_rate_date
        FROM prices
        WHERE loading_date BETWEEN %s AND %s
        """,
        (start, end),
    )
    log.info("прочитано %s", len(df))
    if df.empty:
        write_window(conn, df, df, start, end)
        return

    rates = read_frame(conn, "SELECT num_code, setting_date, value FROM exchange_rates")
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT gc_id FROM cities")
        city_ids = {row[0] for row in cur.fetchall()}

    df["currency_code"] = df["currency_code"].astype(str).str.strip()
    df["nearest_rate_date"] = pd.to_datetime(df["nearest_rate_date"])
    rates["setting_date"] = pd.to_datetime(rates["setting_date"])
    rates = rates.drop_duplicates(["num_code", "setting_date"])
    df = df.merge(
        rates,
        how="left",
        left_on=["currency_code", "nearest_rate_date"],
        right_on=["num_code", "setting_date"],
    )
    if df["id"].duplicated().any():
        raise RuntimeError("после склейки с курсом появились дубли id")
    df["rate"] = df["value"]
    df.loc[df["currency_code"] == "643", "rate"] = 1.0

    bad = quarantine_frame(df, city_ids)
    bad_ids = set(bad["price_id"]) if len(bad) else set()
    good = df[~df["id"].isin(bad_ids)].copy()
    good["price_rub"] = good["price"] * good["rate"]
    good["weight_category"] = fill_weight_category(good["total_weight"], good["weight_category"])
    good["volume_category"] = fill_volume_category(good["total_volume"], good["volume_category"])
    good = good.rename(columns={"id": "price_id"})

    if len(bad):
        counts = bad["reason"].value_counts().to_dict()
        log.info(
            "карантин записей %s, цен %s, %s",
            len(bad),
            bad["price_id"].nunique(),
            counts,
        )
    else:
        log.info("карантин пуст")
    log.info("в core %s", len(good))

    write_window(conn, good, bad, start, end)
