import logging
from datetime import datetime

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor, Pool
from psycopg2.extras import execute_values

from etl.pipeline import as_py, read_frame

log = logging.getLogger("ml")

# снимок данных, не дата запуска
AS_OF = "2026-10-26"
TRAIN_FROM = "2026-07-26"
TEST_FROM = "2026-10-05"  # граница входит только в test

NUM = [
    "distance",
    "total_weight",
    "total_volume",
    "origin_lat",
    "origin_lon",
    "dest_lat",
    "dest_lon",
    "year",
    "month",
    "dow",
    "doy",
    "sin_doy",
    "cos_doy",
    "sin_month",
    "cos_month",
    "sin_dow",
    "cos_dow",
]
CAT = [
    "origin_city",
    "dest_city",
    "origin_region",
    "dest_region",
    "body_group",
    "weight_category",
    "volume_category",
]

PRED_COLS = [
    "price_id",
    "loading_date",
    "loading_city_gcid",
    "unloading_city_gcid",
    "body_group",
    "actual_price_rub",
    "predicted_price_rub",
    "abs_error_rub",
    "ape_pct",
    "model_name",
    "scored_at",
]


def add_features(df):
    d = pd.to_datetime(df["loading_date"])
    out = df.copy()
    out["year"] = d.dt.year
    out["month"] = d.dt.month
    out["dow"] = d.dt.dayofweek
    out["doy"] = d.dt.dayofyear
    out["sin_doy"] = np.sin(2 * np.pi * out["doy"] / 365.25)
    out["cos_doy"] = np.cos(2 * np.pi * out["doy"] / 365.25)
    out["sin_month"] = np.sin(2 * np.pi * out["month"] / 12)
    out["cos_month"] = np.cos(2 * np.pi * out["month"] / 12)
    out["sin_dow"] = np.sin(2 * np.pi * out["dow"] / 7)
    out["cos_dow"] = np.cos(2 * np.pi * out["dow"] / 7)
    return out


def frame(df):
    out = add_features(df)
    x = out[NUM + CAT].copy()
    for col in CAT:
        x[col] = x[col].fillna("Не указан").astype(str)
    x[NUM] = x[NUM].astype(float)
    return x


def metrics(y, pred):
    y = np.asarray(y, dtype=float)
    pred = np.asarray(pred, dtype=float)
    err = y - pred
    rmse = float(np.sqrt(np.mean(err ** 2)))
    mae = float(np.mean(np.abs(err)))
    mape = float(np.mean(np.abs(err) / y) * 100)
    denom = float(np.sum((y - y.mean()) ** 2))
    r2 = float(1 - np.sum(err ** 2) / denom) if denom else float("nan")
    return r2, mape, rmse, mae


def run(conn):
    df = read_frame(
        conn,
        """
        SELECT
            c.price_id,
            c.loading_date,
            c.loading_city_gcid,
            c.unloading_city_gcid,
            c.distance,
            c.total_weight,
            c.total_volume,
            c.weight_category,
            c.volume_category,
            c.price_rub,
            core.body_group(c.body_type) AS body_group,
            lc.name AS origin_city,
            uc.name AS dest_city,
            lc.region AS origin_region,
            uc.region AS dest_region,
            lc.latitude AS origin_lat,
            lc.longitude AS origin_lon,
            uc.latitude AS dest_lat,
            uc.longitude AS dest_lon
        FROM core.prices c
        JOIN core.cities lc ON lc.gc_id = c.loading_city_gcid
        JOIN core.cities uc ON uc.gc_id = c.unloading_city_gcid
        WHERE NOT c.complex
          AND c.distance > 0
          AND c.price_rub > 0
          AND (
                c.loading_date > DATE '2026-10-26'
                OR (
                    c.is_fact
                    AND c.loading_date >= DATE '2026-07-26'
                    AND c.loading_date <= DATE '2026-10-26'
                    AND NOT c.outlier_mark
                )
          )
        """,
    )
    if df.empty:
        raise RuntimeError("для обучения нет строк")

    ld = pd.to_datetime(df["loading_date"])
    train = df[(ld >= TRAIN_FROM) & (ld < TEST_FROM)].copy()
    test = df[(ld >= TEST_FROM) & (ld <= AS_OF)].copy()
    future = df[ld > AS_OF].copy()
    log.info("train %s, test %s, future %s", len(train), len(test), len(future))
    if train.empty or test.empty:
        raise RuntimeError("пустой train или test, проверь окно ETL")

    model = CatBoostRegressor(
        iterations=300,
        depth=6,
        learning_rate=0.1,
        loss_function="RMSE",
        random_seed=42,
        allow_writing_files=False,
        verbose=50,
    )
    model.fit(Pool(frame(train), train["price_rub"].astype(float), cat_features=CAT))

    test_pred = np.ravel(model.predict(Pool(frame(test), cat_features=CAT)))
    r2, mape, rmse, mae = metrics(test["price_rub"], test_pred)
    log.info("метрики test: R2 %.4f  MAPE %.2f%%  RMSE %.0f  MAE %.0f", r2, mape, rmse, mae)

    scored = pd.concat([test, future], ignore_index=True)
    pred = np.empty(len(scored), dtype=float)
    pred[: len(test)] = test_pred
    if len(future):
        pred[len(test) :] = np.ravel(model.predict(Pool(frame(future), cat_features=CAT)))

    scored["predicted_price_rub"] = pred
    is_future = pd.to_datetime(scored["loading_date"]) > AS_OF
    scored["actual_price_rub"] = scored["price_rub"].where(~is_future, np.nan)
    scored["abs_error_rub"] = (scored["actual_price_rub"] - scored["predicted_price_rub"]).abs()
    scored["ape_pct"] = scored["abs_error_rub"] / scored["actual_price_rub"] * 100
    scored["model_name"] = "catboost"
    scored["scored_at"] = datetime.now().replace(microsecond=0)

    values = [tuple(as_py(v) for v in rec) for rec in scored[PRED_COLS].itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        cur.execute("DELETE FROM mart.ml_price_predictions")
        execute_values(
            cur,
            f"INSERT INTO mart.ml_price_predictions ({', '.join(PRED_COLS)}) VALUES %s",
            values,
            page_size=5000,
        )
    conn.commit()
    log.info("прогнозов записано %s", len(scored))
