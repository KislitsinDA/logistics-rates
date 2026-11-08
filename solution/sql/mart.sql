DROP TABLE IF EXISTS mart.route_card;

CREATE TABLE mart.route_card AS
WITH body_one AS (
    SELECT DISTINCT price_id, body_group
    FROM (
        SELECT pbt.price_id, core.body_group(pbt.body_type_id) AS body_group
        FROM price_body_types pbt
        JOIN core.prices c ON c.price_id = pbt.price_id
        UNION ALL
        SELECT c.price_id, core.body_group(c.body_type)
        FROM core.prices c
        WHERE NOT EXISTS (
            SELECT 1 FROM price_body_types pbt WHERE pbt.price_id = c.price_id
        )
    ) g
),
base AS (
    -- одна сделка один раз внутри месяца и группы кузова
    SELECT DISTINCT
        date_trunc('month', pd.price_date)::date AS month,
        c.price_id,
        c.price_rub,
        c.price_rub / c.distance AS price_km,
        c.price_type,
        c.organization_id,
        c.loading_city_gcid,
        c.unloading_city_gcid,
        lc.name AS loading_city,
        uc.name AS unloading_city,
        b.body_group,
        c.weight_category
    FROM core.prices c
    JOIN price_dates pd ON pd.price_id = c.price_id
    JOIN body_one b ON b.price_id = c.price_id
    JOIN core.cities lc ON lc.gc_id = c.loading_city_gcid
    JOIN core.cities uc ON uc.gc_id = c.unloading_city_gcid
    WHERE c.is_fact
      AND NOT c.complex
      AND NOT c.outlier_mark
      AND c.price > 0
      AND c.distance > 0
      AND pd.price_date <= DATE '2026-10-26'
),
agg AS (
    SELECT
        month,
        loading_city_gcid,
        unloading_city_gcid,
        loading_city,
        unloading_city,
        body_group,
        weight_category,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY price_rub) AS p25_trip,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY price_rub) AS median_trip,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY price_rub) AS p75_trip,
        avg(price_rub) AS avg_trip,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY price_km) AS p25_km,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY price_km) AS median_km,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY price_km) AS p75_km,
        count(DISTINCT price_id) AS trips_count,
        count(DISTINCT organization_id) FILTER (WHERE price_type IN (3, 9)) AS carriers_count,
        CASE
            WHEN count(DISTINCT price_id) < 2 OR avg(price_rub) = 0 THEN NULL
            ELSE stddev_samp(price_rub) / avg(price_rub)
        END AS cv_rate
    FROM base
    GROUP BY
        month,
        loading_city_gcid,
        unloading_city_gcid,
        loading_city,
        unloading_city,
        body_group,
        weight_category
)
SELECT
    a.month,
    a.loading_city_gcid,
    a.unloading_city_gcid,
    a.loading_city,
    a.unloading_city,
    a.body_group,
    a.weight_category,
    a.p25_trip,
    a.median_trip,
    a.p75_trip,
    a.avg_trip,
    a.p25_km,
    a.median_km,
    a.p75_km,
    a.trips_count,
    a.carriers_count,
    a.cv_rate,
    CASE
        WHEN prev.median_trip IS NULL OR prev.median_trip = 0 THEN NULL
        ELSE (a.median_trip - prev.median_trip) / prev.median_trip * 100
    END AS mom_pct,
    CASE
        WHEN yoy.median_trip IS NULL OR yoy.median_trip = 0 THEN NULL
        ELSE (a.median_trip - yoy.median_trip) / yoy.median_trip * 100
    END AS yoy_pct,
    CASE
        WHEN a.trips_count >= 30
         AND a.carriers_count >= 5
         AND (DATE '2026-10-26' - a.month) <= 30 THEN 'HIGH'
        WHEN a.trips_count >= 10 THEN 'MEDIUM'
        ELSE 'LOW_DATA'
    END AS reliability_level
FROM agg a
LEFT JOIN agg prev
    ON prev.month = (a.month - INTERVAL '1 month')::date
   AND prev.loading_city_gcid = a.loading_city_gcid
   AND prev.unloading_city_gcid = a.unloading_city_gcid
   AND prev.body_group = a.body_group
   AND prev.weight_category = a.weight_category
LEFT JOIN agg yoy
    ON yoy.month = (a.month - INTERVAL '1 year')::date
   AND yoy.loading_city_gcid = a.loading_city_gcid
   AND yoy.unloading_city_gcid = a.unloading_city_gcid
   AND yoy.body_group = a.body_group
   AND yoy.weight_category = a.weight_category;

CREATE INDEX route_card_route_month_idx
    ON mart.route_card (loading_city_gcid, unloading_city_gcid, body_group, weight_category, month);
