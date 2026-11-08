CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

-- одна строка справочника на gc_id: в источнике Москва и Казань задвоены
CREATE OR REPLACE VIEW core.cities AS
SELECT DISTINCT ON (gc_id)
    gc_id,
    name,
    NULLIF(btrim(split_part(full_name_local, ',', 2)), '') AS region,
    latitude,
    longitude
FROM cities
ORDER BY gc_id, id;

CREATE OR REPLACE FUNCTION core.body_group(body_type_id integer)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN body_type_id IN (56, 57, 58, 59, 60, 61) THEN 'Тент'
        WHEN body_type_id IN (4, 46, 47, 48) THEN 'Рефрижератор'
        WHEN body_type_id = 2 THEN 'Изотерм'
        WHEN body_type_id IN (1, 15) THEN 'Борт'
        WHEN body_type_id IS NULL THEN 'Не указан'
        ELSE 'Прочие'
    END
$$;

CREATE TABLE IF NOT EXISTS core.prices (
    price_id              bigint PRIMARY KEY,
    loading_date          date             NOT NULL,
    loading_city_gcid     integer          NOT NULL,
    unloading_city_gcid   integer          NOT NULL,
    distance              double precision NOT NULL,
    total_weight          double precision NOT NULL,
    total_volume          double precision NOT NULL,
    body_type             integer,
    price                 double precision NOT NULL,
    currency_code         varchar(3)       NOT NULL,
    price_rub             double precision NOT NULL,
    price_type            smallint         NOT NULL,
    is_fact               boolean          NOT NULL,
    complex               boolean          NOT NULL,
    outlier_mark          boolean          NOT NULL,
    organization_id       integer,
    weight_category       varchar(4)       NOT NULL,
    volume_category       varchar(4)       NOT NULL
);

CREATE INDEX IF NOT EXISTS core_prices_loading_date_idx
    ON core.prices (loading_date);

CREATE TABLE IF NOT EXISTS dq_quarantine_prices (
    price_id     bigint NOT NULL,
    loading_date date   NOT NULL,
    reason       text   NOT NULL,
    PRIMARY KEY (price_id, reason)
);

CREATE TABLE IF NOT EXISTS mart.ml_price_predictions (
    price_id              bigint NOT NULL PRIMARY KEY,
    loading_date          date   NOT NULL,
    loading_city_gcid     integer NOT NULL,
    unloading_city_gcid   integer NOT NULL,
    body_group            text   NOT NULL,
    actual_price_rub      double precision,
    predicted_price_rub   double precision NOT NULL,
    abs_error_rub         double precision,
    ape_pct               double precision,
    model_name            text   NOT NULL,
    scored_at             timestamp NOT NULL
);
