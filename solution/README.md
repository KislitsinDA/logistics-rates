# Решена обязательная часть

## Стек

Postgres 17 стоит потому, что это обязательное хранилище в задании: источник, очищенные сделки и витрины лежат в одной базе, коридор ставок считается SQL-ом.

Airflow не ставил. Для одного пакетного прогона хватает `docker-compose`: он поднимает базу и сам запускает очистку, витрину и скоринг.

CatBoost взял из отчёта по модели. Там категории (город, регион, кузов) он принимает как есть, без отдельного кодирования, и на таких таблицах он был сильнее линейной модели и одного дерева.

## Запуск

Из папки solution:

```bash
cd solution
docker-compose up
```

Поднимается Postgres 17, в него загружаются `schema.sql` и `synthetic.sql`, затем сами считаются очистка, витрина и прогноз.

Повторный `docker-compose up` прогоняет пайплайн ещё раз и не копит дубли: сделки окна удаляются и пишутся заново, витрина и прогнозы пересобираются.

Окно ETL — `START_DATE` и `END_DATE` по `prices.loading_date`, обе границы включены. В compose стоит полный диапазон. Узкий пересчёт:

```bash
docker-compose run --rm -e START_DATE=2026-09-01 -e END_DATE=2026-09-30 pipeline
```

Снести базу и собрать заново: `docker-compose down -v`.

## Что куда пишется

`public` — операционная база из задания.

`core.prices` — сделки, которые прошли проверки. Валюта уже в рублях без НДС (`price_rub`). `outlier_mark` не пересчитывается.

`dq_quarantine_prices` — отсев и код причины. Одна цена может получить несколько причин.

`mart.route_card` — коридор ставок.

`mart.ml_price_predictions` — факт и прогноз.

Город в расчётах берётся один на `gc_id`, с минимальным `cities.id`. Кузов сводится к группе: тент, рефрижератор, изотерм, борт, прочие, не указан. Если у цены несколько типов одной группы, в витрине она одна. Если группы разные, цена попадает в каждую. Нет строк в `price_body_types` — берётся `prices.body_type`.

В обучение и в карточку маршрута не входят составные рейсы, не-факты и `outlier_mark = true`.

## Проверки

```bash
docker-compose exec postgres psql -U postgres -d deals_bigdata
```

```sql
SELECT reason, count(*)
FROM dq_quarantine_prices
GROUP BY reason
ORDER BY count(*) DESC;

SELECT max(month) FROM mart.route_card;

SELECT month, body_group, weight_category, trips_count, carriers_count,
       round(median_trip::numeric) AS p50,
       round(mom_pct::numeric, 1) AS mom,
       reliability_level
FROM mart.route_card
WHERE loading_city = 'Казань' AND unloading_city = 'Москва'
ORDER BY month DESC, trips_count DESC
LIMIT 20;

SELECT count(*) AS rows,
       count(actual_price_rub) AS with_fact,
       count(*) FILTER (WHERE actual_price_rub IS NULL) AS future_rows
FROM mart.ml_price_predictions;
```

`max(month)` должен быть `2026-10-01`. У будущих погрузок `actual_price_rub`, `abs_error_rub` и `ape_pct` пустые.

## Модель

CatBoost, `random_seed=42`. Признаки: плечо, вес, объём, города и регионы по отдельности, координаты, группа кузова, категории, год и sin/cos месяца, дня недели и дня года. Группа кузова в прогнозе берётся из `prices.body_type`: у сделки одна ставка, по совместимым кузовам её не размножал.

В признаки не входят цена, цена за км, НДС, маржа и `price_group_stats`. `outlier_mark` тоже не признак, им только чистится таргет.

Разбиение по `loading_date`: train с `2026-07-26` до `2026-10-05` не включая этот день, test с `2026-10-05` по `2026-10-26`. Дальше скорится test и погрузки после `2026-10-26`.

Метрики test печатаются в лог строкой `метрики test:`. На этом прогоне: R2 0.9169, MAPE 16.31%, RMSE 55153 руб., MAE 28563 руб. Train 24226, test 12808, будущих заявок 5820.

## outlier_mark на 2026-10-26

В генераторе ночная разметка аномалий не покрывает сделки, созданные в последний день снимка. У них в источнике `outlier_mark = false`, даже если цена странная. Пайплайн это не исправляет и флаг заново не считает.
