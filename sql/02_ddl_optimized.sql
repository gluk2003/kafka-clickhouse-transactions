-- п.4. Оптимизированный DDL (изменённая версия 01_ddl.sql).
--
-- Что изменено относительно 01_ddl.sql:
--   * типы: DateTime вместо строки, LowCardinality(String) для повторяющихся строк,
--     Decimal для суммы, Float32/UInt* вместо Float64/Int64 там, где хватает точности;
--   * кодеки сжатия: Delta+ZSTD для времени, ZSTD для остальных колонок;
--   * ORDER BY (us_state, cat_id, transaction_time) — данные лежат отсортированными
--     по штату, GROUP BY us_state выполняется «в порядке ключа» и сжимается лучше;
--   * PARTITION BY toYYYYMM(transaction_time) — помесячные партиции;
--   * агрегирующая проекция top_category_by_state: для каждого куска данных заранее
--     хранится argMax(cat_id, amount) и max(amount) по штатам, поэтому запрос из
--     03_top_category_by_state.sql читает десятки строк вместо сотен тысяч;
--   * очередь читается тремя консьюмерами — по числу партиций топика.

CREATE DATABASE IF NOT EXISTS hw2;

CREATE TABLE IF NOT EXISTS hw2.transactions_queue
(
    transaction_time String,
    merch            String,
    cat_id           String,
    amount           Decimal(12, 2),
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        UInt32,
    lat              Float64,
    lon              Float64,
    population_city  UInt32,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 3,
    kafka_max_block_size = 100000;

CREATE TABLE IF NOT EXISTS hw2.transactions
(
    transaction_time DateTime('UTC')        CODEC(Delta, ZSTD(1)),
    merch            LowCardinality(String) CODEC(ZSTD(1)),
    cat_id           LowCardinality(String) CODEC(ZSTD(1)),
    amount           Decimal(12, 2)         CODEC(ZSTD(1)),
    name_1           LowCardinality(String) CODEC(ZSTD(1)),
    name_2           LowCardinality(String) CODEC(ZSTD(1)),
    gender           LowCardinality(String) CODEC(ZSTD(1)),
    street           LowCardinality(String) CODEC(ZSTD(1)),
    one_city         LowCardinality(String) CODEC(ZSTD(1)),
    us_state         LowCardinality(String) CODEC(ZSTD(1)),
    post_code        UInt32                 CODEC(ZSTD(1)),
    lat              Float32                CODEC(ZSTD(1)),
    lon              Float32                CODEC(ZSTD(1)),
    population_city  UInt32                 CODEC(ZSTD(1)),
    jobs             LowCardinality(String) CODEC(ZSTD(1)),
    merchant_lat     Float64                CODEC(ZSTD(1)),
    merchant_lon     Float64                CODEC(ZSTD(1)),
    target           UInt8                  CODEC(ZSTD(1)),

    PROJECTION top_category_by_state
    (
        SELECT
            us_state,
            argMax(cat_id, amount),
            max(amount)
        GROUP BY us_state
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (us_state, cat_id, transaction_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.transactions_mv
TO hw2.transactions
AS
SELECT
    parseDateTimeBestEffort(transaction_time, 'UTC') AS transaction_time,
    merch,
    cat_id,
    amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    us_state,
    post_code,
    toFloat32(lat) AS lat,
    toFloat32(lon) AS lon,
    population_city,
    jobs,
    merchant_lat,
    merchant_lon,
    target
FROM hw2.transactions_queue;
