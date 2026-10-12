-- п.2. Исходный (неоптимизированный) DDL: Kafka -> Materialized View -> MergeTree.
-- Типы колонок «как в CSV», без сортировки, партиционирования и кодеков.

CREATE DATABASE IF NOT EXISTS hw2;

-- Таблица-консьюмер: читает JSON-сообщения из топика transactions
CREATE TABLE IF NOT EXISTS hw2.transactions_queue_raw
(
    transaction_time String,
    merch            String,
    cat_id           String,
    amount           Float64,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        Int64,
    lat              Float64,
    lon              Float64,
    population_city  Int64,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions_raw',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- Таблица хранения
CREATE TABLE IF NOT EXISTS hw2.transactions_raw
(
    transaction_time String,
    merch            String,
    cat_id           String,
    amount           Float64,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        Int64,
    lat              Float64,
    lon              Float64,
    population_city  Int64,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           Int64
)
ENGINE = MergeTree
ORDER BY tuple();

-- Перекладывает данные из очереди в таблицу хранения
CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.transactions_raw_mv
TO hw2.transactions_raw
AS SELECT * FROM hw2.transactions_queue_raw;
