# Kafka → ClickHouse: загрузка и анализ транзакций

Транзакции из `train.csv` соревнования [teta-ml-1-2025](https://www.kaggle.com/competitions/teta-ml-1-2025)
загружаются Python-скриптом в топик Kafka, ClickHouse забирает их из топика через
движок `Kafka` + `MATERIALIZED VIEW` в таблицу `MergeTree`. По загруженным данным
считается категория наибольшей транзакции в каждом штате, а хранение таблицы
оптимизируется так, чтобы этот запрос выполнялся быстрее.

## Архитектура

```
                            Kafka, топик transactions (3 партиции)
 ┌──────────────┐  JSON   ┌─────────────────────┐
 │ producer.py  │ ──────► │    transactions     │
 │ train.csv.xz │         └──────────┬──────────┘
 └──────────────┘                    │ две consumer group читают один топик
                  ┌──────────────────┴────────────────────┐
                  ▼                                       ▼
   hw2.transactions_queue_raw (Kafka)        hw2.transactions_queue (Kafka)
                  │ transactions_raw_mv                   │ transactions_mv
                  ▼                                       ▼
   hw2.transactions_raw (MergeTree)          hw2.transactions (MergeTree)
   исходный DDL — sql/01_ddl.sql             оптимизированный DDL — sql/02_ddl_optimized.sql
```

Обе версии таблицы заполняются одновременно из одного топика (у них разные
`kafka_group_name`), поэтому их можно сравнить на одинаковых данных.

| Сервис | Что делает | Порт на хосте |
|---|---|---|
| `zookeeper`, `kafka` | брокер сообщений | 9094 (внешний listener) |
| `kafka-setup` | создаёт топик `transactions` (3 партиции) и завершается | — |
| `kafka-ui` | просмотр топиков и сообщений | [8081](http://localhost:8081) |
| `clickhouse` | ClickHouse 24.8, таблицы создаются при первом старте из `sql/01_ddl.sql` и `sql/02_ddl_optimized.sql` | 8123 (HTTP), 9000 (native) |
| `producer` | загрузка CSV в Kafka ([producer/producer.py](producer/producer.py)); запускается вручную | — |

## Файлы по пунктам задания

| Пункт | Файл |
|---|---|
| 1. Загрузка CSV в топик Kafka | [producer/producer.py](producer/producer.py) |
| 2. DDL таблиц, получающих данные из топика | [sql/01_ddl.sql](sql/01_ddl.sql) |
| 3. Запрос: категория наибольшей транзакции по штату | [sql/03_top_category_by_state.sql](sql/03_top_category_by_state.sql) → [results/top_category_by_state.csv](results/top_category_by_state.csv) |
| 4. Оптимизированный DDL | [sql/02_ddl_optimized.sql](sql/02_ddl_optimized.sql), сравнение — [scripts/benchmark.sh](scripts/benchmark.sh) |

Данные лежат в репозитории в сжатом виде: [data/train.csv.xz](data/train.csv.xz)
(15 МБ, 786 431 транзакция; это `train.csv` из соревнования, сжатый `xz`).

## Быстрый старт

### Требования
- Docker 20.10+ и Docker Compose v2 (`docker compose`)
- Свободные порты 8081, 8123, 9000, 9094

### 1. Поднять контейнеры
```bash
git clone https://github.com/gluk2003/kafka-clickhouse-transactions.git
cd kafka-clickhouse-transactions
docker compose up -d --wait
```
`--wait` дожидается, пока Kafka и ClickHouse станут `healthy`. Проверить состояние:
```bash
docker compose ps -a
```
`zookeeper`, `kafka`, `kafka-ui`, `clickhouse` — `running`, `kafka-setup` — `exited (0)`.

Таблицы в ClickHouse уже созданы:
```bash
docker compose exec clickhouse clickhouse-client --user clickhouse --password clickhouse \
  -q "SHOW TABLES FROM hw2"
```
```
transactions
transactions_mv
transactions_queue
transactions_queue_raw
transactions_raw
transactions_raw_mv
```

### 2. Загрузить CSV в Kafka
```bash
docker compose run --rm --build producer
```
Скрипт читает `data/train.csv.xz` пачками по 50 000 строк и отправляет каждую
строку отдельным JSON-сообщением в топик `transactions` (~15 секунд):
```
... - INFO - Отправлено 786431 сообщений
... - INFO - Готово: 786431 сообщений за 12.7 с
```
Повторный запуск ничего не отправит, если топик уже не пустой (защита от дублей);
отправить ещё раз принудительно — `docker compose run --rm producer --force`.

Параметры скрипта: `--file`, `--bootstrap-servers`, `--topic`, `--limit N`, `--force`.
Скрипт можно запустить и с хоста, без Docker:
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python producer/producer.py --file data/train.csv.xz --bootstrap-servers localhost:9094
```

Сообщения можно посмотреть в Kafka UI: http://localhost:8081 → Topics → `transactions` → Messages.

### 3. Убедиться, что ClickHouse прочитал топик
```bash
docker compose exec clickhouse clickhouse-client --user clickhouse --password clickhouse -q "
  SELECT (SELECT count() FROM hw2.transactions_raw) AS raw,
         (SELECT count() FROM hw2.transactions)     AS optimized"
```
Через 10–20 секунд после загрузки в обеих таблицах должно быть `786431`.
Если меньше — ClickHouse ещё дочитывает топик, повторите команду.

### 4. Выполнить запрос и сохранить результат в CSV
```bash
docker compose exec -T clickhouse clickhouse-client --user clickhouse --password clickhouse \
  --queries-file /sql/03_top_category_by_state.sql --format CSVWithNames \
  > results/top_category_by_state.csv
```
Каталог `sql/` смонтирован в контейнер как `/sql`. Сам запрос:
```sql
SELECT
    us_state,
    argMax(cat_id, amount) AS category,
    max(amount)            AS max_amount
FROM hw2.transactions
GROUP BY us_state
ORDER BY us_state;
```
`argMax(cat_id, amount)` возвращает категорию той строки, у которой в группе максимальная
сумма. Результат — 51 строка (50 штатов + DC), первые строки:
```
"us_state","category","max_amount"
"AK","travel",3178.51
"AL","travel",10525.07
"AR","travel",13536.84
"AZ","shopping_pos",1929.43
```
Полный результат — [results/top_category_by_state.csv](results/top_category_by_state.csv).
На исходной таблице `hw2.transactions_raw` запрос даёт тот же результат.

### 5. Сравнить исходную и оптимизированную таблицы
```bash
./scripts/benchmark.sh        # по умолчанию 20 повторов запроса на вариант
```

### Остановка
```bash
docker compose down        # остановить контейнеры
docker compose down -v     # + удалить данные ClickHouse (таблицы пересоздадутся при следующем старте)
```
Топик Kafka хранится внутри контейнера и удаляется вместе с ним, поэтому после
`docker compose down` нужно снова запустить шаг 2.

## Оптимизация хранения (п.4)

Исходная таблица ([sql/01_ddl.sql](sql/01_ddl.sql)) хранит колонки в типах «как в CSV»
(`String`, `Float64`, `Int64`, время строкой) и без ключа сортировки — `ORDER BY tuple()`.
В [sql/02_ddl_optimized.sql](sql/02_ddl_optimized.sql) изменено:

| Изменение | Зачем |
|---|---|
| `transaction_time DateTime('UTC') CODEC(Delta, ZSTD)` вместо `String` | 4 байта вместо 16 на строку, время можно сравнивать и партиционировать; разбор строки — в materialized view через `parseDateTimeBestEffort` |
| `LowCardinality(String)` для `merch`, `cat_id`, `us_state`, `one_city`, `jobs`, `name_1`, `name_2`, `street`, `gender` | в колонках от 2 до ~1000 уникальных значений: хранится словарь + номера, `GROUP BY` и сравнения идут по номерам |
| `amount Decimal(12, 2)` | денежная сумма без ошибок округления float |
| `Float32` для `lat`/`lon` клиента, `UInt32`/`UInt8` для `post_code`, `population_city`, `target` | меньше байт на значение без потери точности (у координат клиента 4 знака после запятой) |
| `CODEC(ZSTD(1))` | сжатие сильнее, чем LZ4 по умолчанию |
| `ORDER BY (us_state, cat_id, transaction_time)` | строки одного штата лежат рядом: лучше сжатие, по штату можно фильтровать по первичному индексу, агрегация по штату идёт в порядке ключа |
| `PARTITION BY toYYYYMM(transaction_time)` | помесячные партиции (15 месяцев данных): запросы за период читают только нужные партиции, старые данные удобно удалять целыми партициями |
| `PROJECTION top_category_by_state` | агрегирующая проекция: в каждом куске данных заранее посчитаны `argMax(cat_id, amount)` и `max(amount)` по штатам. ClickHouse сам подставляет её в запрос п.3 (виден в `EXPLAIN` как `ReadFromMergeTree (top_category_by_state)`) |
| `kafka_num_consumers = 3` | по одному консьюмеру на каждую партицию топика |

### Результаты

Вывод `./scripts/benchmark.sh` на 786 431 строке (после `OPTIMIZE TABLE … FINAL`, Apple M-series, Docker/colima):

**Размер на диске**

| Таблица | Несжатый размер | Сжатый размер |
|---|---|---|
| `transactions_raw` (01_ddl.sql) | 136.5 MiB | 58.4 MiB |
| `transactions` (02_ddl_optimized.sql) | 43.2 MiB | **24.0 MiB** (в 2.4 раза меньше) |

**Запрос п.3** (медиана из 20 запусков)

| Вариант | Время | Прочитано строк | Прочитано данных | Память |
|---|---|---|---|---|
| `transactions_raw` | 10 мс | 786 431 | 28.9 MiB | 1.8 MiB |
| `transactions` без проекции (`optimize_use_projections = 0`) | 8 мс | 786 431 | 7.5 MiB | 0.25 MiB |
| `transactions` | **3 мс** | **751** | **83 KiB** | 0.6 MiB |

Типы и сортировка сокращают объём читаемых данных в ~4 раза, а агрегирующая
проекция — ещё в ~90 раз: вместо всех строк читаются заранее посчитанные агрегаты
(51 штат × число кусков). На таком маленьком наборе данных время запроса упирается
в накладные расходы самого запроса, но объём прочитанных данных растёт вместе с
таблицей — на большой таблице разница во времени будет пропорционально больше.

## Структура проекта

```
.
├── docker-compose.yml
├── requirements.txt               # зависимости producer.py
├── producer/
│   ├── producer.py                # п.1: CSV -> Kafka
│   └── Dockerfile
├── sql/
│   ├── 01_ddl.sql                 # п.2: Kafka-таблица, MergeTree, materialized view
│   ├── 02_ddl_optimized.sql       # п.4: оптимизированный DDL
│   └── 03_top_category_by_state.sql  # п.3: запрос
├── results/
│   └── top_category_by_state.csv  # п.3: результат запроса
├── scripts/
│   └── benchmark.sh               # сравнение таблиц по размеру и скорости запроса
└── data/
    └── train.csv.xz               # исходные данные
```
