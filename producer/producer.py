"""Загрузка транзакций из CSV-файла в топик Kafka.

Каждая строка CSV отправляется отдельным JSON-сообщением (формат JSONEachRow),
который ClickHouse читает через таблицу на движке Kafka.

Примеры:
    # внутри docker compose (см. README)
    docker compose run --rm producer

    # с хоста
    python producer/producer.py --file data/train.csv.xz --bootstrap-servers localhost:9094
"""

import argparse
import json
import logging
import math
import os
import sys
import time

import pandas as pd
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("producer")

CHUNK_SIZE = 50_000

# Явные типы колонок, чтобы pandas не угадывал их по каждому чанку по-разному
DTYPES = {
    "transaction_time": "string",
    "merch": "string",
    "cat_id": "string",
    "amount": "float64",
    "name_1": "string",
    "name_2": "string",
    "gender": "string",
    "street": "string",
    "one_city": "string",
    "us_state": "string",
    "post_code": "Int64",
    "lat": "float64",
    "lon": "float64",
    "population_city": "Int64",
    "jobs": "string",
    "merchant_lat": "float64",
    "merchant_lon": "float64",
    "target": "Int64",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Загрузка CSV с транзакциями в Kafka")
    parser.add_argument(
        "--file",
        default=os.getenv("CSV_PATH", "data/train.csv.xz"),
        help="путь к CSV (поддерживаются .csv, .csv.gz, .csv.xz)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions"))
    parser.add_argument(
        "--limit", type=int, default=None, help="отправить только первые N строк"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="отправить данные, даже если топик уже не пустой (будут дубли)",
    )
    return parser.parse_args()


def topic_message_count(bootstrap_servers, topic):
    """Количество сообщений в топике (сумма high watermark по партициям)."""
    consumer = Consumer(
        {"bootstrap.servers": bootstrap_servers, "group.id": "producer-offsets-check"}
    )
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        topic_meta = metadata.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None:
            raise KafkaException(f"Топик {topic} не найден: {topic_meta and topic_meta.error}")
        total = 0
        for partition in topic_meta.partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10
            )
            total += high - low
        return total
    finally:
        consumer.close()


def clean(value):
    """NaN/NA -> None, numpy-типы -> встроенные python-типы."""
    if value is None or value is pd.NA:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def main():
    args = parse_args()

    if not os.path.exists(args.file):
        logger.error("Файл %s не найден", args.file)
        sys.exit(1)

    existing = topic_message_count(args.bootstrap_servers, args.topic)
    if existing and not args.force:
        logger.warning(
            "В топике %s уже %d сообщений — загрузка пропущена, чтобы не создавать дубли. "
            "Для повторной отправки используйте --force.",
            args.topic,
            existing,
        )
        return

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "linger.ms": 50,
            "batch.num.messages": 10_000,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 500_000,
            "acks": "all",
        }
    )

    failed = 0

    def on_delivery(err, _msg):
        nonlocal failed
        if err is not None:
            failed += 1
            if failed <= 10:
                logger.error("Ошибка доставки: %s", err)

    logger.info("Отправка %s в топик %s (%s)", args.file, args.topic, args.bootstrap_servers)
    started = time.time()
    sent = 0

    reader = pd.read_csv(args.file, dtype=DTYPES, chunksize=CHUNK_SIZE)
    for chunk in reader:
        if args.limit is not None:
            chunk = chunk.head(args.limit - sent)
        columns = list(chunk.columns)
        for row in chunk.itertuples(index=False, name=None):
            record = {col: clean(val) for col, val in zip(columns, row)}
            payload = json.dumps(record, ensure_ascii=False).encode("utf-8")
            while True:
                try:
                    producer.produce(args.topic, value=payload, on_delivery=on_delivery)
                    break
                except BufferError:
                    # локальная очередь переполнена — ждём отправки части сообщений
                    producer.poll(0.5)
            producer.poll(0)
        sent += len(chunk)
        logger.info("Отправлено %d сообщений", sent)
        if args.limit is not None and sent >= args.limit:
            break

    remaining = producer.flush(60)
    elapsed = time.time() - started
    if remaining or failed:
        logger.error(
            "Не доставлено сообщений: %d (ошибок доставки: %d)", remaining, failed
        )
        sys.exit(1)
    logger.info("Готово: %d сообщений за %.1f с", sent, elapsed)


if __name__ == "__main__":
    main()
