#!/usr/bin/env bash
# Сравнение исходной (hw2.transactions_raw) и оптимизированной (hw2.transactions) таблиц:
# размер на диске и скорость запроса из sql/03_top_category_by_state.sql.
#
# Запуск из корня репозитория: ./scripts/benchmark.sh [число_повторов]
set -euo pipefail

RUNS="${1:-20}"
QUERY_FILE="sql/03_top_category_by_state.sql"

ch() {
    docker compose exec -T clickhouse clickhouse-client --user clickhouse --password clickhouse "$@"
}

echo "== Размер таблиц =="
ch --format PrettyCompactMonoBlock -q "
    SELECT
        table,
        sum(rows)                                          AS rows,
        count()                                            AS parts,
        formatReadableSize(sum(data_uncompressed_bytes))   AS uncompressed,
        formatReadableSize(sum(data_compressed_bytes))     AS compressed,
        round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
    FROM system.parts
    WHERE database = 'hw2' AND active AND table IN ('transactions_raw', 'transactions')
    GROUP BY table
    ORDER BY table DESC"

echo
echo "== Запрос: ${QUERY_FILE}, ${RUNS} повторов на вариант =="
RUN_ID="$(date +%s)"

run() {  # run <метка> <таблица> [доп. настройки clickhouse-client]
    local label="$1" table="$2"
    shift 2
    local query
    query="$(sed "s/FROM hw2\.transactions$/FROM hw2.${table}/" "${QUERY_FILE}")"
    for _ in $(seq "${RUNS}"); do
        ch --log_comment "bench_${RUN_ID}:${label}" --use_query_cache 0 --format Null "$@" -q "${query}"
    done
}

run "1. transactions_raw (01_ddl.sql)" transactions_raw
run "2. transactions без проекции" transactions --optimize_use_projections 0
run "3. transactions (02_ddl_optimized.sql)" transactions

ch -q "SYSTEM FLUSH LOGS"
ch --format PrettyCompactMonoBlock -q "
    SELECT
        replaceOne(log_comment, 'bench_${RUN_ID}:', '')    AS variant,
        round(quantileExact(0.5)(query_duration_ms), 1)    AS median_ms,
        any(read_rows)                                     AS read_rows,
        formatReadableSize(any(read_bytes))                AS read_bytes,
        formatReadableSize(max(memory_usage))              AS memory
    FROM system.query_log
    WHERE type = 'QueryFinish' AND startsWith(log_comment, 'bench_${RUN_ID}:')
    GROUP BY log_comment
    ORDER BY variant"
