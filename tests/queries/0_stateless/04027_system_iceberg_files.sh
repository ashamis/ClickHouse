#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SETTINGS="--allow_insert_into_iceberg=1"
ICEBERG_DIR="${CLICKHOUSE_USER_FILES}/test_iceberg_files_${CLICKHOUSE_DATABASE}"
NC_NAME="nc_iceberg_files_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} ${SETTINGS} --query "DROP TABLE IF EXISTS test_iceberg_files;"
    ${CLICKHOUSE_CLIENT} ${SETTINGS} --query "DROP NAMED COLLECTION IF EXISTS ${NC_NAME}"
    rm -rf "${ICEBERG_DIR}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} ${SETTINGS} --query "CREATE NAMED COLLECTION IF NOT EXISTS ${NC_NAME} AS dummy = '1'"

${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    DROP TABLE IF EXISTS test_iceberg_files;
    CREATE TABLE test_iceberg_files
    (
        event_date Date,
        user_id UInt64,
        event_type String
    )
    ENGINE = IcebergLocal(${NC_NAME}, path = '${ICEBERG_DIR}')
    PARTITION BY event_date
    SETTINGS iceberg_format_version = 2;
"

${CLICKHOUSE_CLIENT} ${SETTINGS} --query "INSERT INTO test_iceberg_files VALUES ('2025-01-01', 1, 'click'), ('2025-01-01', 2, 'view');"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "INSERT INTO test_iceberg_files VALUES ('2025-01-02', 3, 'click');"

echo "--- has files ---"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "SELECT count() > 0 FROM system.iceberg_files WHERE database = currentDatabase() AND table = 'test_iceberg_files';"

echo "--- file metadata ---"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    SELECT
        content_type,
        record_count > 0,
        file_size_in_bytes > 0,
        file_format,
        sequence_number > 0
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'test_iceberg_files'
    ORDER BY file_path;
"

echo "--- partition populated ---"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    SELECT
        length(partition) > 0
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'test_iceberg_files'
    ORDER BY file_path;
"

echo "--- column stats populated ---"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    SELECT
        length(mapKeys(column_sizes)) > 0
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'test_iceberg_files'
    ORDER BY file_path
    LIMIT 1;
"

echo "--- record count matches ---"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    SELECT sum(record_count) = (SELECT count() FROM test_iceberg_files)
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'test_iceberg_files' AND content_type = 'DATA';
"

echo "--- content type ---"
${CLICKHOUSE_CLIENT} ${SETTINGS} --query "
    SELECT DISTINCT content_type
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'test_iceberg_files';
"

echo "--- streaming produces multiple blocks ---"
${CLICKHOUSE_CLIENT} ${SETTINGS} --max_block_size=1 --query "
    SELECT count(DISTINCT blockNumber()) > 1
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'test_iceberg_files';
"

echo "--- streaming block size at most 1 ---"
${CLICKHOUSE_CLIENT} ${SETTINGS} --max_block_size=1 --query "
    SELECT max(blockSize()) <= 1
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'test_iceberg_files';
"

echo "--- streaming count still correct ---"
${CLICKHOUSE_CLIENT} ${SETTINGS} --max_block_size=1 --query "
    SELECT count()
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'test_iceberg_files';
"

echo "--- streaming record count still matches ---"
${CLICKHOUSE_CLIENT} ${SETTINGS} --max_block_size=1 --query "
    SELECT sum(record_count) = (SELECT count() FROM test_iceberg_files)
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'test_iceberg_files' AND content_type = 'DATA';
"

# cleanup is handled by the EXIT trap
