import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    get_uuid_str,
    execute_spark_query_general,
)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
def test_system_iceberg_files(
    started_cluster_iceberg_with_spark, format_version, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_system_iceberg_files_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark,
            started_cluster_iceberg_with_spark,
            storage_type,
            TABLE_NAME,
            query,
        )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                a INT,
                b STRING
            )
            USING iceberg
            PARTITIONED BY (identity(a))
            OPTIONS('format-version'='{format_version}')
        """
    )

    for i in range(3):
        spark.sql(
            f"""
                INSERT INTO {TABLE_NAME} VALUES
                ({i}, '{i}');
            """
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(
        storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark
    )

    # Verify files exist
    file_count = int(
        instance.query(
            f"SELECT count() FROM system.iceberg_files WHERE database = 'default' AND table = '{TABLE_NAME}'"
        )
    )
    assert file_count == 3, f"Expected 3 data files, got {file_count}"

    # Verify content type is DATA for all files
    distinct_content = instance.query(
        f"SELECT DISTINCT content_type FROM system.iceberg_files WHERE database = 'default' AND table = '{TABLE_NAME}'"
    ).strip()
    assert distinct_content == "DATA", f"Expected content type 'DATA', got '{distinct_content}'"

    # Verify file format is Parquet
    distinct_format = instance.query(
        f"SELECT DISTINCT file_format FROM system.iceberg_files WHERE database = 'default' AND table = '{TABLE_NAME}'"
    ).strip()
    assert distinct_format.upper() == "PARQUET", f"Expected Parquet format, got '{distinct_format}'"

    # Verify record counts match total row count
    total_records = int(
        instance.query(
            f"SELECT sum(record_count) FROM system.iceberg_files WHERE database = 'default' AND table = '{TABLE_NAME}'"
        )
    )
    actual_count = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert (
        total_records == actual_count
    ), f"Record count mismatch: system.iceberg_files says {total_records}, table has {actual_count}"

    # Verify file_size_in_bytes is positive for all files
    min_size = int(
        instance.query(
            f"SELECT min(file_size_in_bytes) FROM system.iceberg_files WHERE database = 'default' AND table = '{TABLE_NAME}'"
        )
    )
    assert min_size > 0, f"Expected positive file sizes, got min {min_size}"

    # Verify partition info is populated (table is partitioned by a)
    empty_partitions = int(
        instance.query(
            f"SELECT count() FROM system.iceberg_files WHERE database = 'default' AND table = '{TABLE_NAME}' AND length(partition) = 0"
        )
    )
    assert (
        empty_partitions == 0
    ), f"Expected all files to have partition info, but {empty_partitions} have empty partitions"

    # Verify sequence_number is positive
    min_seq = int(
        instance.query(
            f"SELECT min(sequence_number) FROM system.iceberg_files WHERE database = 'default' AND table = '{TABLE_NAME}'"
        )
    )
    if format_version == "2":
        assert min_seq > 0, f"Expected positive sequence numbers for v2, got {min_seq}"

    # Verify column_sizes map is populated
    has_column_sizes = int(
        instance.query(
            f"SELECT count() FROM system.iceberg_files WHERE database = 'default' AND table = '{TABLE_NAME}' AND length(mapKeys(column_sizes)) > 0"
        )
    )
    assert has_column_sizes > 0, "Expected at least one file with column_sizes populated"

    # Verify streaming: small max_block_size forces multiple blocks
    num_blocks = int(
        instance.query(
            f"SELECT count(DISTINCT blockNumber()) FROM system.iceberg_files WHERE database = 'default' AND table = '{TABLE_NAME}'",
            settings={"max_block_size": 1},
        )
    )
    assert num_blocks == 3, f"Streaming: expected 3 blocks, got {num_blocks}"

    max_bs = int(
        instance.query(
            f"SELECT max(blockSize()) FROM system.iceberg_files WHERE database = 'default' AND table = '{TABLE_NAME}'",
            settings={"max_block_size": 1},
        )
    )
    assert max_bs <= 1, f"Streaming: expected block size <= 1, got {max_bs}"

    streaming_count = int(
        instance.query(
            f"SELECT count() FROM system.iceberg_files WHERE database = 'default' AND table = '{TABLE_NAME}'",
            settings={"max_block_size": 1},
        )
    )
    assert streaming_count == 3, f"Streaming: expected 3 files, got {streaming_count}"

    streaming_records = int(
        instance.query(
            f"SELECT sum(record_count) FROM system.iceberg_files WHERE database = 'default' AND table = '{TABLE_NAME}'",
            settings={"max_block_size": 1},
        )
    )
    assert (
        streaming_records == actual_count
    ), f"Streaming: record count mismatch: got {streaming_records}, expected {actual_count}"
