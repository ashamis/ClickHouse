#pragma once

#include <Storages/IStorage.h>


namespace DB
{

class Context;

/** Implements a system table that shows data files in Iceberg table snapshots.
 *
 * db_name String
 * table_name String
 * added_snapshot_id Int64,
 * content_type Enum8('DATA' = 0, 'POSITION_DELETE' = 1, 'EQUALITY_DELETE' = 2),
 * file_path String,
 * file_format String,
 * record_count Int64,
 * file_size_in_bytes Int64,
 * partition String,
 * schema_id Nullable(Int32),
 * sequence_number Int64,
 * sort_order_id Nullable(Int32),
 * null_value_counts Map(Int32, Nullable(Int64)),
 * column_sizes Map(Int32, Nullable(Int64)),
 * value_counts Map(Int32, Nullable(Int64)),
 * equality_ids Array(Int32),
 */

class StorageSystemIcebergFiles final : public IStorage
{
public:
    explicit StorageSystemIcebergFiles(const StorageID & table_id_);

    std::string getName() const override { return "SystemIcebergFiles"; }

    bool isSystemStorage() const override { return true; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;
};

}
