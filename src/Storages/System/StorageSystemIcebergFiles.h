#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements a system table that shows data files in Iceberg table snapshots,
 * similar to the Spark `files` metadata table.
 */

/** Implements a table engine for Iceberg tables.  Displays data files in Iceberg table snapshots similar to the Spark files metadata table.
 *
 * db_name String
 * table_name String
 * added_snapshot_id Int64,
 * content Enum8('DATA' = 0, 'POSITION_DELETE' = 1, 'EQUALITY_DELETE' = 2),
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

class StorageSystemIcebergFiles final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemIcebergFiles"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(
        [[maybe_unused]] MutableColumns & res_columns,
        [[maybe_unused]] ContextPtr context,
        const ActionsDAG::Node *,
        std::vector<UInt8>) const override;
};

}
