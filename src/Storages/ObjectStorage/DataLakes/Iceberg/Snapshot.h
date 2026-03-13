#pragma once
#include "config.h"

#if USE_AVRO

#include <map>
#include <DataTypes/DataTypeDateTime64.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

namespace DB::Iceberg
{

struct IcebergDataSnapshot
{
    DB::ManifestFileCacheKeys manifest_list_entries;
    Int64 snapshot_id;
    Int32 schema_id_on_snapshot_commit;
    std::optional<size_t> total_rows;
    std::optional<size_t> total_bytes;
    std::optional<size_t> total_position_delete_rows;

    std::optional<size_t> getTotalRows() const
    {
        if (total_rows.has_value() && total_position_delete_rows.has_value())
            return *total_rows - *total_position_delete_rows;
        return std::nullopt;
    }
};

using IcebergDataSnapshotPtr = std::shared_ptr<IcebergDataSnapshot>;
struct IcebergHistoryRecord
{
    Int64 snapshot_id;
    DB::DateTime64 made_current_at;
    Int64 parent_id;
    bool is_current_ancestor;
    String manifest_list_path;

    Int32 added_files = 0;
    Int32 added_records = 0;
    Int32 added_files_size;
    Int32 num_partitions;
};

using IcebergHistory = std::vector<Iceberg::IcebergHistoryRecord>;

struct IcebergFileRecord
{
    Int64 added_snapshot_id;
    FileContentType content_type;
    String file_path;
    String file_format;
    Int64 record_count;
    Int64 file_size_in_bytes;
    String partition;
    std::optional<Int32> schema_id;
    Int64 sequence_number;
    std::optional<Int32> sort_order_id;
    std::optional<std::vector<Int32>> equality_ids;
    std::map<Int32, std::optional<Int64>> null_value_counts;
    std::map<Int32, std::optional<Int64>> column_sizes;
    std::map<Int32, std::optional<Int64>> value_counts;
};

using IcebergFiles = std::vector<IcebergFileRecord>;
}

#endif
