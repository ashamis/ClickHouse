#include <Storages/System/StorageSystemIcebergFiles.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Access/ContextAccess.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Core/Settings.h>


namespace DB
{

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool use_iceberg_metadata_files_cache;
}

ColumnsDescription StorageSystemIcebergFiles::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"added_snapshot_id", std::make_shared<DataTypeInt64>(), "Snapshot that added this file."},
        {"content", std::make_shared<DataTypeString>(), "Content type: 'data', 'position_deletes', or 'equality_deletes'."},
        {"file_path", std::make_shared<DataTypeString>(), "Path to the data file."},
        {"file_format", std::make_shared<DataTypeString>(), "File format (e.g. PARQUET)."},
        {"record_count", std::make_shared<DataTypeInt64>(), "Number of records in the file."},
        {"file_size_in_bytes", std::make_shared<DataTypeInt64>(), "File size in bytes."},
        {"partition", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), "Partition key-value pairs."},
        {"schema_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "Schema ID the file was written with, NULL when unknown."},
        {"added_sequence_number", std::make_shared<DataTypeInt64>(), "Sequence number when the file was added."},
        {"sort_order_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "Sort order ID."},
        {"equality_ids", std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()), "Equality field IDs for equality deletes."},
        {"null_value_counts", std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt64>()), "Per-column null value counts."},
        {"column_sizes", std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt64>()), "Per-column sizes in bytes."},
        {"value_counts", std::make_shared<DataTypeMap>(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt64>()), "Per-column value counts."},
    };
}

void StorageSystemIcebergFiles::fillData(
    [[maybe_unused]] MutableColumns & res_columns,
    [[maybe_unused]] ContextPtr context,
    const ActionsDAG::Node *,
    std::vector<UInt8>) const
{
#if USE_AVRO
    ContextMutablePtr context_copy = Context::createCopy(context);
    Settings settings_copy = context_copy->getSettingsCopy();
    settings_copy[Setting::use_iceberg_metadata_files_cache] = false;
    context_copy->setSettings(settings_copy);

    const auto access = context_copy->getAccess();

    auto add_files = [&](const DatabaseTablesIteratorPtr & it, StorageObjectStorage * object_storage)
    {
        if (!access->isGranted(AccessType::SHOW_TABLES, it->databaseName(), it->name()))
            return;

        try
        {
            if (IcebergMetadata * iceberg_metadata = dynamic_cast<IcebergMetadata *>(object_storage->getExternalMetadata(context_copy)); iceberg_metadata)
            {
                auto iceberg_files = iceberg_metadata->getFiles(context_copy);

                for (auto & file_record : iceberg_files)
                {
                    size_t column_index = 0;
                    res_columns[column_index++]->insert(it->databaseName());
                    res_columns[column_index++]->insert(it->name());
                    res_columns[column_index++]->insert(file_record.added_snapshot_id);
                    res_columns[column_index++]->insert(file_record.content);
                    res_columns[column_index++]->insert(file_record.file_path);
                    res_columns[column_index++]->insert(file_record.file_format);
                    res_columns[column_index++]->insert(file_record.record_count);
                    res_columns[column_index++]->insert(file_record.file_size_in_bytes);

                    // partition: Map(String, String)
                    {
                        Map partition_map;
                        for (const auto & [key, value] : file_record.partition)
                        {
                            Tuple kv;
                            kv.push_back(key);
                            kv.push_back(value);
                            partition_map.push_back(std::move(kv));
                        }
                        res_columns[column_index++]->insert(partition_map);
                    }

                    // schema_id: Nullable(Int32)
                    if (file_record.schema_id.has_value())
                        res_columns[column_index++]->insert(*file_record.schema_id);
                    else
                        res_columns[column_index++]->insertDefault();

                    res_columns[column_index++]->insert(file_record.added_sequence_number);

                    // sort_order_id: Nullable(Int32)
                    if (file_record.sort_order_id.has_value())
                        res_columns[column_index++]->insert(*file_record.sort_order_id);
                    else
                        res_columns[column_index++]->insertDefault();

                    // equality_ids: Array(Int32)
                    {
                        Array eq_ids;
                        for (const auto & id : file_record.equality_ids)
                            eq_ids.push_back(id);
                        res_columns[column_index++]->insert(eq_ids);
                    }

                    // null_value_counts: Map(Int32, Int64)
                    {
                        Map map;
                        for (const auto & [col_id, count] : file_record.null_value_counts)
                        {
                            if (count.has_value())
                            {
                                Tuple kv;
                                kv.push_back(col_id);
                                kv.push_back(*count);
                                map.push_back(std::move(kv));
                            }
                        }
                        res_columns[column_index++]->insert(map);
                    }

                    // column_sizes: Map(Int32, Int64)
                    {
                        Map map;
                        for (const auto & [col_id, size] : file_record.column_sizes)
                        {
                            if (size.has_value())
                            {
                                Tuple kv;
                                kv.push_back(col_id);
                                kv.push_back(*size);
                                map.push_back(std::move(kv));
                            }
                        }
                        res_columns[column_index++]->insert(map);
                    }

                    // value_counts: Map(Int32, Int64)
                    {
                        Map map;
                        for (const auto & [col_id, count] : file_record.value_counts)
                        {
                            if (count.has_value())
                            {
                                Tuple kv;
                                kv.push_back(col_id);
                                kv.push_back(*count);
                                map.push_back(std::move(kv));
                            }
                        }
                        res_columns[column_index++]->insert(map);
                    }
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("SystemIcebergFiles"), fmt::format("Ignoring broken table {}", object_storage->getStorageID().getFullTableName()));
        }
    };

    const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);

    if (show_tables_granted)
    {
        auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true});
        for (const auto & db : databases)
        {
            for (auto iterator = db.second->getTablesIterator(context_copy, {}, true); iterator->isValid(); iterator->next())
            {
                StoragePtr storage = iterator->table();

                TableLockHolder lock = storage->tryLockForShare(context_copy->getCurrentQueryId(), context_copy->getSettingsRef()[Setting::lock_acquire_timeout]);
                if (!lock)
                    continue;

                if (auto * object_storage_table = dynamic_cast<StorageObjectStorage *>(storage.get()))
                {
                    add_files(iterator, object_storage_table);
                }
            }
        }
    }
#endif
}
}
