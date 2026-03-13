#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/System/StorageSystemIcebergFiles.h>


namespace DB
{

namespace Setting
{
extern const SettingsSeconds lock_acquire_timeout;
extern const SettingsBool use_iceberg_metadata_files_cache;
}

#if USE_AVRO

using namespace Iceberg;

class SystemIcebergFilesSource : public ISource, private WithContext
{
public:
    SystemIcebergFilesSource(
        SharedHeader header_,
        UInt64 max_block_size_,
        ContextPtr context_)
        : ISource(header_)
        , WithContext(context_)
        , max_block_size(max_block_size_)
    {
        ContextMutablePtr context_copy = Context::createCopy(context_);
        Settings settings_copy = context_copy->getSettingsCopy();
        settings_copy[Setting::use_iceberg_metadata_files_cache] = false;
        context_copy->setSettings(settings_copy);
        local_context = std::move(context_copy);

        access = local_context->getAccess();

        if (access->isGranted(AccessType::SHOW_TABLES))
        {
            databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true});
        }
        current_database = databases.begin();
    }

    String getName() const override { return "SystemIcebergFilesSource"; }

protected:
    Chunk generate() override
    {
        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();
        size_t num_rows = 0;

        auto block_size_exceeded = [&]
        {
            size_t total_bytes = 0;
            for (const auto & col : res_columns)
                total_bytes += col->byteSize();
            return num_rows && max_block_size && total_bytes > max_block_size;
        };

        while (!block_size_exceeded() && advanceToNextFile())
        {
            insertFileRecord(res_columns, current_files[current_file_index]);
            ++current_file_index;
            ++num_rows;
        }

        if (num_rows == 0)
            return {};

        return Chunk(std::move(res_columns), num_rows);
    }

private:
    const UInt64 max_block_size;
    ContextMutablePtr local_context;
    std::shared_ptr<const ContextAccessWrapper> access;

    Databases databases;
    Databases::iterator current_database;
    DatabaseTablesIteratorPtr current_table_iterator;

    /// Files loaded from the current table and position within them.
    IcebergFiles current_files;
    size_t current_file_index = 0;
    String current_db_name;
    String current_table_name;

    /// Advances to the next valid table across databases.
    /// Returns true if a valid table is available, false when all databases are exhausted.
    bool advanceToNextTable()
    {
        while (current_database != databases.end())
        {
            if (!current_table_iterator)
                current_table_iterator = current_database->second->getTablesIterator(local_context, {}, true);

            if (current_table_iterator->isValid())
                return true;

            current_table_iterator.reset();
            ++current_database;
        }
        return false;
    }

    /// Ensures we are positioned at a valid file.
    /// Returns true if a file is available at current_file_index, false when all tables are exhausted.
    bool advanceToNextFile()
    {
        if (current_file_index < current_files.size())
            return true;

        while (advanceToNextTable())
        {
            loadCurrentTableFiles();
            current_table_iterator->next();

            if (!current_files.empty())
                return true;
        }
        return false;
    }

    /// Loads files from the table at the current iterator position.
    void loadCurrentTableFiles()
    {
        current_files.clear();
        current_file_index = 0;
        current_db_name.clear();
        current_table_name.clear();

        if (!access->isGranted(AccessType::SHOW_TABLES, current_table_iterator->databaseName(), current_table_iterator->name()))
            return;

        StoragePtr storage = current_table_iterator->table();

        TableLockHolder lock = storage->tryLockForShare(
            local_context->getCurrentQueryId(), local_context->getSettingsRef()[Setting::lock_acquire_timeout]);
        if (!lock)
            return;

        auto * object_storage_table = dynamic_cast<StorageObjectStorage *>(storage.get());
        if (!object_storage_table)
            return;

        try
        {
            auto * iceberg_metadata = dynamic_cast<IcebergMetadata *>(object_storage_table->getExternalMetadata(local_context));
            if (!iceberg_metadata)
                return;

            current_files = iceberg_metadata->getFiles(local_context);
            current_db_name = current_table_iterator->databaseName();
            current_table_name = current_table_iterator->name();
        }
        catch (...)
        {
            tryLogCurrentException(
                getLogger("SystemIcebergFiles"),
                fmt::format("Ignoring broken table {}", object_storage_table->getStorageID().getFullTableName()));
        }
    }

    /// Inserts a single file record into the result columns.
    void insertFileRecord(MutableColumns & res_columns, const IcebergFileRecord & file_record) const
    {
        size_t column_index = 0;
        res_columns[column_index++]->insert(current_db_name);
        res_columns[column_index++]->insert(current_table_name);
        res_columns[column_index++]->insert(file_record.added_snapshot_id);
        res_columns[column_index++]->insert(static_cast<Int8>(file_record.content_type));
        res_columns[column_index++]->insert(file_record.file_path);
        res_columns[column_index++]->insert(file_record.file_format);
        res_columns[column_index++]->insert(file_record.record_count);
        res_columns[column_index++]->insert(file_record.file_size_in_bytes);
        res_columns[column_index++]->insert(file_record.partition);

        // schema_id: Nullable(Int32)
        if (file_record.schema_id.has_value())
            res_columns[column_index++]->insert(*file_record.schema_id);
        else
            res_columns[column_index++]->insertDefault();

        // sequence_number: Int64
        res_columns[column_index++]->insert(file_record.sequence_number);

        // sort_order_id: Nullable(Int32)
        if (file_record.sort_order_id.has_value())
            res_columns[column_index++]->insert(*file_record.sort_order_id);
        else
            res_columns[column_index++]->insertDefault();

        // null_value_counts: Map(Int32, Nullable(Int64))
        {
            Map map;
            for (const auto & [col_id, count] : file_record.null_value_counts)
            {
                Tuple kv;
                kv.push_back(col_id);
                kv.push_back(count.has_value() ? Field(*count) : Field(Null()));
                map.push_back(std::move(kv));
            }
            res_columns[column_index++]->insert(map);
        }

        // column_sizes: Map(Int32, Nullable(Int64))
        {
            Map map;
            for (const auto & [col_id, size] : file_record.column_sizes)
            {
                Tuple kv;
                kv.push_back(col_id);
                kv.push_back(size.has_value() ? Field(*size) : Field(Null()));
                map.push_back(std::move(kv));
            }
            res_columns[column_index++]->insert(map);
        }

        // value_counts: Map(Int32, Nullable(Int64))
        {
            Map map;
            for (const auto & [col_id, count] : file_record.value_counts)
            {
                Tuple kv;
                kv.push_back(col_id);
                kv.push_back(count.has_value() ? Field(*count) : Field(Null()));
                map.push_back(std::move(kv));
            }
            res_columns[column_index++]->insert(map);
        }

        // equality_ids: Array(Int32)
        {
            Array eq_ids;
            if (file_record.equality_ids.has_value())
            {
                for (const auto & id : *file_record.equality_ids)
                    eq_ids.push_back(id);
            }
            res_columns[column_index++]->insert(eq_ids);
        }
    }
};

class ReadFromSystemIcebergFiles final : public SourceStepWithFilter
{
public:
    ReadFromSystemIcebergFiles(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Block & header,
        UInt64 max_block_size_)
        : SourceStepWithFilter(
            std::make_shared<const Block>(header),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage_limits(query_info.storage_limits)
        , max_block_size(max_block_size_)
    {
    }

    String getName() const override { return "ReadFromSystemIcebergFiles"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        auto source = std::make_shared<SystemIcebergFilesSource>(getOutputHeader(), max_block_size, context);
        source->setStorageLimits(storage_limits);
        processors.emplace_back(source);
        pipeline.init(Pipe(std::move(source)));
    }

private:
    std::shared_ptr<const StorageLimitsList> storage_limits;
    const UInt64 max_block_size;
};

#endif

StorageSystemIcebergFiles::StorageSystemIcebergFiles(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription{
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"added_snapshot_id", std::make_shared<DataTypeInt64>(), "Snapshot that added this file."},
        {"content_type",
         std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{{"DATA", 0}, {"POSITION_DELETE", 1}, {"EQUALITY_DELETE", 2}}),
         "Content type of the file."},
        {"file_path", std::make_shared<DataTypeString>(), "Path to the data file."},
        {"file_format", std::make_shared<DataTypeString>(), "File format (e.g. PARQUET)."},
        {"record_count", std::make_shared<DataTypeInt64>(), "Number of records in the file."},
        {"file_size_in_bytes", std::make_shared<DataTypeInt64>(), "File size in bytes."},
        {"partition", std::make_shared<DataTypeString>(), "Human-readable partition key-value pairs."},
        {"schema_id",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()),
         "Schema ID the file was written with, NULL when unknown."},
        {"sequence_number", std::make_shared<DataTypeInt64>(), "Sequence number when the file was added."},
        {"sort_order_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "Sort order ID."},
        {"null_value_counts",
         std::make_shared<DataTypeMap>(
             std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())),
         "Per-column null value counts."},
        {"column_sizes",
         std::make_shared<DataTypeMap>(
             std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())),
         "Per-column sizes in bytes."},
        {"value_counts",
         std::make_shared<DataTypeMap>(
             std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())),
         "Per-column value counts."},
        {"equality_ids", std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()), "Equality field IDs for equality deletes."},
    });
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemIcebergFiles::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    [[maybe_unused]] SelectQueryInfo & query_info,
    [[maybe_unused]] ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    [[maybe_unused]] const size_t max_block_size,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

#if USE_AVRO
    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(getVirtualsList());
    auto read_step = std::make_unique<ReadFromSystemIcebergFiles>(
        column_names,
        query_info,
        storage_snapshot,
        context,
        header,
        max_block_size);
    query_plan.addStep(std::move(read_step));
#else
    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(getVirtualsList());
    auto source = std::make_shared<NullSource>(std::move(header));
    query_plan.addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::move(source))));
#endif
}
}
