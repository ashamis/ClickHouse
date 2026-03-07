#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements a system table that shows data files in Iceberg table snapshots,
 * similar to the Spark `files` metadata table.
 */

class StorageSystemIcebergFiles final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemIcebergFiles"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData([[maybe_unused]] MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
