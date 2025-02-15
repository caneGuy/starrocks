// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/task/engine_storage_migration_task.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef STARROCKS_BE_SRC_OLAP_TASK_ENGINE_STORAGE_MIGRATION_TASK_H
#define STARROCKS_BE_SRC_OLAP_TASK_ENGINE_STORAGE_MIGRATION_TASK_H

#include "gen_cpp/AgentService_types.h"
#include "storage/olap_define.h"
#include "storage/task/engine_task.h"

namespace starrocks {

// base class for storage engine
// add "Engine" as task prefix to prevent duplicate name with agent task
class EngineStorageMigrationTask : public EngineTask {
public:
    OLAPStatus execute() override;

public:
    EngineStorageMigrationTask(TStorageMediumMigrateReq& storage_medium_migrate_req);
    ~EngineStorageMigrationTask() override {}

private:
    OLAPStatus _storage_medium_migrate(TTabletId tablet_id, TSchemaHash schema_hash,
                                       TStorageMedium::type storage_medium);

    void _generate_new_header(DataDir* store, const uint64_t new_shard, const TabletSharedPtr& tablet,
                              const std::vector<RowsetSharedPtr>& consistent_rowsets,
                              TabletMetaSharedPtr new_tablet_meta);

    // TODO: hkp
    // rewrite this function
    OLAPStatus _copy_index_and_data_files(const std::string& header_path, const TabletSharedPtr& ref_tablet,
                                          const std::vector<RowsetSharedPtr>& consistent_rowsets) const;

private:
    const TStorageMediumMigrateReq& _storage_medium_migrate_req;
}; // EngineTask

} // namespace starrocks
#endif //STARROCKS_BE_SRC_OLAP_TASK_ENGINE_STORAGE_MIGRATION_TASK_H
