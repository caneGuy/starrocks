// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/agent/utils.cpp

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

#include "agent/utils.h"

#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstdio>
#include <fstream>
#include <sstream>

#include "common/status.h"

using std::map;
using std::string;
using std::stringstream;
using std::vector;
using apache::thrift::TException;
using apache::thrift::transport::TTransportException;

namespace starrocks {

MasterServerClient::MasterServerClient(const TMasterInfo& master_info, FrontendServiceClientCache* client_cache)
        : _master_info(master_info), _client_cache(client_cache) {}

AgentStatus MasterServerClient::finish_task(const TFinishTaskRequest& request, TMasterResult* result) {
    Status client_status;
    FrontendServiceConnection client(_client_cache, _master_info.network_address, config::thrift_rpc_timeout_ms,
                                     &client_status);

    if (!client_status.ok()) {
        LOG(WARNING) << "Fail to get master client from cache. "
                     << "host=" << _master_info.network_address.hostname
                     << ", port=" << _master_info.network_address.port << ", code=" << client_status.code();
        return STARROCKS_ERROR;
    }

    try {
        try {
            client->finishTask(*result, request);
        } catch (TTransportException& e) {
            LOG(WARNING) << "master client, retry finishTask: " << e.what();
            client_status = client.reopen(config::thrift_rpc_timeout_ms);
            if (!client_status.ok()) {
                LOG(WARNING) << "Fail to get master client from cache. "
                             << "host=" << _master_info.network_address.hostname
                             << ", port=" << _master_info.network_address.port << ", code=" << client_status.code();
                return STARROCKS_ERROR;
            }
            client->finishTask(*result, request);
        }
    } catch (TException& e) {
        client.reopen(config::thrift_rpc_timeout_ms);
        LOG(WARNING) << "Fail to finish_task. "
                     << "host=" << _master_info.network_address.hostname
                     << ", port=" << _master_info.network_address.port << ", error=" << e.what();
        return STARROCKS_ERROR;
    }

    return STARROCKS_SUCCESS;
}

AgentStatus MasterServerClient::report(const TReportRequest& request, TMasterResult* result) {
    Status client_status;
    FrontendServiceConnection client(_client_cache, _master_info.network_address, config::thrift_rpc_timeout_ms,
                                     &client_status);

    if (!client_status.ok()) {
        LOG(WARNING) << "Fail to get master client from cache. "
                     << "host=" << _master_info.network_address.hostname
                     << " port=" << _master_info.network_address.port << " code=" << client_status.code();
        return STARROCKS_ERROR;
    }

    try {
        try {
            client->report(*result, request);
        } catch (TTransportException& e) {
            TTransportException::TTransportExceptionType type = e.getType();
            if (type != TTransportException::TTransportExceptionType::TIMED_OUT) {
                // if not TIMED_OUT, retry
                LOG(WARNING) << "master client, retry finishTask: " << e.what();

                client_status = client.reopen(config::thrift_rpc_timeout_ms);
                if (!client_status.ok()) {
                    LOG(WARNING) << "Fail to get master client from cache. "
                                 << "host=" << _master_info.network_address.hostname
                                 << ", port=" << _master_info.network_address.port << ", code=" << client_status.code();
                    return STARROCKS_ERROR;
                }

                client->report(*result, request);
            } else {
                // TIMED_OUT exception. do not retry
                // actually we don't care what FE returns.
                LOG(WARNING) << "Fail to report to master: " << e.what();
                return STARROCKS_ERROR;
            }
        }
    } catch (TException& e) {
        client.reopen(config::thrift_rpc_timeout_ms);
        LOG(WARNING) << "Fail to report to master. "
                     << "host=" << _master_info.network_address.hostname
                     << ", port=" << _master_info.network_address.port << ", code=" << client_status.code();
        return STARROCKS_ERROR;
    }

    return STARROCKS_SUCCESS;
}

AgentStatus AgentUtils::rsync_from_remote(const string& remote_host, const string& remote_file_path,
                                          const string& local_file_path,
                                          const std::vector<string>& exclude_file_patterns,
                                          uint32_t transport_speed_limit_kbps, uint32_t timeout_second) {
    int ret_code = 0;
    std::stringstream cmd_stream;
    cmd_stream << "rsync -r -q -e \"ssh -o StrictHostKeyChecking=no\"";
    for (auto exclude_file_pattern : exclude_file_patterns) {
        cmd_stream << " --exclude=" << exclude_file_pattern;
    }
    if (transport_speed_limit_kbps != 0) {
        cmd_stream << " --bwlimit=" << transport_speed_limit_kbps;
    }
    if (timeout_second != 0) {
        cmd_stream << " --timeout=" << timeout_second;
    }
    cmd_stream << " " << remote_host << ":" << remote_file_path << " " << local_file_path;
    LOG(INFO) << "rsync cmd: " << cmd_stream.str();

    FILE* fp = NULL;
    fp = popen(cmd_stream.str().c_str(), "r");

    if (fp == NULL) {
        return STARROCKS_ERROR;
    }

    ret_code = pclose(fp);
    if (ret_code != 0) {
        return STARROCKS_ERROR;
    }

    return STARROCKS_SUCCESS;
}

std::string AgentUtils::print_agent_status(AgentStatus status) {
    switch (status) {
    case STARROCKS_SUCCESS:
        return "STARROCKS_SUCCESS";
    case STARROCKS_ERROR:
        return "STARROCKS_ERROR";
    case STARROCKS_TASK_REQUEST_ERROR:
        return "STARROCKS_TASK_REQUEST_ERROR";
    case STARROCKS_FILE_DOWNLOAD_INVALID_PARAM:
        return "STARROCKS_FILE_DOWNLOAD_INVALID_PARAM";
    case STARROCKS_FILE_DOWNLOAD_INSTALL_OPT_FAILED:
        return "STARROCKS_FILE_DOWNLOAD_INSTALL_OPT_FAILED";
    case STARROCKS_FILE_DOWNLOAD_CURL_INIT_FAILED:
        return "STARROCKS_FILE_DOWNLOAD_CURL_INIT_FAILED";
    case STARROCKS_FILE_DOWNLOAD_FAILED:
        return "STARROCKS_FILE_DOWNLOAD_FAILED";
    case STARROCKS_FILE_DOWNLOAD_GET_LENGTH_FAILED:
        return "STARROCKS_FILE_DOWNLOAD_GET_LENGTH_FAILED";
    case STARROCKS_FILE_DOWNLOAD_NOT_EXIST:
        return "STARROCKS_FILE_DOWNLOAD_NOT_EXIST";
    case STARROCKS_FILE_DOWNLOAD_LIST_DIR_FAIL:
        return "STARROCKS_FILE_DOWNLOAD_LIST_DIR_FAIL";
    case STARROCKS_CREATE_TABLE_EXIST:
        return "STARROCKS_CREATE_TABLE_EXIST";
    case STARROCKS_CREATE_TABLE_DIFF_SCHEMA_EXIST:
        return "STARROCKS_CREATE_TABLE_DIFF_SCHEMA_EXIST";
    case STARROCKS_CREATE_TABLE_NOT_EXIST:
        return "STARROCKS_CREATE_TABLE_NOT_EXIST";
    case STARROCKS_DROP_TABLE_NOT_EXIST:
        return "STARROCKS_DROP_TABLE_NOT_EXIST";
    case STARROCKS_PUSH_INVALID_TABLE:
        return "STARROCKS_PUSH_INVALID_TABLE";
    case STARROCKS_PUSH_INVALID_VERSION:
        return "STARROCKS_PUSH_INVALID_VERSION";
    case STARROCKS_PUSH_TIME_OUT:
        return "STARROCKS_PUSH_TIME_OUT";
    case STARROCKS_PUSH_HAD_LOADED:
        return "STARROCKS_PUSH_HAD_LOADED";
    case STARROCKS_TIMEOUT:
        return "STARROCKS_TIMEOUT";
    case STARROCKS_INTERNAL_ERROR:
        return "STARROCKS_INTERNAL_ERROR";
    default:
        return "UNKNOWM";
    }
}

bool AgentUtils::exec_cmd(const string& command, string* errmsg) {
    // The exit status of the command.
    uint32_t rc = 0;

    // Redirect stderr to stdout to get error message.
    string cmd = command + " 2>&1";

    // Execute command.
    FILE* fp = popen(cmd.c_str(), "r");
    if (fp == NULL) {
        std::stringstream err_stream;
        err_stream << "popen failed. " << strerror(errno) << ", with errno: " << errno << ".\n";
        *errmsg = err_stream.str();
        return false;
    }

    // Get command output.
    char result[1024] = {'\0'};
    while (fgets(result, sizeof(result), fp) != NULL) {
        *errmsg += result;
    }

    // Waits for the associated process to terminate and returns.
    rc = pclose(fp);
    if (rc == -1) {
        if (errno == ECHILD) {
            *errmsg += "pclose cannot obtain the child status.\n";
        } else {
            std::stringstream err_stream;
            err_stream << "Close popen failed. " << strerror(errno) << ", with errno: " << errno << "\n";
            *errmsg += err_stream.str();
        }
        return false;
    }

    // Get return code of command.
    int32_t status_child = WEXITSTATUS(rc);
    if (status_child == 0) {
        return true;
    } else {
        return false;
    }
}

bool AgentUtils::write_json_to_file(const map<string, string>& info, const string& path) {
    rapidjson::Document json_info(rapidjson::kObjectType);
    for (auto& it : info) {
        json_info.AddMember(rapidjson::Value(it.first.c_str(), json_info.GetAllocator()).Move(),
                            rapidjson::Value(it.second.c_str(), json_info.GetAllocator()).Move(),
                            json_info.GetAllocator());
    }
    rapidjson::StringBuffer json_info_str;
    rapidjson::Writer<rapidjson::StringBuffer> writer(json_info_str);
    json_info.Accept(writer);
    std::ofstream fp(path);
    if (!fp) {
        return false;
    }
    fp << json_info_str.GetString() << std::endl;
    fp.close();

    return true;
}

} // namespace starrocks
