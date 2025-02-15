// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/broker_scanner.h

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

#pragma once

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/file_scanner.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_pool.h"
#include "util/runtime_profile.h"
#include "util/slice.h"

namespace starrocks {

class Tuple;
class SlotDescriptor;
class Slice;
class TextConverter;
class FileReader;
class LineReader;
class Decompressor;
class RuntimeState;
class ExprContext;
class TupleDescriptor;
class TupleRow;
class RowDescriptor;
class MemTracker;
class RuntimeProfile;
class StreamLoadPipe;

// Broker scanner convert the data read from broker to starrocks's tuple.
class BrokerScanner : public FileScanner {
public:
    BrokerScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                  const std::vector<TBrokerRangeDesc>& ranges, const std::vector<TNetworkAddress>& broker_addresses,
                  ScannerCounter* counter);

    ~BrokerScanner() override;

    // Open this scanner, will initialize information need to
    Status open() override;

    // Get next tuple
    Status get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof) override;

    // Close this scanner
    void close() override;

private:
    Status open_file_reader();
    Status create_decompressor(TFileFormatType::type type);
    Status open_line_reader();
    // Read next buffer from reader
    Status open_next_reader();

    // Split one text line to values
    void split_line(const Slice& line, std::vector<Slice>* values);

    bool check_decimal_input(const Slice& value, int precision, int scale, std::stringstream* error_msg);

    // Convert one row to one tuple
    //  'ptr' and 'len' is csv text line
    //  output is tuple
    bool convert_one_row(const Slice& line, Tuple* tuple, MemPool* tuple_pool);

    //Status init_expr_ctxes();

    Status line_to_src_tuple();
    bool line_to_src_tuple(const Slice& line);

private:
    const std::vector<TBrokerRangeDesc>& _ranges;
    const std::vector<TNetworkAddress>& _broker_addresses;

    std::unique_ptr<TextConverter> _text_converter;

    char _column_separator;
    char _row_delimiter;

    // Reader
    FileReader* _cur_file_reader;
    LineReader* _cur_line_reader;
    Decompressor* _cur_decompressor;
    int _next_range;
    bool _cur_line_reader_eof;

    bool _scanner_eof;

    // When we fetch range doesn't start from 0,
    // we will read to one ahead, and skip the first line
    bool _skip_next_line;

    // used to hold current StreamLoadPipe
    std::shared_ptr<StreamLoadPipe> _stream_load_pipe;
};

} // namespace starrocks
