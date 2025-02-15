// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/udf/udf_ir.cpp

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

#include "column/column.h"
#include "udf/udf_internal.h"

namespace starrocks_udf {
bool FunctionContext::is_arg_constant(int i) const {
    if (i < 0 || i >= _impl->_constant_args.size()) {
        return false;
    }
    return _impl->_constant_args[i] != NULL;
}

AnyVal* FunctionContext::get_constant_arg(int i) const {
    if (i < 0 || i >= _impl->_constant_args.size()) {
        return NULL;
    }
    return _impl->_constant_args[i];
}

int FunctionContext::get_num_args() const {
    return _impl->_arg_types.size();
}

int FunctionContext::get_num_constant_args() const {
    return _impl->_constant_args.size();
}

int FunctionContext::get_num_constant_columns() const {
    return _impl->_constant_columns.size();
}

bool FunctionContext::is_constant_column(int i) const {
    if (i < 0 || i >= _impl->_constant_columns.size()) {
        return false;
    }

    return _impl->_constant_columns[i] != nullptr && _impl->_constant_columns[i]->is_constant();
}

starrocks::vectorized::ColumnPtr FunctionContext::get_constant_column(int i) const {
    if (i < 0 || i >= _impl->_constant_columns.size()) {
        return nullptr;
    }

    return _impl->_constant_columns[i];
}

const FunctionContext::TypeDesc& FunctionContext::get_return_type() const {
    return _impl->_return_type;
}

void* FunctionContext::get_function_state(FunctionStateScope scope) const {
    // assert(!_impl->_closed);
    switch (scope) {
    case THREAD_LOCAL:
        return _impl->_thread_local_fn_state;
    case FRAGMENT_LOCAL:
        return _impl->_fragment_local_fn_state;
    default:
        // TODO: signal error somehow
        return nullptr;
    }
}

} // namespace starrocks_udf
