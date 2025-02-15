// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/exprs/percentile_approx_test.cpp

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

#include <gtest/gtest.h>

#include "exprs/aggregate_functions.h"
#include "exprs/percentile_function.h"
#include "testutil/function_utils.h"

namespace starrocks {

class PercentileApproxTest : public testing::Test {
public:
    PercentileApproxTest() {}
};

TEST_F(PercentileApproxTest, testSample) {
    FunctionUtils* futil = new FunctionUtils();
    starrocks_udf::FunctionContext* context = futil->get_fn_ctx();

    DoubleVal doubleQ(0.9);

    StringVal stringVal1;
    DoubleVal int1(1);
    AggregateFunctions::percentile_approx_init(context, &stringVal1);
    AggregateFunctions::percentile_approx_update(context, int1, doubleQ, &stringVal1);
    DoubleVal int2(2);
    AggregateFunctions::percentile_approx_update(context, int2, doubleQ, &stringVal1);

    StringVal s = AggregateFunctions::percentile_approx_serialize(context, stringVal1);

    StringVal stringVal2;
    AggregateFunctions::percentile_approx_init(context, &stringVal2);
    AggregateFunctions::percentile_approx_merge(context, s, &stringVal2);
    DoubleVal v = AggregateFunctions::percentile_approx_finalize(context, stringVal2);
    ASSERT_EQ(v.val, 2);
    delete futil;
}

TEST_F(PercentileApproxTest, testNoMerge) {
    FunctionUtils* futil = new FunctionUtils();
    starrocks_udf::FunctionContext* context = futil->get_fn_ctx();

    DoubleVal doubleQ(0.9);

    StringVal stringVal1;
    DoubleVal val(1);
    AggregateFunctions::percentile_approx_init(context, &stringVal1);
    AggregateFunctions::percentile_approx_update(context, val, doubleQ, &stringVal1);
    DoubleVal val2(2);
    AggregateFunctions::percentile_approx_update(context, val2, doubleQ, &stringVal1);

    DoubleVal v = AggregateFunctions::percentile_approx_finalize(context, stringVal1);
    ASSERT_EQ(v.val, 2);
    delete futil;
}

TEST_F(PercentileApproxTest, testSerialize) {
    FunctionUtils* futil = new FunctionUtils();
    starrocks_udf::FunctionContext* context = futil->get_fn_ctx();

    DoubleVal doubleQ(0.999);
    StringVal stringVal;
    AggregateFunctions::percentile_approx_init(context, &stringVal);

    for (int i = 1; i <= 100000; i++) {
        DoubleVal val(i);
        AggregateFunctions::percentile_approx_update(context, val, doubleQ, &stringVal);
    }
    StringVal serialized = AggregateFunctions::percentile_approx_serialize(context, stringVal);

    // mock serialize
    StringVal stringVal2;
    AggregateFunctions::percentile_approx_init(context, &stringVal2);
    AggregateFunctions::percentile_approx_merge(context, serialized, &stringVal2);
    DoubleVal v = AggregateFunctions::percentile_approx_finalize(context, stringVal2);
    ASSERT_EQ((int)v.val, 99900);
    delete futil;
}

TEST_F(PercentileApproxTest, testNullVale) {
    FunctionUtils* futil = new FunctionUtils();
    starrocks_udf::FunctionContext* context = futil->get_fn_ctx();

    DoubleVal doubleQ(0.999);
    StringVal stringVal;
    AggregateFunctions::percentile_approx_init(context, &stringVal);

    for (int i = 1; i <= 100000; i++) {
        if (i % 3 == 0) {
            AggregateFunctions::percentile_approx_update(context, DoubleVal::null(), doubleQ, &stringVal);
        } else {
            AggregateFunctions::percentile_approx_update(context, DoubleVal(i), doubleQ, &stringVal);
        }
    }
    StringVal serialized = AggregateFunctions::percentile_approx_serialize(context, stringVal);

    // mock serialize
    StringVal stringVal2;
    AggregateFunctions::percentile_approx_init(context, &stringVal2);
    AggregateFunctions::percentile_approx_merge(context, serialized, &stringVal2);
    DoubleVal v = AggregateFunctions::percentile_approx_finalize(context, stringVal2);
    ASSERT_EQ((int)v.val, 99900);
    delete futil;
}

TEST_F(PercentileApproxTest, testStoragePercentile) {
    FunctionUtils* futil = new FunctionUtils();
    starrocks_udf::FunctionContext* context = futil->get_fn_ctx();

    DoubleVal doubleQ(0.999);
    StringVal stringVal;
    AggregateFunctions::percentile_approx_init(context, &stringVal);

    for (int i = 1; i <= 10000; i++) {
        if (i % 3 == 0) {
            PercentileFunctions::percentile_approx_update(context, PercentileFunctions::percentile_empty(context),
                                                          doubleQ, &stringVal);
        } else {
            PercentileFunctions::percentile_approx_update(context, PercentileFunctions::percentile_hash(context, i),
                                                          doubleQ, &stringVal);
        }
    }
    StringVal serialized = AggregateFunctions::percentile_approx_serialize(context, stringVal);

    // mock serialize
    StringVal stringVal2;
    AggregateFunctions::percentile_approx_init(context, &stringVal2);
    AggregateFunctions::percentile_approx_merge(context, serialized, &stringVal2);
    DoubleVal v = AggregateFunctions::percentile_approx_finalize(context, stringVal2);
    ASSERT_EQ((int)v.val, 9990);
    delete futil;
}

} // namespace starrocks
