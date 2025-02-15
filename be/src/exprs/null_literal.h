// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/null_literal.h

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

#ifndef STARROCKS_BE_SRC_QUERY_EXPRS_NULL_LITERAL_H
#define STARROCKS_BE_SRC_QUERY_EXPRS_NULL_LITERAL_H

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace starrocks {

class TExprNode;

class NullLiteral : public Expr {
public:
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new NullLiteral(*this)); }
    // NullLiteral(PrimitiveType type);
    virtual starrocks_udf::BooleanVal get_boolean_val(ExprContext*, TupleRow*);
    virtual starrocks_udf::TinyIntVal get_tiny_int_val(ExprContext*, TupleRow*);
    virtual starrocks_udf::SmallIntVal get_small_int_val(ExprContext*, TupleRow*);
    virtual starrocks_udf::IntVal get_int_val(ExprContext*, TupleRow*);
    virtual starrocks_udf::BigIntVal get_big_int_val(ExprContext*, TupleRow*);
    virtual starrocks_udf::FloatVal get_float_val(ExprContext*, TupleRow*);
    virtual starrocks_udf::DoubleVal get_double_val(ExprContext*, TupleRow*);
    virtual starrocks_udf::StringVal get_string_val(ExprContext*, TupleRow*);
    virtual starrocks_udf::DateTimeVal get_datetime_val(ExprContext*, TupleRow*);
    virtual starrocks_udf::DecimalVal get_decimal_val(ExprContext*, TupleRow*);
    virtual starrocks_udf::DecimalV2Val get_decimalv2_val(ExprContext*, TupleRow*);

protected:
    friend class Expr;

    NullLiteral(const TExprNode& node);

private:
    static void* return_value(Expr* e, TupleRow* row);
};

} // namespace starrocks

#endif
