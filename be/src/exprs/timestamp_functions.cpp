// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/timestamp_functions.cpp

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

#include "exprs/timestamp_functions.h"

#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "runtime/datetime_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.hpp"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/path_builder.h"
#include "util/timezone_utils.h"

namespace starrocks {

void TimestampFunctions::init() {}

// TODO: accept Java data/time format strings:
// http://docs.oracle.com/javase/1.4.2/docs/api/java/text/SimpleDateFormat.html
// Convert them to boost format strings.
bool TimestampFunctions::check_format(const StringVal& format, DateTimeValue& t) {
    // For now the format  must be of the form: yyyy-MM-dd HH:mm:ss
    // where the time part is optional.
    switch (format.len) {
    case 10:
        if (strncmp((const char*)format.ptr, "yyyy-MM-dd", 10) == 0) {
            t.set_type(TIME_DATE);
            return true;
        }

        break;

    case 19:
        if (strncmp((const char*)format.ptr, "yyyy-MM-dd HH:mm:ss", 19) == 0) {
            t.set_type(TIME_DATETIME);
            return true;
        }
        break;

    default:
        break;
    }

    report_bad_format(&format);
    return false;
}

StringVal TimestampFunctions::convert_format(FunctionContext* ctx, const StringVal& format) {
    switch (format.len) {
    case 8:
        if (strncmp((const char*)format.ptr, "yyyyMMdd", 8) == 0) {
            std::string tmp("%Y%m%d");
            return AnyValUtil::from_string_temp(ctx, tmp);
        }
        break;
    case 10:
        if (strncmp((const char*)format.ptr, "yyyy-MM-dd", 10) == 0) {
            std::string tmp("%Y-%m-%d");
            return AnyValUtil::from_string_temp(ctx, tmp);
        }
        break;
    case 19:
        if (strncmp((const char*)format.ptr, "yyyy-MM-dd HH:mm:ss", 19) == 0) {
            std::string tmp("%Y-%m-%d %H:%i:%s");
            return AnyValUtil::from_string_temp(ctx, tmp);
        }
        break;
    default:
        break;
    }
    return format;
}

void TimestampFunctions::report_bad_format(const StringVal* format) {
    std::string format_str((char*)format->ptr, format->len);
    // LOG(WARNING) << "Bad date/time conversion format: " << format_str
    //             << " Format must be: 'yyyy-MM-dd[ HH:mm:ss]'";
}

IntVal TimestampFunctions::year(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.year());
}

IntVal TimestampFunctions::quarter(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal((ts_value.month() - 1) / 3 + 1);
}

IntVal TimestampFunctions::month(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.month());
}
IntVal TimestampFunctions::day_of_week(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal((ts_value.weekday() + 1) % 7 + 1);
}

IntVal TimestampFunctions::day_of_month(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.day());
}

IntVal TimestampFunctions::day_of_year(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.day_of_year());
}

IntVal TimestampFunctions::week_of_year(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.week(mysql_week_mode(3)));
}

IntVal TimestampFunctions::hour(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.hour());
}

IntVal TimestampFunctions::minute(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.minute());
}

IntVal TimestampFunctions::second(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.second());
}

DateTimeVal TimestampFunctions::to_date(FunctionContext* ctx, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue ts_value = DateTimeValue::from_datetime_val(ts_val);
    ts_value.cast_to_date();
    DateTimeVal result;
    ts_value.to_datetime_val(&result);
    return result;
}

DateTimeVal TimestampFunctions::str_to_date(FunctionContext* ctx, const StringVal& str, const StringVal& format) {
    if (str.is_null || format.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue ts_value;
    if (!ts_value.from_date_format_str((const char*)format.ptr, format.len, (const char*)str.ptr, str.len)) {
        return DateTimeVal::null();
    }
    DateTimeVal ts_val;
    ts_value.to_datetime_val(&ts_val);
    return ts_val;
}

StringVal TimestampFunctions::month_name(FunctionContext* ctx, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return StringVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    const char* name = ts_value.month_name();
    if (name == NULL) {
        return StringVal::null();
    }
    return AnyValUtil::from_string_temp(ctx, name);
}

StringVal TimestampFunctions::day_name(FunctionContext* ctx, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return StringVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    const char* name = ts_value.day_name();
    if (name == NULL) {
        return StringVal::null();
    }
    return AnyValUtil::from_string_temp(ctx, name);
}

DateTimeVal TimestampFunctions::years_add(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<YEAR>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::years_sub(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<YEAR>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::months_add(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<MONTH>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::months_sub(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<MONTH>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::weeks_add(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<WEEK>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::weeks_sub(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<WEEK>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::days_add(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<DAY>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::days_sub(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<DAY>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::hours_add(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<HOUR>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::hours_sub(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<HOUR>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::minutes_add(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<MINUTE>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::minutes_sub(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<MINUTE>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::seconds_add(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<SECOND>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::seconds_sub(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<SECOND>(ctx, ts_val, count, false);
}

DateTimeVal TimestampFunctions::micros_add(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<MICROSECOND>(ctx, ts_val, count, true);
}

DateTimeVal TimestampFunctions::micros_sub(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count) {
    return timestamp_time_op<MICROSECOND>(ctx, ts_val, count, false);
}

template <TimeUnit unit>
DateTimeVal TimestampFunctions::timestamp_time_op(FunctionContext* ctx, const DateTimeVal& ts_val, const IntVal& count,
                                                  bool is_add) {
    if (ts_val.is_null || count.is_null) {
        return DateTimeVal::null();
    }
    TimeInterval interval(unit, count.val, !is_add);

    DateTimeValue ts_value = DateTimeValue::from_datetime_val(ts_val);
    if (!ts_value.date_add_interval(interval, unit)) {
        return DateTimeVal::null();
    }
    DateTimeVal new_ts_val;
    ts_value.to_datetime_val(&new_ts_val);

    return new_ts_val;
}

BigIntVal TimestampFunctions::years_diff(FunctionContext* ctx, const DateTimeVal& ts_val1, const DateTimeVal& ts_val2) {
    return timestamp_diff<YEAR>(ctx, ts_val1, ts_val2);
}

BigIntVal TimestampFunctions::months_diff(FunctionContext* ctx, const DateTimeVal& ts_val1,
                                          const DateTimeVal& ts_val2) {
    return timestamp_diff<MONTH>(ctx, ts_val1, ts_val2);
}

BigIntVal TimestampFunctions::weeks_diff(FunctionContext* ctx, const DateTimeVal& ts_val1, const DateTimeVal& ts_val2) {
    return timestamp_diff<WEEK>(ctx, ts_val1, ts_val2);
}

BigIntVal TimestampFunctions::days_diff(FunctionContext* ctx, const DateTimeVal& ts_val1, const DateTimeVal& ts_val2) {
    return timestamp_diff<DAY>(ctx, ts_val1, ts_val2);
}

BigIntVal TimestampFunctions::hours_diff(FunctionContext* ctx, const DateTimeVal& ts_val1, const DateTimeVal& ts_val2) {
    return timestamp_diff<HOUR>(ctx, ts_val1, ts_val2);
}

BigIntVal TimestampFunctions::minutes_diff(FunctionContext* ctx, const DateTimeVal& ts_val1,
                                           const DateTimeVal& ts_val2) {
    return timestamp_diff<MINUTE>(ctx, ts_val1, ts_val2);
}

BigIntVal TimestampFunctions::seconds_diff(FunctionContext* ctx, const DateTimeVal& ts_val1,
                                           const DateTimeVal& ts_val2) {
    return timestamp_diff<SECOND>(ctx, ts_val1, ts_val2);
}

template <TimeUnit unit>
BigIntVal TimestampFunctions::timestamp_diff(FunctionContext* ctx, const DateTimeVal& ts_val2,
                                             const DateTimeVal& ts_val1) {
    if (ts_val1.is_null || ts_val2.is_null) {
        return BigIntVal::null();
    }

    DateTimeValue ts_value1 = DateTimeValue::from_datetime_val(ts_val1);
    DateTimeValue ts_value2 = DateTimeValue::from_datetime_val(ts_val2);

    switch (unit) {
    case YEAR: {
        int year = (ts_value2.year() - ts_value1.year());
        if (year >= 0) {
            year -= (ts_value2.to_int64() % 10000000000 - ts_value1.to_int64() % 10000000000) < 0;
        } else {
            year += (ts_value2.to_int64() % 10000000000 - ts_value1.to_int64() % 10000000000) > 0;
        }
        return year;
    }
    case MONTH: {
        int month = (ts_value2.year() - ts_value1.year()) * 12 + (ts_value2.month() - ts_value1.month());
        if (month >= 0) {
            month -= (ts_value2.to_int64() % 100000000 - ts_value1.to_int64() % 100000000) < 0;
        } else {
            month += (ts_value2.to_int64() % 100000000 - ts_value1.to_int64() % 100000000) > 0;
        }
        return month;
    }
    case WEEK: {
        int day = ts_value2.daynr() - ts_value1.daynr();
        if (day >= 0) {
            day -= ts_value2.time_part_diff(ts_value1) < 0;
        } else {
            day += ts_value2.time_part_diff(ts_value1) > 0;
        }
        return day / 7;
    }
    case DAY: {
        int day = ts_value2.daynr() - ts_value1.daynr();
        if (day >= 0) {
            day -= ts_value2.time_part_diff(ts_value1) < 0;
        } else {
            day += ts_value2.time_part_diff(ts_value1) > 0;
        }
        return day;
    }
    case HOUR: {
        int64_t second = ts_value2.second_diff(ts_value1);
        int64_t hour = second / 60 / 60;
        return hour;
    }
    case MINUTE: {
        int64_t second = ts_value2.second_diff(ts_value1);
        int64_t minute = second / 60;
        return minute;
    }
    case SECOND: {
        int64_t second = ts_value2.second_diff(ts_value1);
        return second;
    }
    default:
        return BigIntVal::null();
    }
}

void TimestampFunctions::format_prepare(starrocks_udf::FunctionContext* context,
                                        starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || context->get_num_args() < 2 ||
        context->get_arg_type(1)->type != starrocks_udf::FunctionContext::Type::TYPE_VARCHAR ||
        !context->is_arg_constant(1)) {
        VLOG(10) << "format_prepare returned";
        return;
    }

    FormatCtx* fc = new FormatCtx();
    context->set_function_state(scope, fc);

    StringVal* format = reinterpret_cast<StringVal*>(context->get_constant_arg(1));
    if (UNLIKELY(format->is_null)) {
        fc->is_valid = false;
        return;
    }

    fc->fmt = convert_format(context, *format);
    int format_len = DateTimeValue::compute_format_len((const char*)fc->fmt.ptr, fc->fmt.len);
    if (UNLIKELY(format_len >= 128)) {
        fc->is_valid = false;
        return;
    }

    fc->is_valid = true;
    return;
}

void TimestampFunctions::format_close(starrocks_udf::FunctionContext* context,
                                      starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }

    FormatCtx* fc = reinterpret_cast<FormatCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (fc != nullptr) {
        delete fc;
    }
}

StringVal TimestampFunctions::date_format(FunctionContext* ctx, const DateTimeVal& ts_val, const StringVal& format) {
    if (ts_val.is_null || format.is_null) {
        return StringVal::null();
    }

    DateTimeValue ts_value = DateTimeValue::from_datetime_val(ts_val);
    FormatCtx* fc = reinterpret_cast<FormatCtx*>(ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (UNLIKELY(fc == nullptr)) {
        // prepare phase failed, calculate at runtime
        StringVal new_fmt = convert_format(ctx, format);
        if (DateTimeValue::compute_format_len((const char*)new_fmt.ptr, new_fmt.len) >= 128) {
            return StringVal::null();
        }

        char buf[128];
        if (!ts_value.to_format_string((const char*)new_fmt.ptr, new_fmt.len, buf)) {
            return StringVal::null();
        }
        return AnyValUtil::from_string_temp(ctx, buf);
    }

    if (!fc->is_valid) {
        return StringVal::null();
    }

    char buf[128];
    if (!ts_value.to_format_string((const char*)fc->fmt.ptr, fc->fmt.len, buf)) {
        return StringVal::null();
    }
    return AnyValUtil::from_string_temp(ctx, buf);
}

DateTimeVal TimestampFunctions::from_days(FunctionContext* ctx, const IntVal& days) {
    if (days.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue ts_value;
    if (!ts_value.from_date_daynr(days.val)) {
        return DateTimeVal::null();
    }
    DateTimeVal ts_val;
    ts_value.to_datetime_val(&ts_val);
    return ts_val;
}

IntVal TimestampFunctions::to_days(FunctionContext* ctx, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    return IntVal(ts_value.daynr());
}

DoubleVal TimestampFunctions::time_diff(FunctionContext* ctx, const DateTimeVal& ts_val1, const DateTimeVal& ts_val2) {
    if (ts_val1.is_null || ts_val2.is_null) {
        return DoubleVal::null();
    }

    const DateTimeValue& ts_value1 = DateTimeValue::from_datetime_val(ts_val1);
    const DateTimeValue& ts_value2 = DateTimeValue::from_datetime_val(ts_val2);
    return DoubleVal(ts_value1.second_diff(ts_value2));
}

IntVal TimestampFunctions::date_diff(FunctionContext* ctx, const DateTimeVal& ts_val1, const DateTimeVal& ts_val2) {
    if (ts_val1.is_null || ts_val2.is_null) {
        return IntVal::null();
    }
    const DateTimeValue& ts_value1 = DateTimeValue::from_datetime_val(ts_val1);
    const DateTimeValue& ts_value2 = DateTimeValue::from_datetime_val(ts_val2);
    return IntVal(ts_value1.daynr() - ts_value2.daynr());
}

// TimeZone correlation functions.
DateTimeVal TimestampFunctions::timestamp(FunctionContext* ctx, const DateTimeVal& val) {
    return val;
}

// FROM_UNIXTIME() without format
StringVal TimestampFunctions::from_unix(FunctionContext* context, const IntVal& unix_time) {
    if (unix_time.is_null || unix_time.val < 0 || unix_time.val > INT_MAX) {
        return StringVal::null();
    }

    DateTimeValue dtv;
    if (!dtv.from_unixtime(unix_time.val, context->impl()->state()->timezone_obj())) {
        return StringVal::null();
    }
    char buf[64];
    dtv.to_string(buf);
    return AnyValUtil::from_string_temp(context, buf);
}

// FROM_UNIXTIME() with format
StringVal TimestampFunctions::from_unix(FunctionContext* context, const IntVal& unix_time, const StringVal& fmt) {
    if (unix_time.is_null || fmt.is_null || unix_time.val < 0 || unix_time.val > INT_MAX) {
        return StringVal::null();
    }

    DateTimeValue dtv;
    if (!dtv.from_unixtime(unix_time.val, context->impl()->state()->timezone_obj())) {
        return StringVal::null();
    }

    FormatCtx* fc = reinterpret_cast<FormatCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (UNLIKELY(fc == nullptr)) {
        // prepare phase failed, calculate at runtime
        StringVal new_fmt = convert_format(context, fmt);
        char buf[128];
        if (!dtv.to_format_string((const char*)new_fmt.ptr, new_fmt.len, buf)) {
            return StringVal::null();
        }
        return AnyValUtil::from_string_temp(context, buf);
    }

    if (!fc->is_valid) {
        return StringVal::null();
    }

    char buf[128];
    if (!dtv.to_format_string((const char*)fc->fmt.ptr, fc->fmt.len, buf)) {
        return StringVal::null();
    }
    return AnyValUtil::from_string_temp(context, buf);
}

// UNIX_TIMESTAMP()
IntVal TimestampFunctions::to_unix(FunctionContext* context) {
    return IntVal(context->impl()->state()->timestamp_ms() / 1000);
}

// UNIX_TIMESTAMP()
IntVal TimestampFunctions::to_unix(FunctionContext* context, const DateTimeValue& ts_value) {
    int64_t timestamp;
    if (!ts_value.unix_timestamp(&timestamp, context->impl()->state()->timezone_obj())) {
        return IntVal::null();
    } else {
        //To compatible to mysql, timestamp not between 1970-01-01 00:00:00 ~ 2038-01-01 00:00:00 return 0
        timestamp = timestamp < 0 ? 0 : timestamp;
        timestamp = timestamp > INT_MAX ? 0 : timestamp;
        return IntVal(timestamp);
    }
}

// UNIX_TIMESTAMP()
IntVal TimestampFunctions::to_unix(FunctionContext* context, const StringVal& string_val, const StringVal& fmt) {
    if (string_val.is_null || fmt.is_null) {
        return IntVal::null();
    }
    DateTimeValue tv;
    if (!tv.from_date_format_str((const char*)fmt.ptr, fmt.len, (const char*)string_val.ptr, string_val.len)) {
        return IntVal::null();
    }
    return to_unix(context, tv);
}

// UNIX_TIMESTAMP()
IntVal TimestampFunctions::to_unix(FunctionContext* context, const DateTimeVal& ts_val) {
    if (ts_val.is_null) {
        return IntVal::null();
    }
    return to_unix(context, DateTimeValue::from_datetime_val(ts_val));
}

DateTimeVal TimestampFunctions::utc_timestamp(FunctionContext* context) {
    DateTimeValue dtv;
    if (!dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000, "+00:00")) {
        return DateTimeVal::null();
    }

    DateTimeVal return_val;
    dtv.to_datetime_val(&return_val);
    return return_val;
}

DateTimeVal TimestampFunctions::now(FunctionContext* context) {
    DateTimeValue dtv;
    if (!dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000, context->impl()->state()->timezone_obj())) {
        return DateTimeVal::null();
    }

    DateTimeVal return_val;
    dtv.to_datetime_val(&return_val);
    return return_val;
}

DoubleVal TimestampFunctions::curtime(FunctionContext* context) {
    DateTimeValue dtv;
    if (!dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000, context->impl()->state()->timezone_obj())) {
        return DoubleVal::null();
    }

    return dtv.hour() * 3600 + dtv.minute() * 60 + dtv.second();
}

DateTimeVal TimestampFunctions::curdate(FunctionContext* context) {
    DateTimeValue dtv;
    if (!dtv.from_unixtime(context->impl()->state()->timestamp_ms() / 1000, context->impl()->state()->timezone_obj())) {
        return DateTimeVal::null();
    }
    dtv.set_type(TIME_DATE);

    DateTimeVal return_val;
    dtv.to_datetime_val(&return_val);
    return return_val;
}

void TimestampFunctions::convert_tz_prepare(starrocks_udf::FunctionContext* context,
                                            starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || context->get_num_args() != 3 ||
        context->get_arg_type(1)->type != starrocks_udf::FunctionContext::Type::TYPE_VARCHAR ||
        context->get_arg_type(2)->type != starrocks_udf::FunctionContext::Type::TYPE_VARCHAR ||
        !context->is_arg_constant(1) || !context->is_arg_constant(2)) {
        return;
    }

    ConvertTzCtx* ctc = new ConvertTzCtx();
    context->set_function_state(scope, ctc);

    // find from timezone
    StringVal* from = reinterpret_cast<StringVal*>(context->get_constant_arg(1));
    if (UNLIKELY(from->is_null)) {
        ctc->is_valid = false;
        return;
    }
    if (!TimezoneUtils::find_cctz_time_zone(std::string((char*)from->ptr, from->len), ctc->from_tz)) {
        ctc->is_valid = false;
        return;
    }

    // find to timezone
    StringVal* to = reinterpret_cast<StringVal*>(context->get_constant_arg(2));
    if (UNLIKELY(to->is_null)) {
        ctc->is_valid = false;
        return;
    }
    if (!TimezoneUtils::find_cctz_time_zone(std::string((char*)to->ptr, to->len), ctc->to_tz)) {
        ctc->is_valid = false;
        return;
    }

    ctc->is_valid = true;
    return;
}

DateTimeVal TimestampFunctions::convert_tz(FunctionContext* ctx, const DateTimeVal& ts_val, const StringVal& from_tz,
                                           const StringVal& to_tz) {
    if (ts_val.is_null || from_tz.is_null || to_tz.is_null) {
        return DateTimeVal::null();
    }

    const DateTimeValue& ts_value = DateTimeValue::from_datetime_val(ts_val);
    ConvertTzCtx* ctc = reinterpret_cast<ConvertTzCtx*>(ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (UNLIKELY(ctc == nullptr)) {
        int64_t timestamp;
        if (!ts_value.unix_timestamp(&timestamp, std::string((char*)from_tz.ptr, from_tz.len))) {
            return DateTimeVal::null();
        }
        DateTimeValue ts_value2;
        if (!ts_value2.from_unixtime(timestamp, std::string((char*)to_tz.ptr, to_tz.len))) {
            return DateTimeVal::null();
        }

        DateTimeVal return_val;
        ts_value2.to_datetime_val(&return_val);
        return return_val;
    }

    if (!ctc->is_valid) {
        return DateTimeVal::null();
    }

    int64_t timestamp;
    if (!ts_value.unix_timestamp(&timestamp, ctc->from_tz)) {
        return DateTimeVal::null();
    }
    DateTimeValue ts_value2;
    if (!ts_value2.from_unixtime(timestamp, ctc->to_tz)) {
        return DateTimeVal::null();
    }

    DateTimeVal return_val;
    ts_value2.to_datetime_val(&return_val);
    return return_val;
}

void TimestampFunctions::convert_tz_close(starrocks_udf::FunctionContext* context,
                                          starrocks_udf::FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return;
    }

    ConvertTzCtx* ctc = reinterpret_cast<ConvertTzCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (ctc != nullptr) {
        delete ctc;
    }
}

void TimestampFunctions::datetime_trunc_prepare(starrocks_udf::FunctionContext* context,
                                                starrocks_udf::FunctionContext::FunctionStateScope scope) {
    return;
}

DateTimeVal TimestampFunctions::datetime_trunc(starrocks_udf::FunctionContext* ctx,
                                               const starrocks_udf::StringVal& format,
                                               const starrocks_udf::DateTimeVal& ts_val2) {
    DateTimeVal ts_val;
    return ts_val;
}

void TimestampFunctions::datetime_trunc_close(starrocks_udf::FunctionContext* context,
                                              starrocks_udf::FunctionContext::FunctionStateScope scope) {
    return;
}

void TimestampFunctions::date_trunc_prepare(starrocks_udf::FunctionContext* context,
                                            starrocks_udf::FunctionContext::FunctionStateScope scope) {
    return;
}

DateTimeVal TimestampFunctions::date_trunc(starrocks_udf::FunctionContext* ctx, const starrocks_udf::StringVal& format,
                                           const starrocks_udf::DateTimeVal& ts_val2) {
    DateTimeVal ts_val;
    return ts_val;
}

void TimestampFunctions::date_trunc_close(starrocks_udf::FunctionContext* context,
                                          starrocks_udf::FunctionContext::FunctionStateScope scope) {
    return;
}

} // namespace starrocks
