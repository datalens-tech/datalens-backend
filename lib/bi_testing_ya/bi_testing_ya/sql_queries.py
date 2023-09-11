from __future__ import annotations


CH_QUERY = r"""
select
    arrayJoin([11, 22, NULL]) as a,
    [33, 44] as b,
    toDateTime('2020-01-02 03:04:05', 'UTC') + a as ts
"""


CH_QUERY_FULL = r"""
select
    arrayJoin(range(7)) as number,
    'test' || toString(number) as str,
    cast(number as Int64) as num_int64,
    cast(number as Int32) as num_int32,
    cast(number as Int16) as num_int16,
    cast(number as Int8) as num_int8,
    cast(number as UInt64) as num_uint64,
    cast(number as UInt32) as num_uint32,
    cast(number as UInt16) as num_uint16,
    cast(number as Nullable(UInt8)) as num_uint8_n,
    cast(number as Nullable(Date)) as num_date,
    cast(number as Nullable(DateTime)) as num_datetime,
    cast(number as Float64) as num_float64,
    cast(number as Nullable(Float32)) as num_float32_n,
    cast(number as Decimal(3, 3)) as num_decimal,
    cast(number as String) as num_string,
    cast('bcc3de04-d31a-4e17-8485-8ef423f646be' as UUID) as num_uuid,
    cast(number as IPv4) as num_ipv4,
    cast('20:43:ff::40:1bc' as IPv6) as num_ipv6,
    cast(toString(number) as FixedString(10)) as num_fixedstring,
    cast(number as Enum8('a'=0, 'b'=1, 'c'=2, 'd'=3, 'e'=4, 'f'=5, 'g'=6)) as num_enum8,
    cast(number as Enum16('a'=0, 'b'=1, 'c'=2, 'd'=3, 'e'=4, 'f'=5, 'g'=6)) as num_enum16,
    (number, 'x') as num_tuple,
    [number, -2] as num_intarray,
    [toString(number), '-2'] as num_strarray,
    cast(toString(number) as LowCardinality(Nullable(String))) as num_lc,
    cast(number as DateTime('Pacific/Chatham')) as num_dt_tz,
    cast(number as DateTime64(6)) as num_dt64,
    cast(number as DateTime64(6, 'America/New_York')) as num_dt64_tz
limit 10
"""


# results in 7 rows, which is checked in a bi_core subselect test
PG_QUERY_FULL = r"""
with base as (
    select generate_series(0, 6) as number
)
select
    number,
    'test' || number::text as str,
    number::bool as num_bool,
    number::text::bytea as num_bytea,
    number::char as num_char,
    -- number::name as num_name,
    number::int8 as num_int8,
    number::int2 as num_int2,
    number::int4 as num_int4,
    number::text as num_text,
    number::oid as num_oid,
    number::text::json as num_json,
    -- number::text::jsonb as num_jsonb, -- tests-pg too old
    number::float4 as num_float4,
    number::float8 as num_float8,
    number::numeric as num_numeric,
    (number || ' second')::interval as num_interval,
    number::varchar(12) as num_varchar,
    ('2020-01-0' || (number + 1))::date as num_date,
    ('00:01:0' || number)::time as num_time,
    ('2020-01-01T00:00:0' || number)::timestamp as num_timestamp,
    ('2020-01-01T00:00:0' || number)::timestamptz as num_timestamptz,
    ARRAY[number, 2, 3] as num_array,
    'nan'::double precision + number as some_nan
from base
limit 10
"""


ORACLE_QUERY_FULL = r"""
select
    num,
    'test' || num as num_str,
    cast(num as integer) as num_integer,
    cast(num as number) as num_number,
    -- cast(num as number(9,9)) as num_number_9_9,
    cast(num as binary_float) as num_binary_float,
    cast(num as binary_double) as num_binary_double,
    cast(num as char) as num_char,
    cast(num as varchar(3)) as num_varchar,
    cast(num as varchar2(4)) as num_varchar2,
    cast(num as nchar) as num_nchar,
    -- cast(num as nvarchar(5)) as num_nvarchar,
    cast(num as nvarchar2(5)) as num_nvarchar2,
    DATE '2020-01-01' + num as num_date,
    TIMESTAMP '1999-12-31 23:59:59.10' + numToDSInterval(num, 'second') as num_timestamp,
    TIMESTAMP '1999-12-31 23:59:59.10-07:00' + numToDSInterval(num, 'second') as num_timestamp_tz
from (
    select 0 as num from dual
    union all
    select 1 as num from dual
    union all
    select 6 as num from dual
) sq
"""


# host='ydb-ru.yandex.net', port=2135, db_name='/ru/yql/test/datalens-ydb-integration-test'
# Source table creation:
# https://yql.yandex-team.ru/Operations/YOMmqBJKfYhSBOEvZkmM5ChYkHorjfQe_ScjBRHDLLw=
YDB_QUERY_FULL = r'''
select
    id,
    MAX('⋯') as some_str,
    MAX(CAST('⋯' AS UTF8)) as some_utf8,
    MAX(111) as some_int,
    MAX(CAST(111 AS UInt8)) as some_uint8,
    MAX(4398046511104) as some_int64,
    MAX(18446744073709551606) as some_uint64,
    MAX(1.11e-11) as some_double,
    MAX(true) as some_bool,
    MAX(Date('2021-06-09')) as some_date,
    MAX(Datetime('2021-06-09T20:50:47Z')) as some_datetime,
    MAX(Timestamp('2021-07-10T21:51:48.841512Z')) as some_timestamp,

    MAX(ListHead(ListSkip(Unicode::SplitToList(CAST(some_string AS UTF8), ''), 3))) as str_split,
    MAX(ListConcat(ListReplicate(CAST(' ' AS UTF8), 5))) as num_space_by_lst,
    MAX(CAST(String::LeftPad('', 5, ' ') AS UTF8)) as num_space,
    MAX(Unicode::ReplaceAll(CAST(some_string AS UTF8), COALESCE(CAST('f' AS UTF8), ''), COALESCE(CAST(some_string AS UTF8), ''))) as str_replace,
    MAX(Unicode::Substring(CAST(some_utf8 AS UTF8), 3, 3)) as utf8_tst,
    MAX(Unicode::Substring(CAST(some_string AS UTF8), Unicode::GetLength(CAST(some_string AS UTF8)) - 3)) as str_right,
    MAX(Unicode::Substring(CAST(some_utf8 AS UTF8), Unicode::GetLength(CAST(some_utf8 AS UTF8)) - 3)) as utf8_right,
    MAX(Unicode::Substring(CAST(some_string AS UTF8), 0, 3)) as str_left,
    MAX(Unicode::Substring(CAST(some_utf8 AS UTF8), 0, 3)) as utf8_left,
    MAX(Unicode::Substring(some_utf8, CAST(String::Find(some_utf8, '≝') AS UInt64))) as utf8_find_substring_wrong,
    MAX(String::StartsWith(some_utf8, '…')) as utf8_startswith_const,
    MAX(String::EndsWith(some_utf8, '!')) as utf8_endswith_const,
    MAX(Unicode::ToLower(CAST(some_string AS UTF8))) as str_lower,
    MAX(Unicode::ToUpper(CAST(some_utf8 AS UTF8))) as utf8_upper,
    MAX(Unicode::ToLower(CAST(some_utf8 AS UTF8))) as utf8_lower,
    MAX(CASE WHEN some_string IS NULL THEN NULL ELSE String::Contains(some_utf8, COALESCE(some_string, '')) END) as utf8_contains_nonconst,
    MIN(String::Contains(some_string, 'a')) as str_contains_const,
    MIN(String::Contains(some_utf8, '!')) as utf8_contains_const,
    MIN(some_string LIKE '%a%' ESCAPE '!') as str_contains_like_const,
    MIN(some_utf8 LIKE '%!!%' ESCAPE '!') as utf8_contains_like_const,
    MIN(some_utf8 || some_utf8) as utf8_concat,
    MIN(CASE WHEN some_uint8 IS NULL THEN NULL ELSE Unicode::FromCodePointList(AsList(COALESCE(CAST(some_uint8 AS SMALLINT), 0))) END) as num_char,
    MIN(ListHead(Unicode::ToCodePointList(Unicode::Substring(cast(some_utf8 as utf8), 0, 1)))) as utf8_ascii,

    MIN(some_string) as str_straight,
    MIN(some_utf8) as utf8_straight,
    MIN(some_uint8) as uint8_straight,
    MIN(Math::Tan(some_double)) as dbl_tan,
    MIN(Math::Pow(some_double, 2)) as dbl_square,
    MIN(Math::Sqrt(some_double)) as dbl_sqrt,
    MIN(Math::Sin(some_double)) as dbl_sin,
    MIN(CASE WHEN some_double < 0 THEN -1 WHEN some_double > 0 THEN 1 ELSE 0 END) as dbl_sign,
    MIN(Math::Round(some_double, -2)) as dbl_round2n,
    MIN(Math::Round(some_double, 2)) as dbl_round2,
    MIN(Math::Round(some_double)) as dbl_round,
    MIN(CAST(some_int64 / some_uint64 AS BIGINT)) as int_int_div,
    MIN(LEAST(some_uint64, some_int64)) as int_least,
    MIN(GREATEST(some_uint64, some_int64)) as int_greatest,
    MIN(Math::Log10(some_uint8)) as int_log10,
    MIN(Math::Log(some_uint8)) as int_log,
    MIN(Math::Exp(some_uint8)) as int_exp,
    MIN(some_uint8 / Math::Pi() * 180.0) as int_degrees,
    MAX(Math::Cos(some_double)) as dbl_cos,
    MAX(Math::Floor(some_double)) as dbl_floor,
    MAX(Math::Ceil(some_double)) as dbl_ceil,
    MAX(some_double) as dbl_straight,
    MIN(Math::Atan2(some_uint8, some_int32)) as int_atan2,
    MIN(Math::Atan(some_uint8)) as int_atan,
    MIN(Math::Asin(some_uint8)) as int_asin,
    MIN(Math::Acos(some_uint8)) as int_acos,
    MIN(COALESCE(some_uint8, some_int64, some_uint64)) as int_coalesce,
    MIN_BY(some_int64, Math::Abs(some_int64)) as int_min_by,
    MAX_BY(some_int64, Math::Abs(some_int64)) as int_max_by,
    MAX(some_int64 IS NULL) as int_isnull,
    COUNT(DISTINCT IF(some_int64 > -9999, some_int64, NULL)) as some_countd_if,
    MAX(IF(some_bool, 1, 0)) as some_if,
    MAX(some_datetime not between Datetime('2011-06-07T18:19:20Z') and Datetime('2031-06-07T18:19:20Z')) as dt_notbetween,
    MAX(some_datetime between Datetime('2022-06-07T18:19:20Z') and Datetime('2031-06-07T18:19:20Z')) as dt_between_f,
    MAX(some_datetime between Datetime('2011-06-07T18:19:20Z') and Datetime('2031-06-07T18:19:20Z')) as dt_between,
    MAX(some_double in (1.0, NULL)) as dbl_in_null,
    MIN(some_double not in (1.0, 2.2, 3579079710.351049881)) as dbl_notin,
    MAX(some_double in (1.0, 2.2, 3579079710.351049881)) as dbl_in_f,
    -- IN may produce unexpected result when used with nullable arguments. Consider adding 'PRAGMA AnsiInForEmptyOrNullableItemsCollections;'
    MAX(some_double in (1.0, 2.2, 1579079710.351049881)) as dbl_in,
    MAX(some_utf8 in ('a', 'b', 'C')) as text_in_f,
    MAX(some_utf8 in ('a', 'b', '… «C≝⋯≅M»!')) as text_in,
    SOME(some_bool or some_int64 is not null) as some_or,
    MAX(some_bool and some_int64 is null) as some_and,
    MAX(some_utf8 > some_string) as str_gt,
    MAX(some_utf8 <= some_string) as str_lte,
    MAX(some_double >= some_double) as dbl_gte,
    MAX(some_double <= some_double) as dbl_lte,
    MAX(some_double < some_double) as dbl_lt,
    MAX(some_double != some_double) as dbl_neq,
    MAX(some_double = some_double) as dbl_eq,
    MAX(some_utf8 = some_utf8) as text_eq,
    MAX(some_utf8 not like 'ы%') as text_not_like,
    MAX(some_utf8 like 'ы%') as text_like_false,
    MAX(some_utf8 like '%') as text_like,
    MAX(some_string like 'ы%') as bytes_like_false,
    MAX(some_string like '%') as bytes_like,
    MAX(DateTime::ToMicroseconds(
        some_datetime
        - (some_datetime + DateTime::IntervalFromSeconds(12345))
    ) / 86400000000.0) as datetime_datetime_sub,
    MAX(DateTime::ToDays(
        some_date
        - (some_date + DateTime::IntervalFromSeconds(1234567))
    )) as date_date_sub,
    MAX(some_datetime - DateTime::IntervalFromMicroseconds(CAST(Math::Ceil(4.4 * 86400 * 1000000) AS INTEGER))) as datetime_sub,
    MAX(some_datetime + DateTime::IntervalFromMicroseconds(CAST(4.4 * 86400 * 1000000 AS INTEGER))) as datetime_add,
    MAX(some_date - DateTime::IntervalFromDays(CAST(Math::Ceil(4.4) AS INTEGER))) as date_subtract,
    MAX(some_date + DateTime::IntervalFromDays(CAST(4.4 AS INTEGER))) as date_add,
    MAX(some_int64 - some_uint8) as int_subtract,
    MAX(some_int64 + some_uint8) as int_add,
    MAX(some_double % (some_double / 3.456)) as dbl_mod,
    MAX(some_int64 % some_uint8) as int_mod,
    MAX(CAST(some_int64 / 10000 AS DOUBLE) / some_uint8) as int_div,
    MIN(some_int64 * some_double) as num_mult,
    MIN(Math::Pow(some_int64, 2)) as num_pow,
    MAX(some_bool = FALSE) as bool_isfalse,
    MAX(some_bool = TRUE) as bool_istrue,
    MIN(some_int64 != 0.0) as num_istrue,
    MIN(-some_int64) as num_neg,
    MIN(some_utf8 = '') as text_not,
    MIN(some_string = '') as bytes_not,
    MIN(some_uint8 = 0) as num_not,
    MIN(NOT some_bool) as bool_not,

    MIN(DateTime::GetWeekOfYearIso8601(some_datetime)) as datetime_yearweek,
    MIN(DateTime::GetDayOfWeek(some_datetime)) as datetime_weekday,  -- 1 .. 7
    MIN(DateTime::GetYear(some_datetime)) as datetime_year,
    MIN(CAST((DateTime::GetMonth(some_datetime) + 2) / 3 AS INTEGER)) as datetime_quarter,
    MIN(DateTime::GetMonth(some_datetime)) as datetime_month,
    MIN(DateTime::GetDayOfMonth(some_datetime)) as datetime_day,
    MIN(DateTime::GetHour(some_datetime)) as datetime_hour,
    MIN(DateTime::GetMinute(some_datetime)) as datetime_minute,
    MIN(DateTime::GetSecond(some_datetime)) as datetime_second,

    MIN(DateTime::MakeDatetime(DateTime::StartOfWeek(some_datetime))) as datetime_startofweek,
    MIN(DateTime::MakeDatetime(DateTime::StartOfMonth(some_datetime))) as datetime_startofmonth,
    MIN(DateTime::MakeDatetime(DateTime::StartOfQuarter(some_datetime))) as datetime_startofquarter,
    MIN(DateTime::MakeDatetime(DateTime::StartOfYear(some_datetime))) as datetime_startofyear,

    MIN(DateTime::MakeDate(DateTime::StartOfWeek(some_date))) as date_startofweek,
    MIN(DateTime::MakeDate(DateTime::StartOfMonth(some_date))) as date_startofmonth,
    MIN(DateTime::MakeDate(DateTime::StartOfQuarter(some_date))) as date_startofquarter,
    MIN(DateTime::MakeDate(DateTime::StartOfYear(some_date))) as date_startofyear,

    MIN(DateTime::MakeDate(DateTime::ShiftYears(some_date, coalesce(some_uint8, 0)))) as date_shiftyears,
    MIN('[' || CAST(some_double AS UTF8) || ',' || CAST(37.622504 AS UTF8) || ']') as tst_geopoint,
    MIN(CAST(CAST('2008e1c9-44a6-4fac-a61d-e42675b77309' AS UUID) AS UTF8)) as text_from_uuid,
    -- SOME(CAST('2008e1c9-44a6-4fac-a61d-e42675b77309' AS UUID)) as some_uuid,
    MIN(DateTime::Format('%Y-%m-%d %H:%M:%S')(some_datetime)) as text_from_datetime_proper,
    MIN(CAST(some_datetime AS UTF8)) as text_from_datetime,
    MIN(CAST(some_date AS UTF8)) as text_from_date,
    MIN(CASE WHEN true IS NULL THEN NULL WHEN true = true THEN 'True' ELSE 'False' END) as text_from_bool,
    MIN(CAST(ToBytes(Yson::SerializePretty(Yson::From(some_string))) AS UTF8)) as text_from_stuff,
    MIN(CASE WHEN some_datetime IS NULL THEN NULL ELSE true END) as bool_from_datetime,
    MIN(CAST(true as BIGINT)) as int_from_bool,
    MIN(CAST(some_double AS UTF8)) as text_from_double,  -- XXXXXXXXXX: 1579079710.35105 -> "1579079710"
    DateTime::MakeDatetime(DateTime::ParseIso8601('2021-06-01 18:00:59')) as datetime_from_str,
    MIN(CAST(NULL AS BOOL)) as null_boolean,
    MIN(CAST(NULL AS BIGINT)) as null_bigint,
    MIN(CAST(NULL AS DATETIME)) as null_datetime,
    MIN(CAST(NULL AS DATE)) as null_date,
    MIN(Math::Abs(-1 * some_double)) as dbl_abs,
    String::JoinFromList(ListMap(TOPFREQ(some_datetime, 5), ($x) -> { RETURN cast($x.Value as Utf8); }), ', ') as top_concat,
    String::JoinFromList(ListSortAsc(AGGREGATE_LIST_DISTINCT(cast(some_date as Utf8))), ', ') as date_all_concat,
    SOME(some_string) as bytes_some,
    MEDIAN(some_int64) as int_median,
    PERCENTILE(some_int64, 0.8) as int_percentile,
    VARPOP(some_int64) as int_varpop,
    VARSAMP(some_int64) as int_varsamp,
    STDDEVPOP(some_int64) as int_stddevpop,
    STDDEVSAMP(some_int64) as int_stddevsamp,
    CountDistinctEstimate(some_double) as dbl_count_distinct_approx,
    COUNT(DISTINCT some_double) as dbl_count_distinct,
    COUNT_IF(some_double < 0) as dbl_count_if_empty,
    COUNT_IF(some_double > 0) as dbl_count_if,
    COUNT(1) as cnt,
    MIN(some_date) as date_min,
    MAX(some_datetime) as datetime_max,
    CAST(CAST(AVG(CAST(some_datetime as DOUBLE)) AS BIGINT) AS DATETIME) as datetime_avg,
    -- date -> dt -> bigint -> avg (-> double) -> bigint -> datetime -> date
    CAST(CAST(CAST(
        AVG(
            CAST(CAST(some_date AS DATETIME) AS BIGINT)
        )
        AS BIGINT) AS DATETIME) AS DATE) as date_avg,
    AVG_IF(some_int64, some_int64 > -1) as int_avg_if,
    AVG(some_int64) as int_avg,
    SUM_IF(some_int64, some_int64 > 10) as int_sum_if,
    SUM(some_int64) as int_sum,

from `test_table_h`
group by id
order by id
limit 1000
'''
