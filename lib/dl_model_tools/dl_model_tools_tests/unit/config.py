import datetime
import decimal
import ipaddress
from typing import Any
import uuid

from dl_type_transformer.native_type import (
    ClickHouseDateTime64NativeType,
    ClickHouseDateTime64WithTZNativeType,
    ClickHouseDateTimeWithTZNativeType,
    ClickHouseNativeType,
    CommonNativeType,
    GenericNativeType,
    LengthedNativeType,
)


TZINFO = datetime.timezone(datetime.timedelta(seconds=-1320))
SAMPLE_DATA: dict[str, Any] = dict(
    # Scalars
    some_int=42,
    some_float=19.89,
    some_str="Some say",
    some_bool=True,
    some_none=None,
    # BI data values
    some_dt=datetime.datetime(2019, 6, 17, 7, 1, 41, 79585, tzinfo=TZINFO),
    some_date=datetime.date(2020, 7, 18),
    some_time=datetime.time(11, 22, 33, 444555, tzinfo=TZINFO),
    some_timedelta=datetime.timedelta(seconds=1320.0231),
    some_decimal=decimal.Decimal("12345" * 9 + "." + "54321" * 9),
    some_uuid=uuid.UUID("12345678123456781234567812345678"),
    some_bytes=b"Another one bites",
    some_ipv4_address=ipaddress.IPv4Address("192.0.2.5"),
    some_ipv6_address=ipaddress.IPv6Address("2001:db8::1000"),
    some_ipv4_network=ipaddress.IPv4Network("192.0.2.0/24"),
    some_ipv6_network=ipaddress.IPv6Network("2001:db8::/64"),
    some_ipv4_interface=ipaddress.IPv4Interface("192.0.2.5/24"),
    some_ipv6_interface=ipaddress.IPv6Interface("2001:db8::1000/24"),
    some_native_types=[
        GenericNativeType(name="tinyblob"),
        CommonNativeType(name="double_precision", nullable=True),
        LengthedNativeType(name="nvarchar2", nullable=False, length=121),
        ClickHouseNativeType(name="uint64", nullable=True, lowcardinality=True),
        ClickHouseDateTimeWithTZNativeType(
            name="datetimewithtz",
            nullable=False,
            lowcardinality=True,
            timezone_name="Europe/Moscow",
        ),
        ClickHouseDateTime64NativeType(
            name="datetime64",
            nullable=False,
            lowcardinality=True,
            precision=3,
        ),
        ClickHouseDateTime64WithTZNativeType(
            name="datetime64withtz",
            nullable=False,
            lowcardinality=True,
            precision=3,
            timezone_name="Europe/Moscow",
        ),
    ],
)


EXPECTED_DUMP: dict[str, Any] = dict(
    # Scalars are unchanged
    some_int=42,
    some_float=19.89,
    some_str="Some say",
    some_bool=True,
    some_none=None,
    # More complex types have custom dumpers
    some_dt={"__dl_type__": "datetime", "value": [[2019, 6, 17, 7, 1, 41, 79585], -1320.0]},
    some_date={"__dl_type__": "date", "value": "2020-07-18"},
    some_time={"__dl_type__": "time", "value": [[11, 22, 33, 444555], -1320.0]},
    some_timedelta={"__dl_type__": "timedelta", "value": 1320.0231},
    some_decimal={
        "__dl_type__": "decimal",
        "value": "123451234512345123451234512345123451234512345.543215432154321543215432154321543215432154321",
    },
    some_uuid={"__dl_type__": "uuid", "value": "12345678-1234-5678-1234-567812345678"},
    some_bytes={"__dl_type__": "bytes", "value": "QW5vdGhlciBvbmUgYml0ZXM="},
    some_ipv4_address={"__dl_type__": "ipv4_address", "value": "192.0.2.5"},
    some_ipv6_address={"__dl_type__": "ipv6_address", "value": "2001:db8::1000"},
    some_ipv4_network={"__dl_type__": "ipv4_network", "value": "192.0.2.0/24"},
    some_ipv6_network={"__dl_type__": "ipv6_network", "value": "2001:db8::/64"},
    some_ipv4_interface={"__dl_type__": "ipv4_interface", "value": "192.0.2.5/24"},
    some_ipv6_interface={"__dl_type__": "ipv6_interface", "value": "2001:db8::1000/24"},
    some_native_types=[
        {
            "__dl_type__": "dl_native_type",
            "value": {"name": "tinyblob", "native_type_class_name": "generic_native_type"},
        },
        {
            "__dl_type__": "dl_native_type",
            "value": {"name": "double_precision", "native_type_class_name": "common_native_type", "nullable": True},
        },
        {
            "__dl_type__": "dl_native_type",
            "value": {
                "length": 121,
                "name": "nvarchar2",
                "native_type_class_name": "lengthed_native_type",
                "nullable": False,
            },
        },
        {
            "__dl_type__": "dl_native_type",
            "value": {
                "lowcardinality": True,
                "name": "uint64",
                "native_type_class_name": "clickhouse_native_type",
                "nullable": True,
            },
        },
        {
            "__dl_type__": "dl_native_type",
            "value": {
                "explicit_timezone": True,
                "lowcardinality": True,
                "name": "datetimewithtz",
                "native_type_class_name": "clickhouse_datetimewithtz_native_type",
                "nullable": False,
                "timezone_name": "Europe/Moscow",
            },
        },
        {
            "__dl_type__": "dl_native_type",
            "value": {
                "lowcardinality": True,
                "name": "datetime64",
                "native_type_class_name": "clickhouse_datetime64_native_type",
                "nullable": False,
                "precision": 3,
            },
        },
        {
            "__dl_type__": "dl_native_type",
            "value": {
                "explicit_timezone": True,
                "lowcardinality": True,
                "name": "datetime64withtz",
                "native_type_class_name": "clickhouse_datetime64withtz_native_type",
                "nullable": False,
                "precision": 3,
                "timezone_name": "Europe/Moscow",
            },
        },
    ],
)
PERF_DATA = {
    "data": [
        [
            1,
            1.0,
            False,
            None,
            "data",
            SAMPLE_DATA["some_dt"],
            SAMPLE_DATA["some_date"],
            SAMPLE_DATA["some_time"],
            SAMPLE_DATA["some_uuid"],
        ],
        [
            {"array": list(range(10))},
            {"another_array": list(range(10, 20))},
        ],
    ]
}


class CustomType:
    pass
