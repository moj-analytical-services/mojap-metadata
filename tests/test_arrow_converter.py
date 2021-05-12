import pytest

from tests.helper import assert_meta_col_conversion, valid_types

from mojap_metadata import Metadata
from mojap_metadata.converters.arrow_converter import (
    ArrowConverter,
    _extract_bracket_params,
)
import pyarrow as pa
from mojap_metadata.converters import BaseConverterOptions


@pytest.mark.parametrize(
    argnames="meta_type",
    argvalues=valid_types
)
def test_converter_accepts_type(meta_type):
    """
    If new type is added to tests.valid_types then it may fail this test

    Args:
        meta_type ([type]): str
    """
    ac = ArrowConverter()
    _ = ac.convert_col_type(meta_type)


@pytest.mark.parametrize(
    argnames="meta_type,arrow_type",
    argvalues=[
        ("bool_", pa.bool_()),
        ("int8", pa.int8()),
        ("int16", pa.int16()),
        ("int32", pa.int32()),
        ("int64", pa.int64()),
        ("uint8", pa.uint8()),
        ("uint16", pa.uint16()),
        ("uint32", pa.uint32()),
        ("uint64", pa.uint64()),
        ("float16", pa.float16()),
        ("float32", pa.float32()),
        ("float64", pa.float64()),
        ("decimal128(38,1)", pa.decimal128(38, 1)),
        ("decimal128(1,2)", pa.decimal128(1, 2)),
        ("time32(s)", pa.time32("s")),
        ("time32(ms)", pa.time32("ms")),
        ("time64(us)", pa.time64("us")),
        ("time64(ns)", pa.time64("ns")),
        ("timestamp(s)", pa.timestamp("s")),
        ("timestamp(ms)", pa.timestamp("ms")),
        ("timestamp(us)", pa.timestamp("us")),
        ("timestamp(ns)", pa.timestamp("ns")),
        ("date32", pa.date32()),
        ("date64", pa.date64()),
        ("string", pa.string()),
        ("large_string", pa.large_string()),
        ("utf8", pa.utf8()),
        ("large_utf8", pa.large_utf8()),
        ("binary", pa.binary()),
        ("binary(128)", pa.binary(128)),
        ("large_binary", pa.large_binary()),
        ("struct<num:int64>", pa.struct([("num", pa.int64())])),
        ("list_<int64>", pa.list_(pa.int64())),
        ("list_<list_<int64>>", pa.list_(pa.list_(pa.int64()))),
        ("large_list<int64>", pa.large_list(pa.int64())),
        ("large_list<large_list<int64>>", pa.large_list(pa.large_list(pa.int64()))),
        (
            "struct<num:int64, newnum:int64>",
            pa.struct([("num", pa.int64()), ("newnum", pa.int64())]),
        ),
        (
            "struct<num:int64, arr:list_<int64>>",
            pa.struct([("num", pa.int64()), ("arr", pa.list_(pa.int64()))]),
        ),
        (
            "list_<struct<num:int64,desc:string>>",
            pa.list_(pa.struct([("num", pa.int64()), ("desc", pa.string())])),
        ),
        (
            "struct<num:int64,desc:string>",
            pa.struct([("num", pa.int64()), ("desc", pa.string())]),
        ),
        ("list_<decimal128(38,0)>", pa.list_(pa.decimal128(38, 0))),
        (
            "struct<a:timestamp(s),b:struct<f1: int32, f2: string,f3:decimal128(3,5)>>",
            pa.struct(
                [
                    ("a", pa.timestamp("s")),
                    (
                        "b",
                        pa.struct(
                            [
                                ("f1", pa.int32()),
                                ("f2", pa.string()),
                                ("f3", pa.decimal128(3, 5)),
                            ]
                        ),
                    ),
                ]
            ),
        ),
    ],
)
def test_meta_to_arrow_type(meta_type, arrow_type):
    assert_meta_col_conversion(
        ArrowConverter, meta_type, arrow_type, expect_raises=None
    )


def test_generate_from_meta():
    md = Metadata.from_dict(
        {
            "name": "test_table",
            "file_format": "test-format",
            "columns": [
                {
                    "name": "my_int",
                    "type": "int64",
                    "description": "This is an integer",
                    "nullable": False,
                },
                {"name": "my_double", "type": "float64", "nullable": True},
                {"name": "my_date", "type": "date64"},
                {"name": "my_decimal", "type": "decimal128(10,2)"},
                {
                    "name": "my_timestamp",
                    "type": "timestamp(s)",
                    "description": "Partition column",
                },
            ],
            "partitions": ["my_timestamp"],
        }
    )

    ac = ArrowConverter()
    assert isinstance(ac.options, BaseConverterOptions)

    schema1 = ac.generate_from_meta(md)
    schema2 = ac.generate_from_meta(md, False)

    assert isinstance(schema1, pa.Schema)
    assert isinstance(schema2, pa.Schema)

    schema_str1 = (
        "my_int: int64 not null\nmy_double: double\n"
        "my_date: date64[ms]\nmy_decimal: decimal(10, 2)"
    )
    schema_str2 = schema_str1 + "\nmy_timestamp: timestamp[s]"
    assert schema1.to_string() == schema_str1
    assert schema2.to_string() == schema_str2


@pytest.mark.parametrize(
    "meta_type,response",
    [
        ("decimal128(1,2)", ("decimal128", [1, 2])),
        ("binary", ("binary", [])),
        ("binary()", ("binary", [])),
        ("binary(8)", ("binary", [8])),
        ("timestamp(s)", ("timestamp", ["s"])),
        ("time64(us)", ("time64", ["us"])),
    ],
)
def test_extract_bracket_params(meta_type, response):
    assert response == _extract_bracket_params(meta_type)
