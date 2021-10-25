import pytest

from tests.helper import assert_meta_col_conversion, valid_types

from mojap_metadata import Metadata
from mojap_metadata.converters.arrow_converter import (
    ArrowConverter,
    _extract_bracket_params,
)
import pyarrow as pa
from mojap_metadata.converters import BaseConverterOptions


@pytest.mark.parametrize(argnames="meta_type", argvalues=valid_types)
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
        ("list<int64>", pa.list_(pa.int64())),
        ("list_<list<int64>>", pa.list_(pa.list_(pa.int64()))),
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
    # Test round trip
    ac = ArrowConverter()
    round_trip_meta_type = ac.reverse_convert_col_type(arrow_type)
    # reverse always returns non-underscored aliases for bool and list
    meta_type = meta_type.replace("bool_", "bool")
    meta_type = meta_type.replace("list_", "list")

    # utf8 and string are the same
    # pa.string().equals(pa.utf8()) # True
    # So reverse conversion sets pa.utf8() to "string"
    meta_type = meta_type.replace("utf8", "string")

    # finally remove any whitespace
    meta_type = "".join(meta_type.split())
    round_trip_meta_type = "".join(round_trip_meta_type.split())

    assert meta_type == round_trip_meta_type


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

    expected_names = ["my_int", "my_double", "my_date", "my_decimal"]
    expected_types = [pa.int64(), pa.float64(), pa.date64(), pa.decimal128(10, 2)]
    assert schema1.names == expected_names

    checks1 = [a.equals(e) for a, e in zip(schema1.types, expected_types)]
    assert all(checks1)

    # Do schema2 assertions
    expected_names.append("my_timestamp")
    expected_types.append(pa.timestamp("s"))

    assert schema2.names == expected_names

    checks2 = [a.equals(e) for a, e in zip(schema2.types, expected_types)]
    assert all(checks2)

    # Also check specific type properties
    assert schema2.field("my_decimal").type.precision == 10
    assert schema2.field("my_decimal").type.scale == 2
    assert schema2.field("my_timestamp").type.unit == "s"


def test_generate_to_meta():

    struct = pa.struct(
        [
            ("x", pa.timestamp("s")),
            (
                "y",
                pa.struct(
                    [
                        ("f1", pa.int32()),
                        ("f2", pa.string()),
                        ("f3", pa.decimal128(3, 5)),
                    ]
                ),
            ),
        ]
    )

    example_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
            pa.field("c", struct),
            pa.field("d", pa.list_(pa.int64())),
        ]
    )

    expected_name_type = (
        ("a", "int64"),
        ("b", "string"),
        ("c", "struct<"),
        ("d", "list<"),
    )

    ac = ArrowConverter()
    meta1 = ac.generate_to_meta(arrow_schema=example_schema)

    assert isinstance(meta1, Metadata)

    checks = [
        c["name"] == e[0] and c["type"].startswith(e[1])
        for c, e in zip(meta1.columns, expected_name_type)
    ]
    assert all(checks)

    meta2 = ac.generate_to_meta(
        arrow_schema=example_schema,
        meta_init_dict={"name": "test", "file_format": "parquet"},
    )
    assert isinstance(meta2, Metadata)
    assert meta2.name == "test"
    assert meta2.file_format == "parquet"
    assert meta1.columns == meta2.columns

    # Check warning is raised on columns being overwritten
    with pytest.warns(UserWarning):
        _ = ac.generate_to_meta(
            arrow_schema=example_schema,
            meta_init_dict={"columns": [{"name": "stuff", "type": "string"}]},
        )


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
