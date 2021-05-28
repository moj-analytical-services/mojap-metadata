import pytest
import json

from tests.helper import assert_meta_col_conversion, valid_types

from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import (
    GlueConverter,
    GlueConverterOptions,
    _default_type_converter,
)


@pytest.mark.parametrize(argnames="meta_type", argvalues=valid_types)
def test_converter_accepts_type(meta_type):
    """
    If new type is added to tests.valid_types then it may fail this test

    Args:
        meta_type ([type]): str
    """
    emc = GlueConverter()
    emc.options.ignore_warnings = True
    unsupported_types = [k for k, v in _default_type_converter.items() if v[0] is None]
    unsupported_types = tuple(unsupported_types)
    if not meta_type.startswith(unsupported_types):
        _ = emc.convert_col_type(meta_type)


@pytest.mark.parametrize(
    argnames="meta_type,glue_type,expect_raises",
    argvalues=[
        ("bool", "boolean", None),
        ("bool_", "boolean", None),
        ("int8", "tinyint", None),
        ("int16", "smallint", None),
        ("int32", "int", None),
        ("int64", "bigint", None),
        ("uint8", "smallint", "warning"),
        ("uint16", "int", "warning"),
        ("uint32", "bigint", "warning"),
        ("uint64", None, "error"),
        ("float16", "float", "warning"),
        ("float32", "float", None),
        ("float64", "double", None),
        ("decimal128(0,38)", "decimal(0,38)", None),
        ("decimal128(1,2)", "decimal(1,2)", None),
        ("time32(s)", None, "error"),
        ("time32(ms)", None, "error"),
        ("time64(us)", None, "error"),
        ("time64(ns)", None, "error"),
        ("timestamp(s)", "timestamp", None),
        ("timestamp(ms)", "timestamp", None),
        ("timestamp(us)", "timestamp", None),
        ("timestamp(ns)", "timestamp", None),
        ("date32", "date", None),
        ("date64", "date", None),
        ("string", "string", None),
        ("large_string", "string", None),
        ("utf8", "string", None),
        ("large_utf8", "string", None),
        ("binary", "binary", None),
        ("binary(128)", "binary", None),
        ("large_binary", "binary", None),
        ("struct<num:int64>", "struct<num:bigint>", None),
        ("list_<int64>", "array<bigint>", None),
        ("list<int64>", "array<bigint>", None),
        ("list_<list_<int64>>", "array<array<bigint>>", None),
        ("list_<list<int64>>", "array<array<bigint>>", None),
        ("large_list<int64>", "array<bigint>", None),
        ("large_list<large_list<int64>>", "array<array<bigint>>", None),
        ("struct<num:int64, newnum:int64>", "struct<num:bigint, newnum:bigint>", None),
        (
            "struct<num:int64, arr:list_<int64>>",
            "struct<num:bigint, arr:array<bigint>>",
            None,
        ),
        (
            "list_<struct<num:int64,desc:string>>",
            "array<struct<num:bigint, desc:string>>",
            None,
        ),
        ("struct<num:int64,desc:string>", "struct<num:bigint, desc:string>", None),
        ("list_<decimal128(38,0)>", "array<decimal(38,0)>", None),
        (
            "struct<a:timestamp(s),b:struct<f1: int32, f2: string,f3:decimal128(3,5)>>",
            "struct<a:timestamp, b:struct<f1:int, f2:string, f3:decimal(3,5)>>",
            None,
        ),
        (
            "struct<k1:list<string>, k2:string, k3:string, k4:string, k5:list<string>, k6:string>", # noqa
            "struct<k1:array<string>, k2:string, k3:string, k4:string, k5:array<string>, k6:string>", # noqa
            None
        )
    ],
)
def test_meta_to_glue_type(meta_type, glue_type, expect_raises):
    assert_meta_col_conversion(GlueConverter, meta_type, glue_type, expect_raises)


@pytest.mark.parametrize(
    argnames="spec_name,serde_name,expected_file_name",
    argvalues=[
        ("csv", "lazy", "test_simple_lazy_csv"),
        ("csv", "open", "test_simple_open_csv"),
        ("json", "hive", "test_simple_hive_json"),
        ("json", "openx", "test_simple_openx_json"),
        ("parquet", None, "test_simple_parquet"),
    ],
)
def test_generate_from_meta(spec_name, serde_name, expected_file_name):
    md = Metadata.from_dict(
        {
            "name": "test_table",
            "file_format": spec_name,
            "columns": [
                {
                    "name": "my_int",
                    "type": "int64",
                    "description": "This is an integer",
                },
                {"name": "my_double", "type": "float64"},
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

    gc = GlueConverter()
    if spec_name == "csv":
        gc.options.set_csv_serde(serde_name)

    if spec_name == "json":
        gc.options.set_json_serde(serde_name)

    opts = GlueConverterOptions(
        default_db_base_path="s3://bucket/", default_db_name="test_db"
    )

    gc_default_opts = GlueConverter(opts)

    table_path = "s3://bucket/test_table"

    # DO DICT TEST
    spec = gc.generate_from_meta(md, database_name="test_db", table_location=table_path)
    spec_default_opts = gc_default_opts.generate_from_meta(
        md,
    )
    assert spec == spec_default_opts

    with open(f"tests/data/glue_converter/{expected_file_name}.json") as f:
        expected_spec = json.load(f)

    assert spec == expected_spec
