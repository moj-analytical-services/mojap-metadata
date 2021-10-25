import os
import pytest
import json

from tests.helper import assert_meta_col_conversion, valid_types, get_meta
from moto import mock_glue
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
        ("struct<num:int64,newnum:int64>", "struct<num:bigint,newnum:bigint>", None),
        ("struct<num:int64, newnum:int64>", "struct<num:bigint,newnum:bigint>", None),
        (
            "struct<num:int64, arr:list_<int64>>",
            "struct<num:bigint,arr:array<bigint>>",
            None,
        ),
        (
            "list_<struct<num:int64,desc:string>>",
            "array<struct<num:bigint,desc:string>>",
            None,
        ),
        ("struct<num:int64,desc:string>", "struct<num:bigint,desc:string>", None),
        ("list_<decimal128(38,0)>", "array<decimal(38,0)>", None),
        (
            "struct<a:timestamp(s),b:struct<f1: int32, f2: string, f3:decimal128(3,5)>>",  # noqa
            "struct<a:timestamp,b:struct<f1:int,f2:string,f3:decimal(3,5)>>",
            None,
        ),
        (
            "struct<k1:list<string>, k2:string, k3:string, k4:string, k5:list<string>, k6:string>",  # noqa
            "struct<k1:array<string>,k2:string,k3:string,k4:string,k5:array<string>,k6:string>",  # noqa
            None,
        ),
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
    md = get_meta(spec_name, {})

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


@mock_glue
@pytest.mark.parametrize(
    "gc_kwargs,add_to_meta",
    [
        ({}, {"table_location": "s3://bucket/meta/", "database_name": "meta"}),
        ({"table_location": "s3://bucket/kwarg/", "database_name": "kwarg"}, {}),
        (
            {"table_location": "s3://bucket/kwarg/", "database_name": "kwarg"},
            {"table_location": "s3://bucket/meta/", "database_name": "meta"},
        ),
    ],
)
def test_meta_property_inection_glue_converter(gc_kwargs: dict, add_to_meta: dict):
    """
    This will test the two optional metadata properties "table_location" and
    "database_name" and that the glue converter correctly converts to a glue schema in 3
    states: either present, both present
    """
    gc = GlueConverter()
    # get the metadata with any additional properties
    md = get_meta("csv", add_to_meta)
    # convert it
    boto_dict = gc.generate_from_meta(md, **gc_kwargs)
    # get the correct dictionary to assert
    expected_in_boto_dict = gc_kwargs if gc_kwargs else add_to_meta
    # assert
    assert expected_in_boto_dict == {
        "table_location": boto_dict["TableInput"]["StorageDescriptor"]["Location"],
        "database_name": boto_dict["DatabaseName"],
    }
