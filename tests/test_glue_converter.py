import pytest
import json

from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import (
    GlueConverter,
    GlueConverterOptions,
)


@pytest.mark.parametrize(
    argnames="meta_type,glue_type,expect_raises",
    argvalues=[
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
    ],
)
def test_meta_to_glue_type(meta_type, glue_type, expect_raises):
    gc = GlueConverter()
    gc_yolo = GlueConverter(GlueConverterOptions(ignore_warnings=True))
    if expect_raises == "error":
        with pytest.raises(ValueError):
            gc.convert_col_type(meta_type)
            gc_yolo.convert_col_type(meta_type)

    elif expect_raises == "warning":
        with pytest.warns(UserWarning):
            assert gc.convert_col_type(meta_type) == glue_type

        with pytest.warns(None) as record:
            assert gc_yolo.convert_col_type(meta_type) == glue_type
        if len(record) != 0:
            fail_info = "Explected no warning as options.ignore_warnings = True."
            pytest.fail(fail_info)

    else:
        assert gc.convert_col_type(meta_type) == glue_type
        assert gc_yolo.convert_col_type(meta_type) == glue_type


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
    spec = gc.generate_from_meta(
        md,
        database_name="test_db",
        table_location=table_path
    )
    spec_default_opts = gc_default_opts.generate_from_meta(
        md,
    )
    assert spec == spec_default_opts

    with open(f"tests/data/glue_converter/{expected_file_name}.json") as f:
        expected_spec = json.load(f)

    assert spec == expected_spec

