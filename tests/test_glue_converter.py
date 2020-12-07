import pytest
import json

from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import (
    GlueConverter,
    GlueConverterOptions,
    _get_base_table_ddl,
)


@pytest.mark.parametrize(
    argnames="meta_type,glue_type,expect_raises",
    argvalues=[
        ("bool_", "BOOLEAN", None),
        ("int8", "TINYINT", None),
        ("int16", "SMALLINT", None),
        ("int32", "INT", None),
        ("int64", "BIGINT", None),
        ("uint8", "SMALLINT", "warning"),
        ("uint16", "INT", "warning"),
        ("uint32", "BIGINT", "warning"),
        ("uint64", None, "error"),
        ("float16", "FLOAT", "warning"),
        ("float32", "FLOAT", None),
        ("float64", "DOUBLE", None),
        ("decimal128(0,38)", "DECIMAL(0,38)", None),
        ("decimal128(1,2)", "DECIMAL(1,2)", None),
        ("time32(s)", None, "error"),
        ("time32(ms)", None, "error"),
        ("time64(us)", None, "error"),
        ("time64(ns)", None, "error"),
        ("timestamp(s)", "TIMESTAMP", None),
        ("timestamp(ms)", "TIMESTAMP", None),
        ("timestamp(us)", "TIMESTAMP", None),
        ("timestamp(ns)", "TIMESTAMP", None),
        ("date32", "DATE", None),
        ("date64", "DATE", None),
        ("string", "STRING", None),
        ("large_string", "STRING", None),
        ("utf8", "STRING", None),
        ("large_utf8", "STRING", None),
        ("binary", "BINARY", None),
        ("binary(128)", "BINARY", None),
        ("large_binary", "BINARY", None),
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

    # DO DDL TEST
    ddl = gc.generate_from_meta(
        md,
        database_name="test_db",
        table_location=table_path,
        return_as_str_ddl=True
    )
    ddl_default_opts = gc_default_opts.generate_from_meta(
        md,
        return_as_str_ddl=True
    )
    assert ddl == ddl_default_opts

    with open(f"tests/data/glue_converter/{expected_file_name}.txt") as f:
        expected_ddl = "".join(f.readlines())

    assert ddl == expected_ddl

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

    with open(f"tests/data/glue_converter/{expected_file_name}.JSON") as f:
        expected_spec = json.load(f)

    assert spec == expected_spec


def test_start_of_ddl_templates_match():

    t_parquet = _get_base_table_ddl("parquet", None)
    t_parquet = t_parquet.split("ROW FORMAT SERDE")[0]

    t_json1 = _get_base_table_ddl("json", "openx")
    t_json1 = t_json1.split("ROW FORMAT SERDE")[0]

    t_json2 = _get_base_table_ddl("json", "hive")
    t_json2 = t_json2.split("ROW FORMAT SERDE")[0]

    t_csv1 = _get_base_table_ddl("csv", "open")
    t_csv1 = t_csv1.split("ROW FORMAT SERDE")[0]

    t_csv2 = _get_base_table_ddl("csv", "lazy")
    t_csv2 = t_csv2.split("ROW FORMAT SERDE")[0]

    assert t_parquet == t_json1
    assert t_parquet == t_json2
    assert t_parquet == t_csv1
    assert t_parquet == t_csv2
