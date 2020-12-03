import pytest

from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import (
    GlueConverter,
    GlueConverterOptions,
)


@pytest.mark.parametrize(
    argnames="meta_type,glue_type,expect_raises",
    argvalues=[
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
    argnames="file_format,csv_type,expected_file_name",
    argvalues=[
        ("csv", "lazy", "test_simple_lazy_csv.txt"),
        ("csv", "open", "test_simple_open_csv.txt"),
        ("json", None, "test_simple_json.txt"),
        ("parquet", None, "test_simple_parquet.txt"),
    ],
)
def test_generate_from_meta(file_format, csv_type, expected_file_name):
    md = Metadata.from_dict(
        {
            "name": "test_table",
            "file_format": file_format,
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
    if csv_type == "open":
        gc.options.set_csv_serde("open")

    opts = GlueConverterOptions(
        default_db_base_path="s3://bucket/", default_db_name="test_db"
    )
    if csv_type == "open":
        opts.set_csv_serde("open")

    gc_default_opts = GlueConverter(opts)

    table_path = "s3://bucket/test_table/"
    ddl = gc.generate_from_meta(md, database_name="test_db", table_location=table_path)
    ddl_default_opts = gc_default_opts.generate_from_meta(md)

    assert ddl == ddl_default_opts

    with open(f"tests/data/glue_converter/{expected_file_name}") as f:
        expected_ddl = "".join(f.readlines())

    assert ddl == expected_ddl


def test_start_of_ddl_templates_match():
    opts = GlueConverterOptions()
    t_parquet = opts.parquet_template.split("ROW FORMAT SERDE")[0]
    t_json = opts.json_template.split("ROW FORMAT SERDE")[0]

    opts.set_csv_serde("lazy")
    t_csv_lazy = opts.csv_template.split("ROW FORMAT SERDE")[0]

    opts.set_csv_serde("open")
    t_csv_open = opts.csv_template.split("ROW FORMAT SERDE")[0]

    assert t_parquet == t_json
    assert t_parquet == t_csv_lazy
    assert t_parquet == t_csv_open
