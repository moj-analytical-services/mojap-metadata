import pytest
import copy

from tests.helper import assert_meta_col_conversion

from mojap_metadata import Metadata
from mojap_metadata.converters import BaseConverterOptions
from mojap_metadata.converters.etl_manager_converter import (
    EtlManagerConverter,
)

from etl_manager.meta import TableMeta


@pytest.mark.parametrize(
    argnames="meta_type,etl_type,expect_raises",
    argvalues=[
        ("bool_", "boolean", None),
        ("int8", "int", "warning"),
        ("int16", "int", "warning"),
        ("int32", "int", None),
        ("int64", "long", None),
        ("uint8", "int", "warning"),
        ("uint16", "int", "warning"),
        ("uint32", "long", "warning"),
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
        ("timestamp(s)", "datetime", None),
        ("timestamp(ms)", "datetime", None),
        ("timestamp(us)", "datetime", None),
        ("timestamp(ns)", "datetime", None),
        ("date32", "date", None),
        ("date64", "date", None),
        ("string", "character", None),
        ("large_string", "character", None),
        ("utf8", "character", None),
        ("large_utf8", "character", None),
        ("binary", "binary", None),
        ("binary(128)", "binary", None),
        ("large_binary", "binary", None),
    ],
)
def test_meta_to_etl_manager_type(meta_type, etl_type, expect_raises):
    assert_meta_col_conversion(EtlManagerConverter, meta_type, etl_type, expect_raises)


@pytest.mark.parametrize(
    argnames="etl_type,meta_type,expect_raises",
    argvalues=[
        ("character", "string", None),
        ("int", "int32", None),
        ("long", "int64", None),
        ("float", "float32", None),
        ("double", "float64", None),
        ("decimal(0,38)", "decimal128(0,38)", None),
        ("decimal(1,2)", "decimal128(1,2)", None),
        ("date", "date32", None),
        ("datetime", "timestamp(s)", None),
        ("binary", "binary", None),
        ("boolean", "bool_", None),
    ],
)
def test_etl_manager_to_meta_type(etl_type, meta_type, expect_raises):
    assert_meta_col_conversion(
        EtlManagerConverter, etl_type, meta_type, expect_raises, reverse=True
    )


def test_generate_from_meta():
    md = Metadata.from_dict(
        {
            "name": "test_table",
            "file_format": "csv",
            "description": "A test table",
            "columns": [
                {
                    "name": "my_int",
                    "type": "int64",
                    "description": "This is an integer",
                    "nullable": False,
                    "minimum": 10,
                },
                {"name": "my_double", "type": "float64", "nullable": True},
                {"name": "my_date", "type": "date64"},
                {"name": "my_decimal", "type": "decimal128(10,2)"},
                {"name": "my_string", "type": "string", "enum": ["cat", "dog"]},
                {
                    "name": "my_timestamp",
                    "type": "timestamp(s)",
                    "description": "Partition column",
                },
            ],
            "partitions": ["my_timestamp"],
        }
    )

    expected1 = {
        "$schema": "https://moj-analytical-services.github.io/metadata_schema/table/v1.4.0.json", # noqa: 401
        "name": "test_table",
        "data_format": "csv",
        "location": "test_table/",
        "description": "A test table",
        "columns": [
            {
                "name": "my_int",
                "type": "long",
                "description": "This is an integer",
                "nullable": False,
                "minimum": 10,
            },
            {
                "name": "my_double",
                "type": "double",
                "nullable": True,
                "description": "",
            },
            {"name": "my_date", "type": "date", "description": ""},
            {"name": "my_decimal", "type": "decimal(10,2)", "description": ""},
            {
                "name": "my_string",
                "type": "character",
                "enum": ["cat", "dog"],
                "description": "",
            },
            {
                "name": "my_timestamp",
                "type": "datetime",
                "description": "Partition column",
            },
        ],
        "partitions": ["my_timestamp"],
    }
    emc = EtlManagerConverter()
    assert isinstance(emc.options, BaseConverterOptions)

    etl1 = emc.generate_from_meta(md)
    assert isinstance(etl1, TableMeta)

    etl1 = etl1.to_dict()
    assert etl1 == expected1

    expected2 = copy.deepcopy(expected1)

    # Remove additional cols not native to etl_manager
    del expected2["columns"][0]["minimum"]

    etl2 = emc.generate_from_meta(md, include_extra_column_params=False).to_dict()
    assert etl2 == expected2

    # Check table_location works
    expected3 = copy.deepcopy(expected1)
    expected3["location"] = "some/new/tablepath/"
    etl3 = emc.generate_from_meta(md, table_location="some/new/tablepath/").to_dict()
    assert etl3 == expected3

    # Check file_format_mapper
    mapper = {"csv": "csv_quoted_nodate"}.get
    expected4 = copy.deepcopy(expected1)
    expected4["data_format"] = "csv_quoted_nodate"
    etl4 = emc.generate_from_meta(md, file_format_mapper=mapper).to_dict()
    assert etl4 == expected4

    # Check glue_specific
    mapper = {"csv": "csv_quoted_nodate"}.get
    expected5 = copy.deepcopy(expected1)
    gs = {
        "Parameters": {"skip.header.line.count": "1"},
        "StorageDescriptor": {"Parameters": {"skip.header.line.count": "1"}},
    }
    expected5["glue_specific"] = gs
    etl5 = emc.generate_from_meta(md, glue_specific=gs).to_dict()
    assert etl5 == expected5


def test_generate_to_meta():
    expected1 = {
        "$schema": "",
        "name": "test_table",
        "file_format": "csv",
        "description": "A test table",
        "columns": [
            {
                "name": "my_int",
                "type": "int64",
                "description": "This is an integer",
                "nullable": False,
                "minimum": 10,
            },
            {
                "name": "my_double",
                "type": "float64",
                "nullable": True,
                "description": "",
            },
            {"name": "my_date", "type": "date32", "description": ""},
            {"name": "my_decimal", "type": "decimal128(10,2)", "description": ""},
            {
                "name": "my_string",
                "type": "string",
                "enum": ["cat", "dog"],
                "description": "",
            },
            {
                "name": "my_timestamp",
                "type": "timestamp(s)",
                "description": "Partition column",
            },
        ],
        "partitions": ["my_timestamp"],
        "_converted_from": "etl_manager",
        "location": "test_table/",
        "primary_key": [],
        "sensitive": False,
    }

    tab_meta = TableMeta(
        name="test_table",
        location="test_table/",
        description="A test table",
        columns=[
            {
                "name": "my_int",
                "type": "long",
                "description": "This is an integer",
                "nullable": False,
                "minimum": 10,
            },
            {
                "name": "my_double",
                "type": "double",
                "nullable": True,
                "description": "",
            },
            {"name": "my_date", "type": "date", "description": ""},
            {"name": "my_decimal", "type": "decimal(10,2)", "description": ""},
            {
                "name": "my_string",
                "type": "character",
                "enum": ["cat", "dog"],
                "description": "",
            },
            {
                "name": "my_timestamp",
                "type": "datetime",
                "description": "Partition column",
            },
        ],
        data_format="csv",
        partitions=["my_timestamp"],
    )

    emc = EtlManagerConverter()
    meta1 = emc.generate_to_meta(tab_meta)
    assert isinstance(meta1, Metadata)

    meta1 = meta1.to_dict()
    assert meta1 == expected1

    # test data_format_mapper
    expected2 = copy.deepcopy(expected1)
    expected2["file_format"] = "textfile"

    meta2 = emc.generate_to_meta(
        tab_meta, data_format_mapper={"csv": "textfile"}.get
    ).to_dict()
    assert meta2 == expected2

    # test col_type_mapper
    expected3 = copy.deepcopy(expected1)
    for c in expected3["columns"]:
        c["type"] = "string"

    meta3 = emc.generate_to_meta(tab_meta, col_type_mapper=lambda x: "string").to_dict()
    assert meta3 == expected3
