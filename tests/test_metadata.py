from typing import Any

from jsonschema.exceptions import ValidationError

import pytest
from mojap_metadata import Metadata

from mojap_metadata.metadata.metadata import (
    _parse_and_split,
    _get_first_level,
    _unpack_complex_data_type,
)


@pytest.mark.parametrize(
    argnames="attribute,default_value,valid_value,invalid_value",
    argvalues=[
        ("name", "", "test", 0),
        ("description", "", "test", 0),
        ("file_format", "", "test", 0),
        ("sensitive", False, True, 0),
    ],
)
def test_basic_attributes(
    attribute: str, default_value: Any, valid_value: Any, invalid_value: Any
):
    """
    Attributes with default, valid and invalid types are handled as
    expected.
    """
    metadata = Metadata()
    assert getattr(metadata, attribute) == default_value

    setattr(metadata, attribute, valid_value)
    assert getattr(metadata, attribute) == valid_value

    with pytest.raises(ValidationError):
        setattr(metadata, attribute, invalid_value)


def test_columns_default():
    metadata = Metadata()
    assert metadata.columns == []


@pytest.mark.parametrize(
    argnames="col_input",
    argvalues=[
        "",
        [""],
        [{"name": "test"}],
        [{"type_category": "integer"}],
        [{"type": "null"}],
        [{"name": "test", "type_category": "test"}],
        [{"name": 0, "type_category": "integer"}],
        [{"name": "test", "type_category": 0}],
        [{"name": "test", "type": 0}],
        [{"name": "test", "type": "int7"}],
        [{"name": "test", "type": "kint8"}],
        [{"name": "test", "type": "float8"}],
        [{"name": "test", "type": "decimal"}],
        [{"name": "test", "type": "decimal(0,38)"}],
        [{"name": "test", "type_category": "null", "type": "test"}],
        [{"name": "test", "type_category": "integer", "type": "test"}],
        [{"name": "test", "type": "time32"}],
        [{"name": "test", "type": "time64"}],
        [{"name": "test", "type": "timestamp"}],
        [{"name": "test", "type_category": "timestamp", "type": "timestamp"}],
        [{"name": "test", "type_category": "datetime", "type": "timestamp(s)"}],
    ],
)
def test_columns_validation_error(col_input: Any):
    metadata = Metadata()
    with pytest.raises(ValidationError):
        metadata.columns = col_input


@pytest.mark.parametrize(
    argnames="col_input",
    argvalues=[
        [{"name": "test", "type_category": "null"}],
        [{"name": "test", "type_category": "integer"}],
        [{"name": "test", "type_category": "float"}],
        [{"name": "test", "type_category": "string"}],
        [{"name": "test", "type_category": "timestamp"}],
        [{"name": "test", "type_category": "binary"}],
        [{"name": "test", "type_category": "boolean"}],
        [{"name": "test", "type": "int8"}],
        [{"name": "test", "type": "bool_"}],
        [{"name": "test", "type": "int16"}],
        [{"name": "test", "type": "int32"}],
        [{"name": "test", "type": "int64"}],
        [{"name": "test", "type": "uint8"}],
        [{"name": "test", "type": "uint16"}],
        [{"name": "test", "type": "uint32"}],
        [{"name": "test", "type": "uint64"}],
        [{"name": "test", "type": "float16"}],
        [{"name": "test", "type": "float32"}],
        [{"name": "test", "type": "float64"}],
        [{"name": "test", "type": "decimal128(0,38)"}],
        [{"name": "test", "type": "time32(s)"}],
        [{"name": "test", "type": "time32(ms)"}],
        [{"name": "test", "type": "time64(us)"}],
        [{"name": "test", "type": "time64(ns)"}],
        [{"name": "test", "type": "timestamp(s)"}],
        [{"name": "test", "type": "timestamp(ms)"}],
        [{"name": "test", "type": "timestamp(us)"}],
        [{"name": "test", "type": "timestamp(ns)"}],
        [{"name": "test", "type": "date32"}],
        [{"name": "test", "type": "date64"}],
        [{"name": "test", "type": "string"}],
        [{"name": "test", "type": "large_string"}],
        [{"name": "test", "type": "utf8"}],
        [{"name": "test", "type": "large_utf8"}],
        [{"name": "test", "type": "binary"}],
        [{"name": "test", "type": "binary(128)"}],
        [{"name": "test", "type": "large_binary"}],
        [{"name": "test", "type_category": "null", "type": "null"}],
        [{"name": "test", "type_category": "integer", "type": "int8"}],
        [{"name": "test", "type_category": "float", "type": "float32"}],
        [{"name": "test", "type_category": "string", "type": "string"}],
        [{"name": "test", "type_category": "timestamp", "type": "timestamp(ms)"}],
        [{"name": "test", "type_category": "binary", "type": "binary(128)"}],
        [{"name": "test", "type_category": "binary", "type": "binary"}],
        [{"name": "test", "type_category": "boolean", "type": "bool_"}],
        [{"name": "test", "type": "struct<num:int64>"}],
        [{"name": "test", "type": "list_<int64>"}],
        [{"name": "test", "type": "list_<list_<int64>>"}],
        [{"name": "test", "type": "large_list<int64>"}],
        [{"name": "test", "type": "large_list<large_list<int64>>"}],
        [{"name": "test", "type": "struct<num:int64,newnum:int64>"}],
        [{"name": "test", "type": "struct<num:int64,arr:list_<int64>>"}],
        [{"name": "test", "type": "list_<struct<num:int64,desc:string>>"}],
        [{"name": "test", "type": "struct<num:int64,desc:string>"}],
        [{"name": "test", "type": "list_<decimal128(38,0)>"}],
        [{"name": "test", "type": "large_list<decimal128(38,0)>"}],
    ],
)
def test_columns_pass(col_input: Any):
    Metadata(columns=col_input)


def test_primary_key_and_partitions_attributes():
    pass


def test_from_dict():
    test_dict = {
        "name": "test",
        "description": "test",
        "file_format": "test",
        "sensitive": False,
        "columns": [{"name": "test", "type": "null"}],
        "primary_key": ["test"],
        "partitions": ["test"],
    }
    meta = Metadata.from_dict(test_dict)

    for k, v in test_dict.items():
        assert getattr(meta, k) == v

    meta.name = "bob"
    assert meta.name == meta._data["name"]


def test_preservation_of_underlying_metadata():

    # Test if additional data is preserved
    test_dict = {
        "name": "test",
        "description": "test",
        "file_format": "test",
        "sensitive": False,
        "columns": [{"name": "test", "type": "null"}],
        "primary_key": ["test"],
        "partitions": ["test"],
        "additional-attr": "test",
    }
    meta = Metadata.from_dict(test_dict)
    out_dict = meta.to_dict()
    for k, v in test_dict.items():
        assert out_dict[k] == v

    # make sure data is copied and not just a pointer
    assert id(test_dict) != id(meta._data)

    test_dict["columns"] = [{"name": "new_test", "type": "bool_"}]
    assert test_dict != meta.columns

    # Assert Metadata instances are different
    m1 = Metadata()
    m2 = Metadata()

    assert m1.columns == m2.columns

    m1.columns.append({"name": "new_test", "type": "bool_"})
    assert m1.columns != m2.columns


def test_to_dict():
    metadata = Metadata(
        name="test",
        description="test",
        file_format="test",
        sensitive=False,
        columns=[{"name": "test", "type": "null"}],
        primary_key=["test"],
        partitions=["test"],
    )
    assert metadata.to_dict() == {
        "$schema": "",
        "name": "test",
        "description": "test",
        "file_format": "test",
        "sensitive": False,
        "columns": [{"name": "test", "type": "null"}],
        "primary_key": ["test"],
        "partitions": ["test"],
    }


# content of test_tmpdir.py
def test_create_file(tmpdir):
    p = tmpdir.mkdir("sub").join("hello.txt")
    p.write("content")
    assert p.read() == "content"


@pytest.mark.parametrize(argnames="writer", argvalues=["json", "yaml"])
def test_to_from_json_yaml(tmpdir, writer):
    path_file = tmpdir.mkdir("test_outputs").join("meta.{writer}")

    test_dict = {
        "name": "test",
        "description": "test",
        "file_format": "test",
        "sensitive": False,
        "columns": [{"name": "test", "type": "null"}],
        "primary_key": ["test"],
        "partitions": ["test"],
    }
    meta = Metadata.from_dict(test_dict)

    # test in/out reader
    getattr(meta, f"to_{writer}")(str(path_file))
    read_meta = getattr(Metadata, f"from_{writer}")(str(path_file))
    out_dict = read_meta.to_dict()
    for k, v in test_dict.items():
        assert out_dict[k] == v


@pytest.mark.parametrize(
    "t,e",
    [
        ("Don't grab this <Get this stuff> Don't grab this ", "Get this stuff"),
        (
            "struct<a: timestamp[s], b: struct<f1: int32, f2: string>>",
            "a: timestamp[s], b: struct<f1: int32, f2: string>",
        ),
        ("a: timestamp[s], b: struct<f1: int32, f2: string>", "f1: int32, f2: string"),
    ],
)
def test_get_first_level(t, e):
    assert _get_first_level(t) == e


@pytest.mark.parametrize(
    "text,char,expected",
    [
        (
            "a: timestamp[s], b: struct<f1: int32, f2: string>",
            ",",
            ["a: timestamp[s]", "b: struct<f1: int32, f2: string>"],
        ),
        (
            'a: timestamp["s", +07:30], b: decimal128(3,5)',
            ",",
            ['a: timestamp["s", +07:30]', "b: decimal128(3,5)"],
        ),
        (
            'a: timestamp["s", +07:30], b: decimal128(3,5)',
            ":",
            ["a", 'timestamp["s", +07:30], b', "decimal128(3,5)"],
        ),
    ],
)
def test_parse_and_split(text, char, expected):
    assert list(_parse_and_split(text, char)) == expected


@pytest.mark.parametrize(
    "data_type,expected",
    [
        ("string", "string"),
        ("struct<num:int64>", {"struct": {"num": "int64"}}),
        ("list_<int64>", {"list_": "int64"}),
        ("list_<list_<int64>>", {"list_": {"list_": "int64"}}),
        ("large_list<int64>", {"large_list": "int64"}),
        ("large_list<large_list<int64>>", {"large_list": {"large_list": "int64"}}),
        (
            "struct<num:int64,newnum:int64>",
            {"struct": {"num": "int64", "newnum": "int64"}},
        ),
        (
            "struct<num:int64,arr:list_<int64>>",
            {"struct": {"num": "int64", "arr": {"list_": "int64"}}},
        ),
        (
            "list_<struct<num:int64,desc:string>>",
            {"list_": {"struct": {"num": "int64", "desc": "string"}}},
        ),
        (
            "struct<num:int64,desc:string>",
            {"struct": {"num": "int64", "desc": "string"}},
        ),
        (
            "list_<decimal128(38,0)>",
            {"list_": "decimal128(38,0)"},
        ),
        (
            "struct<a:timestamp[s], b:struct<f1:int32, f2:string, f3:decimal128(3,5)>>",
            {
                "struct": {
                    "a": "timestamp[s]",
                    "b": {
                        "struct": {
                            "f1": "int32",
                            "f2": "string",
                            "f3": "decimal128(3,5)",
                        }
                    },
                }
            },
        ),
    ],
)
def test_unpack_complex_data_type(data_type, expected):
    meta = Metadata()
    assert _unpack_complex_data_type(data_type) == expected
    assert meta.unpack_complex_data_type(data_type) == expected
