import pytest

from jsonschema.exceptions import ValidationError
from mojap_metadata import Metadata

def test_basic_column_functions_mutable_mapping():
    meta = Metadata(
        name='metadata_name',
        columns=[
            {"name": "a", "type": "int8"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "date32"},
        ]
    )

    col_a = meta["a"]
    assert col_a == {"name": "a", "type": "int8"}

    meta["a"] = {"name": "a", "type": "int64"}
    assert meta.columns[0]["type"] == "int64"

    meta["d"] = {"name": "d", "type": "string"}
    assert meta.column_names == ["a", "b", "c", "d"]

    del meta["d"]
    assert meta.column_names == ["a", "b", "c"]

    with pytest.raises(ValueError):
        meta["b"] = {"name": "d", "type": "string"}

    with pytest.raises(ValidationError):
        meta.update_column({"name": "d", "type": "error"})

    with pytest.raises(ValueError):
        del meta["e"]

def test_mutable_mapping_iter():
    meta = Metadata(
        name='metadata_name',
        columns=[
            {"name": "a", "type": "int8"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "date32"},
        ]
    )

    assert [(col["name"], col["type"]) for col in meta] == [("a","int8"), ("b", "string"), ("c", "date32")]

def test_mutable_mapping_len():
    meta = Metadata(
        name="metadata_name",
        columns=[
            {"name": "a", "type": "int8"}
        ]
    )
    assert len(meta) == 1

    del meta["a"]
    assert len(meta) == 0

    meta["a"] = {"name": "a", "type": "int8"}
    meta["b"] = {"name": "b", "type": "string"},
    meta["c"] = {"name": "c", "type": "date32"}
    assert len(meta) == 3

