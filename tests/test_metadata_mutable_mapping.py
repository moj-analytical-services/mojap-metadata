import pytest

from jsonschema.exceptions import ValidationError
from mojap_metadata import Metadata

# from mojap_metadata.metadata.metadata import (
#     _parse_and_split,
#     _get_first_level,
#     _unpack_complex_data_type,
#     _table_schema,
#     _get_type_category_pattern_dict_from_schema,
#     _schema_url,
# )
# from typing import Any

def test_basic_column_functions_mutable_mapping():
    meta = Metadata(
        columns=[
            {"name": "a", "type": "int8"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "date32"},
        ]
    )
    assert [column_name for column_name in meta] == ["a", "b", "c"]

    meta["a"] = {"name": "a", "type": "int64"}
    assert meta.columns[0]["type"] == "int64"

    meta["d"] = {"name": "d", "type": "string"}
    assert meta.column_names == ["a", "b", "c", "d"]

    del meta["d"]
    assert meta.column_names == ["a", "b", "c"]

    with pytest.raised(ValueError):
        meta["b"] = {"name": "d", "type": "string"}

    with pytest.raises(ValidationError):
        meta.update_column({"name": "d", "type": "error"})

    with pytest.raises(ValueError):
        meta.remove_column("e")


