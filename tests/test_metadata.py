from typing import Any, Type
from attr import attr, attributes, set_run_validators
import py

import pytest
from mojap_metadata.metadata.metadata import Metadata


class TestMetadata:
    @pytest.mark.parametrize(
        argnames="attribute,default_value,valid_value,invalid_value",
        argvalues=[
            ("name", "", "test", 0),
            ("description", "", "test", 0),
            ("format", "", "test", 0),
            ("sensitive", False, True, 0),
        ],
    )
    def test_attributes(
        self, attribute: str, default_value: Any, valid_value: Any, invalid_value: Any
    ):
        metadata = Metadata()
        assert getattr(metadata, attribute) == default_value

        setattr(metadata, attribute, valid_value)
        assert getattr(metadata, attribute) == valid_value

        with pytest.raises(TypeError):
            setattr(metadata, attribute, invalid_value)

    def test_columns(self):
        metadata = Metadata()
        assert metadata.columns == list()
        metadata.columns = [{"name": "col1"}, {"name": "col2"}]
        assert metadata.columns == [{"name": "col1"}, {"name": "col2"}]

        with pytest.raises(TypeError):
            setattr(metadata, "columns", 0)

        with pytest.raises(TypeError):
            setattr(metadata, "columns", [0])

        with pytest.raises(TypeError):
            setattr(metadata, "columns", [{"name": 0}])

    def _test_with_columns(self, attribute, default_value, valid_value, invalid_value):
        metadata = Metadata()
        assert getattr(metadata, attribute) == default_value

        with pytest.raises(ValueError):
            setattr(metadata, attribute, valid_value)

        with pytest.raises(TypeError):
            setattr(metadata, attribute, invalid_value)

        with pytest.raises(ValueError):
            setattr(metadata, attribute, valid_value * 2)

        metadata.columns = [{"name": "test"}]
        setattr(metadata, attribute, valid_value)
        assert getattr(metadata, attribute) == ["test"]

    @pytest.mark.parametrize(
        argnames="attribute,default_value,valid_value,invalid_value",
        argvalues=[("primary_key", [], ["test"], 0), ("partitions", [], ["test"], 0)],
    )
    def test_primary_key_and_partitions(
        self, attribute: str, default_value: Any, valid_value: Any, invalid_value: Any
    ):
        self._test_with_columns(
            attribute=attribute,
            default_value=default_value,
            valid_value=valid_value,
            invalid_value=invalid_value,
        )
