from typing import Any

from jsonschema.exceptions import ValidationError

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

        with pytest.raises(ValidationError):
            setattr(metadata, attribute, invalid_value)

    def test_from_dict(self):
        Metadata().from_dict(
            {
                "name": "test",
                "description": "test",
                "format": "test",
                "sensitive": False,
                "columns": [{"name": "test", "type": "int8"}],
                "primary_key": ["test"],
                "partitions": ["test"],
            }
        )

        with pytest.warns(
            UserWarning, match="Some properties will be ignored: prop_1, prop_2"
        ):
            Metadata().from_dict(
                {
                    "prop_1": None,
                    "prop_2": None,
                }
            )

    def test_to_dict(self):
        metadata = Metadata(
            name="test",
            description="test",
            format="test",
            sensitive=False,
            columns=[{"name": "test", "type": "int8"}],
            primary_key=["test"],
            partitions=["test"],
        )
        assert metadata.to_dict() == {
            "name": "test",
            "description": "test",
            "format": "test",
            "sensitive": False,
            "columns": [{"name": "test", "type": "int8"}],
            "primary_key": ["test"],
            "partitions": ["test"],
        }
