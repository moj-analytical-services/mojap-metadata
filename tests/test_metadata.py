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
            ("file_format", "", "test", 0),
            ("sensitive", False, True, 0),
        ],
    )
    def test_basic_attributes(
        self, attribute: str, default_value: Any, valid_value: Any, invalid_value: Any
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

    def test_columns_default(self):
        metadata = Metadata()
        assert metadata.columns == []

    @pytest.mark.parametrize(
        argnames="input",
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
        ],
    )
    def test_columns_validation_error(self, input: Any):
        metadata = Metadata()
        with pytest.raises(ValidationError):
            metadata.columns = input

    @pytest.mark.parametrize(
        argnames="input",
        argvalues=[
            [{"name": "test", "type_category": "null"}],
            [{"name": "test", "type_category": "integer"}],
            [{"name": "test", "type_category": "float"}],
            [{"name": "test", "type_category": "string"}],
            [{"name": "test", "type_category": "datetime"}],
            [{"name": "test", "type_category": "binary"}],
            [{"name": "test", "type_category": "boolean"}],
            [{"name": "test", "type": "int8"}],
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
            [{"name": "test", "type": "time32"}],
            [{"name": "test", "type": "time64"}],
            [{"name": "test", "type": "time64(s)"}],
            [{"name": "test", "type": "time64(ms)"}],
            [{"name": "test", "type": "timestamp"}],
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
            [{"name": "test", "type_category": "datetime", "type": "timestamp"}],
            [{"name": "test", "type_category": "binary", "type": "binary(128)"}],
            [{"name": "test", "type_category": "binary", "type": "binary"}],
            [{"name": "test", "type_category": "boolean", "type": "bool_"}],
        ],
    )
    def test_columns_pass(self, input: Any):
        Metadata(columns=input)

    def test_primary_key_and_partitions_attributes(self):
        pass

    def test_from_dict(self):
        Metadata().from_dict(
            {
                "name": "test",
                "description": "test",
                "file_format": "test",
                "sensitive": False,
                "columns": [{"name": "test", "type": "null"}],
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
            file_format="test",
            sensitive=False,
            columns=[{"name": "test", "type": "null"}],
            primary_key=["test"],
            partitions=["test"],
        )
        assert metadata.to_dict() == {
            "name": "test",
            "description": "test",
            "file_format": "test",
            "sensitive": False,
            "columns": [{"name": "test", "type": "null"}],
            "primary_key": ["test"],
            "partitions": ["test"],
        }
