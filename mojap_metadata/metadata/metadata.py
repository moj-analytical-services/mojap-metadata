import json
import yaml
from copy import deepcopy
import importlib.resources as pkg_resources
import jsonschema
from mojap_metadata.metadata import specs

from typing import Union, List

_table_schema = json.load(pkg_resources.open_text(specs, "table_schema.json"))


def _parse_and_split(text: str, char: str) -> List[str]:
    """
    Splits a string into a list by splitting on
    any input char that is outside of parentheses.
    If `char` is inside parentheses then no split
    occurs. Also strips each str in the list.
    """
    in_parentheses = [0, 0, 0]  # square  # round  # angular

    start = -1
    for i, s in enumerate(text):
        if s == "[":
            in_parentheses[0] += 1
        elif s == "]":
            in_parentheses[0] -= 1
        elif s == "(":
            in_parentheses[1] += 1
        elif s == ")":
            in_parentheses[1] -= 1
        elif s == "<":
            in_parentheses[2] += 1
        elif s == ">":
            in_parentheses[2] += 1

        if s == char and not any([bool(p) for p in in_parentheses]):
            yield text[start + 1 : i].strip()
            start = i

    yield text[start + 1 :].strip()


def _get_first_level(text: str) -> str:
    """Returns everything in first set of <>"""
    start = 0
    end = len(text)
    for i, c in enumerate(text):
        if c == "<":
            start = i + 1
            break
    for i, c in enumerate(reversed(text)):
        if c == ">":
            end = len(text) - (i + 1)
            break

    return text[start:end]


def _unpack_complex_data_type(data_type: str) -> Union[str, dict]:
    """Recursive function that jumps into complex
    data types and returns complex types as a dict.
    Non complex types are returned as a str.

    Args:
        data_type (str): Name of agnostic data type

    Returns:
        Union[str, dict]: unpacked representation of data type as dict
            for complex types. If datatype is not complex then original
            data type is returned (as str).
    """
    d = {}
    if data_type.startswith("struct<"):
        d["struct"] = {}
        next_data_type = _get_first_level(data_type)
        for data_param in _parse_and_split(next_data_type, ","):
            k, v = data_param.split(":", 1)
            k = k.strip()
            v = v.strip()
            if (
                v.startswith("struct<")
                or v.startswith("list_<")
                or v.startswith("large_list<")
            ):
                d["struct"][k] = _unpack_complex_data_type(v)
            else:
                d["struct"][k] = v
        return d
    elif data_type.startswith("list_<") or data_type.startswith("large_list<"):
        k = "list_" if data_type.startswith("list_<") else "large_list"
        d[k] = {}
        next_data_type = _get_first_level(data_type).strip()
        if (
            next_data_type.startswith("struct<")
            or next_data_type.startswith("list_<")
            or next_data_type.startswith("large_list<")
        ):
            d[k] = _unpack_complex_data_type(next_data_type)
        else:
            d[k] = next_data_type
        return d
    else:
        return data_type


class MetadataProperty:
    def __set_name__(self, owner, name) -> None:
        self.name = name

    def __get__(self, obj, type=None) -> object:
        return obj.__dict__["_data"].get(self.name)

    def __set__(self, obj, value) -> None:
        obj.__dict__["_data"][self.name] = value
        obj.validate()


class Metadata:
    @classmethod
    def from_dict(cls, d: dict) -> object:
        m = cls()
        m._init_data_with_default_key_values(d)
        m.validate()
        return m

    @classmethod
    def from_json(cls, filename, **kwargs) -> object:
        with open(filename, "r") as f:
            obj = json.load(f, **kwargs)
            return cls.from_dict(obj)

    @classmethod
    def from_yaml(cls, filename, **kwargs) -> object:
        with open(filename, "r") as f:
            obj = yaml.safe_load(f, **kwargs)
            return cls.from_dict(obj)

    name = MetadataProperty()
    description = MetadataProperty()
    file_format = MetadataProperty()
    sensitive = MetadataProperty()
    columns = MetadataProperty()
    primary_key = MetadataProperty()
    partitions = MetadataProperty()

    def __init__(
        self,
        name: str = "",
        description: str = "",
        file_format: str = "",
        sensitive: bool = False,
        columns: list = None,
        primary_key: list = None,
        partitions: list = None,
    ) -> None:

        self._schema = deepcopy(_table_schema)

        self._data = {
            "$schema": "",
            "name": name,
            "description": description,
            "file_format": file_format,
            "sensitive": sensitive,
            "columns": columns if columns else [],
            "primary_key": primary_key if primary_key else [],
            "partitions": partitions if partitions else [],
        }

        self.validate()

    def _init_data_with_default_key_values(self, data: dict):
        """
        Used to create the class from a dictionary

        Args:
            data (dict): [description]
            copy_data (bool, optional): [description]. Defaults to True.
        """
        _data = deepcopy(data)
        self._data = _data

        defaults = {
            "$schema": "",
            "name": "",
            "description": "",
            "file_format": "",
            "sensitive": False,
            "columns": [],
            "primary_key": [],
            "partitions": [],
        }

        for k, v in defaults.items():
            self._data[k] = _data.get(k, v)

    def validate(self):
        jsonschema.validate(instance=self._data, schema=self._schema)
        self._validate_list_attribute(attribute="primary_key", columns=self.primary_key)
        self._validate_list_attribute(attribute="partitions", columns=self.partitions)

    def _validate_list_attribute(self, attribute: str, columns: list) -> None:
        if not isinstance(columns, list):
            raise TypeError(f"'{attribute}' must be of type 'list'")
        if not all([isinstance(column, str) for column in columns]):
            raise TypeError(f"'{attribute}' must be a list of strings")
        if not set(columns).issubset(
            {column["name"] for column in self.columns if self.columns}
        ):
            raise ValueError(f"'All elements of '{attribute}' must be in self.columns")
        if len(columns) != len(set(columns)):
            raise ValueError(f"'All elements of '{attribute}' must be unique")

    def to_dict(self) -> dict:
        return deepcopy(self._data)

    def to_json(self, filepath: str, mode: str = "w", **kwargs) -> None:
        with open(filepath, mode) as f:
            json.dump(self.to_dict(), f, **kwargs)

    def to_yaml(self, filepath: str, mode: str = "w", **kwargs) -> None:
        with open(filepath, mode) as f:
            yaml.safe_dump(self.to_dict(), f)

    def unpack_complex_data_type(self, data_type: str) -> Union[str, dict]:
        """
        Takes the coltype definition as a string and parses it.
        If the data_type is complex (list_ or struct) then a dict
        representation of unpacked coltypes is returned. If the
        type is not complex then the same input `data_type` is
        returned.

        Args:
            coltype (str): Agnostic data type

        Returns:
            Union[str, dict]: unpacked representation of data type as dict
                for complex types. If data type is not complex then original
                data type is returned (as str).
        """
        return _unpack_complex_data_type(data_type)
