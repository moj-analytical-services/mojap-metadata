import json
import yaml
from copy import deepcopy
import importlib.resources as pkg_resources
import jsonschema
from mojap_metadata.metadata import specs

_table_schema = json.load(pkg_resources.open_text(specs, "table_schema.json"))


class MetadataProperty:
    def __set_name__(self, owner, name) -> None:
        self.name = name

    def __get__(self, obj, type=None) -> object:
        return obj._data.get(self.name)

    def __set__(self, obj, value) -> None:
        obj._data[self.name] = value
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
        columns: list = [],
        primary_key: list = [],
        partitions: list = [],
    ) -> None:

        self._schema = deepcopy(_table_schema)

        self._data = {
            "$schema": "",
            "name": name,
            "description": description,
            "file_format": file_format,
            "sensitive": sensitive,
            "columns": columns,
            "primary_key": primary_key,
            "partitions": partitions,
        }

        self.validate()

    def _init_data_with_default_key_values(self, data: dict, copy_data: bool = True):
        """
        Used to create the class from a dictionary

        Args:
            data (dict): [description]
            copy_data (bool, optional): [description]. Defaults to True.
        """
        if copy_data:
            self._data = deepcopy(data)
        else:
            self._data = data

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
            self._data[k] = data.get(k, v)

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
