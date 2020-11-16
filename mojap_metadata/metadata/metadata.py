import json
from typing import Any
from attr import attr

from jsonschema import validate


class Metadata:
    def __init__(
        self,
        name: str = "",
        description: str = "",
        format: str = "",
        sensitive: bool = False,
        primary_key: set = [],
        partitions: set = [],
        columns: list = [],
    ) -> None:
        self.name = name
        self.description = description
        self.format = format
        self.sensitive = sensitive
        self.columns = columns
        self.primary_key = primary_key
        self.partitions = partitions

        with open("mojap_metadata/metadata/specs/table_schema.json") as file:
            self._schema = json.load(file)

        self._validate_list_attribute(attribute="primary_key", columns=primary_key)
        self._validate_list_attribute(attribute="partitions", columns=partitions)

        validate(instance=self.to_dict(include_schema=False), schema=self._schema)

    def _setter(self, attribute: str, type: type, value: Any):
        if isinstance(value, type):
            setattr(self, f"_{attribute}", value)
        else:
            raise TypeError(f"'{attribute}' must be of type '{type.__name__}'")

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name: str):
        self._setter(attribute="name", type=str, value=name)

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, description: str):
        self._setter(attribute="description", type=str, value=description)

    @property
    def format(self):
        return self._format

    @format.setter
    def format(self, format: str):
        self._setter(attribute="format", type=str, value=format)

    @property
    def sensitive(self):
        return self._sensitive

    @sensitive.setter
    def sensitive(self, sensitive: bool):
        self._setter(attribute="sensitive", type=bool, value=sensitive)

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, columns: list):
        if not isinstance(columns, list):
            raise TypeError("'columns' attribute must be type 'list'.")
        for col in columns:
            if not isinstance(col, dict):
                raise TypeError("'columns' attribute must be a list of dicts.")
            if not isinstance(col["name"], str):
                raise TypeError(f"The column '{col['name']}' must be type 'str'")
        self._setter(attribute="columns", type=list, value=columns)

    @property
    def primary_key(self):
        return self._primary_key

    @primary_key.setter
    def primary_key(self, primary_key: set):
        has_distinct_elements = len(primary_key) == len(set(primary_key))
        all_str = all([type(e) is str for e in primary_key])
        column_set = {e["name"] for e in self._columns if self._columns}
        if set(primary_key).issubset(column_set) and all_str and has_distinct_elements:
            self._setter(attribute="primary_key", type=list, value=primary_key)
        else:
            raise ValueError(
                "'primary_key' must be a set of names defined in 'columns'"
            )

    def _validate_list_attribute(self, attribute: str, columns: list):
        if not isinstance(columns, list):
            raise TypeError(f"'{attribute}' must be of type 'list'")
        if not all([isinstance(column, str) for column in columns]):
            raise TypeError(f"'{attribute}' must be a list of strings")
        if not set(columns).issubset(
            {column["name"] for column in self._columns if self._columns}
        ):
            raise ValueError(f"'All elements of '{attribute}' must be in self.columns")
        if len(columns) != len(set(columns)):
            raise ValueError(f"'All elements of '{attribute}' must be unique")

    @property
    def partitions(self):
        return self._partitions

    @partitions.setter
    def partitions(self, partitions: set):
        has_distinct_elements = len(partitions) == len(set(partitions))
        all_str = all([type(e) is str for e in partitions])
        column_set = {e["name"] for e in self._columns if self._columns}
        if set(partitions).issubset(column_set) and all_str and has_distinct_elements:
            self._setter(attribute="partitions", type=list, value=partitions)
        else:
            raise ValueError("'partitions' must be a set of names defined in 'columns'")

    def from_json(self):
        pass

    def to_json(self):
        pass

    def to_dict(self, include_schema: bool = False):
        output = {
            "name": self.name,
            "description": self.description,
            "format": self.format,
            "sensitive": self.sensitive,
            "primary_key": self.primary_key,
            "partitions": self.partitions,
            "columns": self.columns,
        }
        if include_schema:
            output["$schema"] = self._schema
        return output

    def from_dict(self):
        pass
