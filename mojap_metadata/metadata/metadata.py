import json
from warnings import warn

import jsonschema


class MetadataProperty:
    def __set_name__(self, owner, name) -> None:
        self.name = f"_{name}"

    def __get__(self, obj, type=None) -> object:
        return getattr(obj, self.name)

    def __set__(self, obj, value) -> None:
        setattr(obj, self.name, value)
        obj.validate()


class Metadata:
    name = MetadataProperty()
    description = MetadataProperty()
    format = MetadataProperty()
    sensitive = MetadataProperty()
    columns = MetadataProperty()
    primary_key = MetadataProperty()
    partitions = MetadataProperty()

    def __init__(
        self,
        name: str = "",
        description: str = "",
        format: str = "",
        sensitive: bool = False,
        columns: list = [],
        primary_key: set = [],
        partitions: set = [],
    ) -> None:
        self._name = name
        self._description = description
        self._format = format
        self._sensitive = sensitive
        self._columns = columns
        self._primary_key = primary_key
        self._partitions = partitions

        with open("mojap_metadata/metadata/specs/table_schema.json") as file:
            self._schema = json.load(file)

        self.validate()

    def validate(self):
        jsonschema.validate(
            instance=self.to_dict(include_schema=False), schema=self._schema
        )
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

    def from_dict(self, d: dict) -> None:
        self.name = d.get("name", "")
        self.description = d.get("description", "")
        self.format = d.get("format", "")
        self.sensitive = d.get("sensitive", False)
        self.columns = d.get("columns", [])
        self.primary_key = d.get("primary_key", [])
        self.partitions = d.get("parititions", [])
        if diff := set(d).difference(
            {
                "name",
                "description",
                "format",
                "sensitive",
                "columns",
                "primary_key",
                "partitions",
            }
        ):
            warn(
                f"Some properties will be ignored: {', '.join(sorted(diff))}",
                UserWarning,
            )

    def to_dict(self, include_schema: bool = False) -> dict:
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

    def from_json(self, file, **kwargs) -> dict:
        with open(file, "r") as f:
            obj = json.load(f, **kwargs)
            self.from_dict(obj)

    def to_json(self, file: str, mode: str = "w", **kwargs) -> None:
        with open(file, mode) as f:
            json.dump(self.to_dict(), f)
