import re
import json

import yaml
import warnings
from copy import deepcopy
import importlib.resources as pkg_resources
import jsonschema
from mojap_metadata.metadata import specs

from typing import Union, List, Callable

_table_schema = json.load(pkg_resources.open_text(specs, "table_schema.json"))
_schema_url = "https://moj-analytical-services.github.io/metadata_schema/mojap_metadata/v1.3.0.json"  # noqa

_metadata_struct_dtype_names = ("struct",)
_metadata_struct_dtype_names_bracket = tuple(
    [x + "<" for x in _metadata_struct_dtype_names]
)
_metadata_list_dtype_names = ("list", "list_", "large_list", "array")
_metadata_list_dtype_names_bracket = tuple(
    [x + "<" for x in _metadata_list_dtype_names]
)
_metadata_complex_dtype_names = (
    _metadata_struct_dtype_names + _metadata_list_dtype_names
)
_metadata_complex_dtype_names_bracket = (
    _metadata_struct_dtype_names_bracket + _metadata_list_dtype_names_bracket
)


def _get_type_category_pattern_dict_from_schema():
    out = {}
    for type_cat in _table_schema["definitions"].keys():
        if not type_cat.endswith("types"):
            continue

        for a in _table_schema["definitions"][type_cat]["allOf"]:
            enum = a.get("properties", {}).get("type_category", {}).get("enum")
            patt = a.get("properties", {}).get("type", {}).get("pattern")

            if enum and patt:
                new_patt = patt.replace("\\\\", "\\")
                out[enum[0]] = rf"{new_patt}"
                break

    return out


def _parse_and_split(text: str, char: str) -> List[str]:
    """
    Splits a string into a list by splitting on
    any input char that is outside of parentheses.
    If `char` is inside parentheses then no split
    occurs. Also strips each str in the list.
    """
    in_parentheses = [0, 0, 0]  # [square, round, angular]

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
            in_parentheses[2] -= 1

        if s == char and not any([bool(p) for p in in_parentheses]):
            yield text[start + 1 : i].strip()
            start = i

    yield text[start + 1 :].strip()


def _get_first_level(text: str) -> str:
    """Returns everything in first set of <>"""
    bracket_counter = 0
    start = -1
    end = -1
    found_first_bracket = False
    for i, c in enumerate(text):
        if c == "<":
            bracket_counter += 1
            if not found_first_bracket:
                start = i + 1
                found_first_bracket = True
        elif c == ">":
            bracket_counter -= 1
            if bracket_counter == 0:
                end = i
                break

    if start == -1 or end == -1:
        raise ValueError(f"No closed brackets found in: {text}")

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
    _metadata_list_dtype_names_bracket
    _metadata_struct_dtype_names_bracket
    _metadata_complex_dtype_names_bracket

    if data_type.startswith(_metadata_struct_dtype_names_bracket):
        d["struct"] = {}
        next_data_type = _get_first_level(data_type)
        for data_param in _parse_and_split(next_data_type, ","):
            k, v = data_param.split(":", 1)
            k = k.strip()
            v = v.strip()
            if v.startswith(_metadata_complex_dtype_names_bracket):
                d["struct"][k] = _unpack_complex_data_type(v)
            else:
                d["struct"][k] = v
        return d
    elif data_type.startswith(_metadata_list_dtype_names_bracket):
        k = data_type.split("<", 1)[0]
        d[k] = {}
        next_data_type = _get_first_level(data_type).strip()
        if next_data_type.startswith(_metadata_complex_dtype_names_bracket):
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
        """
        generates Metadata object from a dictioanary representation of metadata
        args:
            d: metadata that adheres to the metadata schema
        returns:
            Metadata object
        """
        m = cls()
        m._init_data_with_default_key_values(d)
        m.validate()
        return m

    @classmethod
    def from_json(cls, filename, **kwargs) -> object:
        """
        generates Metadata object from a string path to a json metadata formatted file
        args:
            filename: path to the metadata json file
        returns:
            Metadata object
        """
        with open(filename, "r") as f:
            obj = json.load(f, **kwargs)
            return cls.from_dict(obj)

    @classmethod
    def from_yaml(cls, filename, **kwargs) -> object:
        """
        generates Metadata object from a string path to a yaml metadata formatted file
        args:
            filename: path to the metadata yaml file
        returns:
            Metadata object
        """
        with open(filename, "r") as f:
            obj = yaml.safe_load(f, **kwargs)
            return cls.from_dict(obj)

    @classmethod
    def from_infer(cls, inp, **kwargs) -> object:
        """
        generates Metadata object from and infers the format of the metadata input and
        uses one of the above class methods to load. If passed a Metadata object, will
        return that unchanged.
        args:
            inp: input that is either a string path to a yaml or json file, a dictionary
            or a Metadata object
        returns:
            Metadata object
        """
        if isinstance(inp, str) and inp.lower().endswith("json"):
            return cls.from_json(inp, **kwargs)
        elif isinstance(inp, str) and inp.lower().endswith("yaml"):
            return cls.from_yaml(inp, **kwargs)
        elif isinstance(inp, dict):
            return cls.from_dict(inp)
        elif isinstance(inp, cls):
            return inp
        else:
            raise TypeError(f"input type not recognised: {type(inp)}")

    name = MetadataProperty()
    description = MetadataProperty()
    file_format = MetadataProperty()
    sensitive = MetadataProperty()
    primary_key = MetadataProperty()
    database_name = MetadataProperty()
    table_location = MetadataProperty()

    def __init__(
        self,
        name: str = "",
        description: str = "",
        file_format: str = "",
        sensitive: bool = False,
        columns: list = None,
        primary_key: list = None,
        partitions: list = None,
        force_partition_order: str = None,
    ) -> None:

        self._schema = deepcopy(_table_schema)

        self._data = {
            "$schema": _schema_url,
            "name": name,
            "description": description,
            "file_format": file_format,
            "sensitive": sensitive,
            "columns": columns if columns else [],
            "primary_key": primary_key if primary_key else [],
            "partitions": partitions if partitions else [],
        }

        self.default_type_category_lookup = {
            "null": "null",
            "integer": "int64",
            "float": "float64",
            "string": "string",
            "timestamp": "timestamp(s)",
            "binary": "binary",
            "boolean": "bool",
            "list": "list<null>",
            "struct": "struct<null>",
        }

        self.validate()
        self.force_partition_order = force_partition_order

    @property
    def force_partition_order(self):
        return self._force_partition_order

    @force_partition_order.setter
    def force_partition_order(self, order: str):
        if order is not None and order not in ["start", "end"]:
            raise ValueError(
                "force_partition_order can only be set to None, "
                f'"start" or "end". Given {order}'
            )
        else:
            self._force_partition_order = order
            self.reorder_cols_based_on_partition_order()

    def reorder_cols_based_on_partition_order(self):
        """
        Reorders columns if metadata data has columns, partitions
        and has force_partition_order set to "start" or "end".
        """
        if self.force_partition_order and self.partitions and self.columns:
            non_partitions = [
                c for c in self.columns if c["name"] not in self.partitions
            ]
            partitions = [c for c in self.columns if c["name"] in self.partitions]

            # Ensure partitions listed in column match partition order
            partitions = sorted(
                partitions, key=lambda x: self.partitions.index(x["name"])
            )

            # Set private property to avoild recursive calling
            # no validation takes place as no data is added (just reordered)
            if self.force_partition_order == "start":
                self._data["columns"] = partitions + non_partitions
            else:
                self._data["columns"] = non_partitions + partitions
        else:
            pass

    @property
    def columns(self):
        return self._data["columns"]

    @columns.setter
    def columns(self, columns: List[dict]):
        self._data["columns"] = columns
        self.validate()
        self.reorder_cols_based_on_partition_order()

    @property
    def partitions(self):
        return self._data["partitions"]

    @partitions.setter
    def partitions(self, partitions: List[str]):
        self._data["partitions"] = partitions
        self.validate()
        self.reorder_cols_based_on_partition_order()

    @property
    def column_names(self):
        return [c["name"] for c in self.columns]

    def get_column(self, name: str):
        """
        Returns a column thats name matched input.
        None is returned if no match.
        """
        c = None
        for col in self.columns:
            if col["name"] == name:
                c = col
                break
        return c

    def remove_column(self, name: str):
        if name in self.column_names:
            del self.columns[self.column_names.index(name)]
            if name in self.partitions:
                del self.partitions[self.partitions.index(name)]
        else:
            raise ValueError(f"Column: {name} not in metadata columns")

    def update_column(self, column: dict):
        """
        Adds a column to the columns property. If the column name
        alredy exists in in the metadata then that column is replaced.
        If the column name does not exist it is added to the end.

        Args:
            column (dict): A dict that has the expected properties of
                a column.
        """
        name = column["name"]
        if name in self.column_names:
            i = self.column_names.index(name)
            self.columns[i] = column
        else:
            self.columns.append(column)
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
            "$schema": _schema_url,
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

    def set_col_type_category_from_types(self):
        """
        Sets any missing type_category based on the type attribute of the column.
        """
        mapping = _get_type_category_pattern_dict_from_schema()

        # Apply new types
        for col in self.columns:
            if "type_category" not in col and col.get("type"):
                for type_cat, pattern in mapping.items():
                    if re.match(pattern, col["type"]):
                        col["type_category"] = type_cat
                        break

    def set_col_types_from_type_category(self, type_category_lookup: Callable = None):
        """Set any missing type attribute for each column
        based on the type_category attribute.

        Args:
            type_category_lookup (Callable): A function that takes
            the col dict and returns the type based on attributes of the column
            dict. To apply a simple dictionary lookup pass the dictionary get
            method with a lambda as this arg (e.g. lambda x: d.get(x["type_category"])).
            If None applies the dictionary (self.default_type_category_lookup) is used
            to assign types based on the column's type_category attribute. Will raise a
            warning for "list" or "struct" type_category (this is due to these types
            need) to be specified by the user.
        """

        # Define standard getter for type category lookup
        if type_category_lookup is None:

            def type_category_lookup(col):
                tc = col.get("type_category")
                if tc is None:
                    err_msg = (
                        "type and type_category attributes "
                        f"are missing from the col: {name}"
                    )
                    raise KeyError(err_msg)

                if tc in ["struct", "list"]:
                    warn_msg = (
                        "For type_category of struct or list only a basic "
                        "version is inferred i.e. struct<null> or list<null>"
                    )
                    warnings.warn(warn_msg)

                return self.default_type_category_lookup.get(tc)

        # Apply new types
        for col in self.columns:
            name = col.get("name")
            if col.get("type") is None:
                new_type = type_category_lookup(col)

                if new_type is None:
                    raise ValueError(f"No type returned for col: {col}")
                col["type"] = new_type

        self.validate()
