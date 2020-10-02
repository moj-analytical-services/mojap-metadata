from etl_manager.utils import (
    read_json,
    write_json,
    _dict_merge,
    _end_with_slash,
    _validate_string,
    _validate_enum,
    _validate_pattern,
    _validate_nullable,
    _remove_final_slash,
    trim_complex_data_types,
    trim_complex_type,
    data_type_is_regex
)

import copy
import string
import json
import os
import re
import urllib
import time
import pkg_resources
import jsonschema
import warnings

_web_link_to_table_json_schema = "https://moj-analytical-services.github.io/metadata_schema/table/v1.0.0.json"

try:
    with urllib.request.urlopen(_web_link_to_table_json_schema) as url:
        _table_json_schema = json.loads(url.read().decode())
except urllib.error.URLError as e:
    warnings.warn("Could not get schema from URL. Reading schema from package instead...")
    _table_json_schema = json.load(
        pkg_resources.resource_stream(__name__, "metadata/specs/table_schema.json")
    )

_supported_column_types = _table_json_schema["properties"]["columns"]["items"][
    "properties"
]["type"]["enum"]
_supported_data_formats = _table_json_schema["properties"]["data_format"]["enum"]
_column_properties = list(
    _table_json_schema["properties"]["columns"]["items"]["properties"].keys()
)


class MetaColumnTypeMismatch(Exception):
    pass


class Metadata(object):
    """
    Manipulate the agnostic metadata associated with a table
    """

    def __init__(
        self,
        name,
        location = None,
        columns=[],
        data_format="csv",
        description="",
        partitions=[],
        partition_type=None
    ):

        self.name = name
        self.location = location
        self.columns = copy.deepcopy(columns)
        self.data_format = data_format
        self.description = description
        self.partitions = copy.deepcopy(partitions)
        self.partition_type = partition_type

        self.validate_json_schema()
        self.validate_column_types()

    def validate_json_schema(self):
        jsonschema.validate(
            trim_complex_data_types(self.to_dict()),
            _table_json_schema
        )

    def validate_column_types(self):
        assert all(
            data_type_is_regex(c["type"]) for c in self.to_dict()["columns"]
        )

    @property
    def name(self):
        return self._name

    # Adding validation as Athena doesn't like names with dashes
    @name.setter
    def name(self, name):
        _validate_string(name)
        self._name = name

    @property
    def data_format(self):
        return self._data_format

    @data_format.setter
    def data_format(self, data_format):
        self._check_valid_data_format(data_format)
        self._data_format = data_format

    @property
    def column_names(self):
        return [c["name"] for c in self.columns]

    # partitions
    @property
    def partitions(self):
        return self._partitions

    @partitions.setter
    def partitions(self, partitions):
        if not partitions:
            self._partitions = []
        else:
            for p in partitions:
                self._check_column_exists(p)
            new_col_order = [c for c in self.column_names if c not in partitions]
            new_col_order = new_col_order + partitions
            self._partitions = partitions
            self.reorder_columns(new_col_order)

    @property
    def partition_type(self):
        return self._partition_type


    @partition_type.setter
    def partition_type(self, partition_type):
        self._partition_type = partition_type

    @property
    def location(self):
        return self._location

    @location.setter
    def location(self, location):
        self._location = location


    def remove_column(self, column_name):
        self._check_column_exists(column_name)
        new_cols = [c for c in self.columns if c["name"] != column_name]
        new_partitions = [p for p in self.partitions if p != column_name]
        self.columns = new_cols
        self.partitions = new_partitions


    def add_column(
        self, name, type, description, pattern=None, enum=None, nullable=None
    ):
        self._check_column_does_not_exists(name)
        self._check_valid_datatype(type)
        _validate_string(name)
        cols = self.columns
        cols.append({"name": name, "type": type, "description": description})
        if enum:
            _validate_enum(enum)
            cols[-1]["enum"] = enum
        if pattern:
            _validate_pattern(pattern)
            cols[-1]["pattern"] = pattern
        if nullable is not None:
            _validate_nullable(nullable)
            cols[-1]["nullable"] = nullable

        self.columns = cols

        # Reorder columns if partitions exist
        if self.partitions:
            new_col_order = [c for c in self.column_names if c not in self.partitions]
            new_col_order = new_col_order + copy.deepcopy(self.partitions)
            self.reorder_columns(new_col_order)


    def reorder_columns(self, column_name_order):
        for c in self.column_names:
            if c not in column_name_order:
                raise ValueError(f"input column_name_order is missing column ({c}) in meta table")
        self.columns = sorted(
            self.columns, key=lambda x: column_name_order.index(x["name"])
        )


    def _check_valid_data_format(self, data_format):
        if data_format not in _supported_data_formats:
            sdf = ", ".join(_supported_data_formats)
            raise ValueError(
                f"The data_format provided ({data_format}) must match the supported data_type names: {sdf}"
                )

    def _check_valid_datatype(self, data_type):
        if data_type not in _supported_column_types:
            scf = ", ".join(_supported_column_types)
            raise ValueError(f"The data_type provided must match the supported data_type names: {scf}")


    def _check_column_exists(self, column_name):
        if column_name not in self.column_names:
            cn = ", ".join(self.column_names)
            raise ValueError(
                f"The column name: {column_name} does not match those existing in meta: {cn}"
            )


    def _check_column_does_not_exists(self, column_name):
        if column_name in self.column_names:
            raise ValueError(
                f"The column name provided ({column_name}) already exists table in meta."
            )


    def update_column(self, column_name, **kwargs):

        if len([k for k in kwargs.keys() if k in _column_properties]) == 0:
            raise ValueError(
                f"one or more of the function inputs ({', '.join(_column_properties)}) must be specified."
            )

        self._check_column_exists(column_name)

        new_cols = []
        for c in self.columns:
            if c["name"] == column_name:

                if "name" in kwargs:
                    _validate_string(kwargs["name"], "_")
                    c["name"] = kwargs["name"]

                if "type" in kwargs:
                    self._check_valid_datatype(kwargs["type"])
                    c["type"] = kwargs["type"]

                if "description" in kwargs:
                    c["description"] = kwargs["description"]

                if "pattern" in kwargs:
                    _validate_pattern(kwargs["pattern"])
                    c["pattern"] = kwargs["pattern"]

                if "enum" in kwargs:
                    _validate_enum(kwargs["enum"])
                    c["enum"] = kwargs["enum"]

                if "nullable" in kwargs:
                    _validate_nullable(kwargs["nullable"])
                    c["nullable"] = kwargs["nullable"]

            new_cols.append(c)

        self.columns = new_cols


    def to_dict(self):
        meta = {
            "$schema": _web_link_to_table_json_schema,
            "name": self.name,
            "description": self.description,
            "data_format": self.data_format,
            "columns": self.columns,
            "location": self.location,
        }
        if bool(self.partitions):
            meta["partitions"] = self.partitions

        if bool(self.glue_specific):
            meta["glue_specific"] = self.glue_specific

        return meta


    def to_json(self, filepath):
        write_json(self.to_dict(), filepath)


    def read_json(self, filepath):
        m = read_json(filepath)
        if "partitions" not in m:
            m["partitions"] = []

        metadata = Metadata(
            name=meta["name"],
            location=meta["location"],
            columns=meta["columns"],
            data_format=meta["data_format"],
            description=meta["description"],
            partitions=meta["partitions"]
        )

    return metadata
