from typing import Callable, List

from mojap_metadata.metadata.metadata import Metadata
from mojap_metadata.converters import BaseConverter
import warnings
from etl_manager.meta import TableMeta

# Format generictype: (glue_type, is_fully_supported)
# If not fully supported we decide on best option
# if glue_type is Null then we have no way to safely
# convert it
_default_type_converter = {
    "bool_": ("boolean", True),
    "int8": ("int", False),
    "int16": ("int", False),
    "int32": ("int", True),
    "int64": ("long", True),
    "uint8": ("int", False),
    "uint16": ("int", False),
    "uint32": ("long", False),
    "uint64": (None, False),
    "decimal128": ("decimal", True),
    "float16": ("float", False),
    "float32": ("float", True),
    "float64": ("double", True),
    "time32": (None, False),
    "time32(s)": (None, False),
    "time32(ms)": (None, False),
    "time64(us)": (None, False),
    "time64(ns)": (None, False),
    "date32": ("date", True),
    "date64": ("date", True),
    "timestamp(s)": ("datetime", True),
    "timestamp(ms)": ("datetime", True),
    "timestamp(us)": ("datetime", True),
    "timestamp(ns)": ("datetime", True),
    "string": ("character", True),
    "large_string": ("character", True),
    "utf8": ("character", True),
    "large_utf8": ("character", True),
    "binary": ("binary", True),
    "large_binary": ("binary", True),
    # Need to do MAPS / STRUCTS
}

_reverse_default_type_converter = {
    "character": ("string", True),
    "int": ("int32", True),
    "long": ("int64", True),
    "float": ("float32", True),
    "double": ("float64", True),
    "decimal": ("decimal128", True),
    "date": ("date32", True),
    "datetime": ("timestamp(s)", True),
    "binary": ("binary", True),
    "boolean": ("bool_", True),
    "struct": ("map_", False),
    "array": ("list_", False),
}


class EtlManagerConverter(BaseConverter):
    def __init__(self):
        """
        Converts metadata objects to etl-manager metadata
        and vice-versa.

        Note that this converter has no options
        (i.e. EtlManagerConverter().options returns
        the BaseCoverterOptions)

        Example:
        from mojap_metadata.converters.etl_manager_converter import (
            EtlManagerConverter,
        )
        emc = EtlManagerConverter()
        metadata = Metadata.from_json("my-table-metadata.json")
        table_meta = emc.generate_from_meta(metadata) # get TableMeta obj
        """

        super().__init__(None)
        self._default_type_converter = _default_type_converter
        self._reverse_default_type_converter = _reverse_default_type_converter

    def warn_conversion(self, coltype, converted_type, is_fully_supported):
        if converted_type is None:
            raise ValueError(
                f"{coltype} has no equivalent in Metadata so cannot be converted"
            )

        if not self.options.ignore_warnings and not is_fully_supported:
            w = (
                f"{coltype} is not fully supported by our Metadata using best "
                "representation. To supress these warnings set this converters "
                "options.ignore_warnings = True"
            )
            warnings.warn(w)

    def convert_col_type(self, coltype: str):
        """Converts our metadata types to etl-manager metadata

        Args:
            coltype ([str]): str representation of our metadata column types

        Returns:
            [type]: str representation of etl-manager column type version of `coltype`
        """
        if coltype.startswith("decimal128"):
            t, is_supported = self._default_type_converter.get("decimal128")
            brackets = coltype.split("(")[1].split(")")[0]
            t = f"{t}({brackets})"
        elif coltype.startswith("binary"):
            coltype_ = coltype.split("(", 1)[0]
            t, is_supported = self._default_type_converter.get(coltype_, (None, None))
        else:
            t, is_supported = self._default_type_converter.get(coltype, (None, None))

        self.warn_conversion(coltype, t, is_supported)
        return t

    def convert_columns(
        self,
        metadata: Metadata,
        include_extra_column_params: bool = True
    ) -> List[dict]:
        """Converts metadata.columns to a list of etl_manager columns

        Args:
            metadata (Metadata): Metadata object
            include_extra_column_params (bool, optional): If true add on the extra
                column properties from metadata to the etl_manager meta data columns.

        Returns:
            List[dict]: list of etl_manager columns
        """

        etl_manager_cols = []
        etl_manager_params = [
            "nullable",
            "sensitive",
            "enum",
            "pattern"
        ]

        default_params = [
            "name",
            "type",
            "description"
        ]

        for c in metadata.columns:
            etl_manager_cols.append(
                {
                    "name": c["name"],
                    "type": self.convert_col_type(c["type"]),
                    "description": c.get("description", ""),
                }
            )

            for k, v in c.items():
                if k in default_params:
                    continue

                if include_extra_column_params or (k in etl_manager_params):
                    etl_manager_cols[-1][k] = v

        return etl_manager_cols

    def _default_file_format_to_data_format(self, file_format):

        if file_format.startswith("csv"):
            data_format = "csv"
        elif file_format.startswith("avro"):
            data_format = "avro"
        elif file_format.startswith("json") or file_format.startswith("ndjson"):
            data_format = "json"
        elif file_format.startswith("parquet"):
            data_format = "parquet"
        else:
            warnings.warn(
                "Could not determine data_format. Try setting your own "
                "file_format_mapper."
            )
            data_format = file_format
        return data_format

    def generate_from_meta(
        self,
        metadata: Metadata,
        table_location: str = None,
        file_format_mapper: Callable = None,
        include_extra_column_params: bool = True,
    ) -> dict:
        """Generates a TableMeta object from our metadata

        Args:
            metadata (Metadata): metadata object from the Metadata class
            file_format_mapper (Callable, optional): If not set the function
                will infer what the etl_manager data_type should be based on what
                your file format starts with. e.g. parquet.snappy -> parquet,
                csv.gz -> csv, etc. If you want to use your own mapper set a function
                object to this param e.g.
                file_format_mapper = my_lookup_dict.get
            table_location (str): relative path in S3, this is the location property of
                etl_manager metadata. If not set defaults to "<table_name>/"
            include_extra_column_params (bool, option): If True will add on additional
                column parameters that may not be used by etl_manager but other
                downstream tools that use etl_manager schemas. Set to False to only
                convert the params that are used by etl_manager.
        Returns:
            TableMeta: An object from the TableMeta class in etl_manager.meta
        """

        if file_format_mapper:
            data_format = file_format_mapper(metadata.file_format)
        else:
            data_format = self._default_file_format_to_data_format(metadata.file_format)

        columns = self.convert_columns(metadata, include_extra_column_params)
        location = table_location if table_location else f"{metadata.name}/"

        tab = TableMeta(
            name=metadata.name,
            location=location,
            columns=columns,
            data_format=data_format,
            description=getattr(metadata, "description", ""),
            partitions=getattr(metadata, "partitions", []),
            primary_key=getattr(metadata, "primary_key", []),
            glue_specific={},
            database=None,
        )

        return tab

    def reverse_convert_col_type(self, coltype: str):
        """Converts etl-manager metadata col types to Metadata col types

        Args:
            coltype ([str]): str representation of etl-manager column types

        Returns:
            [type]: str representation of Metadta col type
        """
        if coltype.startswith("decimal"):
            t, is_supported = self._reverse_default_type_converter.get("decimal")
            brackets = coltype.split("(")[1].split(")")[0]
            t = f"{t}({brackets})"
        elif coltype.startswith("binary"):
            coltype_ = coltype.split("(", 1)[0]
            t, is_supported = self._reverse_default_type_converter.get(
                coltype_,
                (None, None)
            )
        elif coltype.startswith("struct") or coltype.startswith("array"):
            coltype_ = coltype.split("<", 1)[0]
            t, is_supported = self._reverse_default_type_converter.get(
                coltype_,
                (None, None)
            )
        else:
            t, is_supported = self._reverse_default_type_converter.get(
                coltype,
                (None, None)
            )

        self.warn_conversion(coltype, t, is_supported)
        return t

    def generate_to_meta(
        self,
        table_meta: TableMeta,
        data_format_mapper: Callable = None,
        col_type_mapper: Callable = None
    ) -> Metadata:
        """Takes a TableMeta object and converts it to our Metadata object

        Args:
            etl_manager_table_meta (Metadata): TableMeta object from etl-manager
            data_format_mapper (Callable, optional): If not set the function
                will just set the file_format parameter to the str in the
                original data_format of the TableMeta. If you want to use
                your own mapper set a function object to this param e.g.
                data_format_mapper = my_lookup_dict.get
            col_type_mapper (Callable, option): If not set the col type conversion
                from TableMeta -> Metadata is done using the converters
                reverse_convert_col_type method. If you need a custom conversion
                set a function to this parameter to use said function instead of
                reverse_convert_col_type This callable should expect the TableMeta
                col type str and return the Metadata col type str name.
        Returns:
            TableMeta: An object from the TableMeta class in etl_manager.meta
        """

        table_meta_dict = table_meta.to_dict()

        renamed_params = {"data_format": "file_format"}
        for old_name, new_name in renamed_params.items():
            table_meta_dict[new_name] = table_meta_dict.pop(old_name)

        if data_format_mapper:
            table_meta_dict["file_format"] = data_format_mapper(
                table_meta_dict["file_format"]
            )

        # remove etl_manager schema
        del table_meta_dict["$schema"]

        # convert columns
        etl_cols = table_meta_dict.pop("columns")
        for c in etl_cols:
            if col_type_mapper is None:
                c["type"] = self.reverse_convert_col_type(c["type"])
            else:
                c["type"] = col_type_mapper(c["type"])

        table_meta_dict["columns"] = etl_cols

        table_meta_dict["_converted_from"] = "etl_manager"
        return Metadata.from_dict(table_meta_dict)
