import boto3
import os
import json
import re
import warnings

import pydbtools as pydb
from typing import Tuple, List, Union
from awswrangler.catalog import delete_table_if_exists
from mojap_metadata.metadata.metadata import (
    Metadata,
    _unpack_complex_data_type,
    _metadata_complex_dtype_names,
)
from mojap_metadata.converters import (
    BaseConverter,
    _flatten_and_convert_complex_data_type,
)
import importlib.resources as pkg_resources
from dataclasses import dataclass
from mojap_metadata.converters.glue_converter import specs


# Format generictype: (glue_type, is_fully_supported)
# If not fully supported we decide on best option
# if glue_type is Null then we have no way to safely
# convert it
_default_type_converter = {
    "null": (None, False),
    "bool": ("boolean", True),
    "bool_": ("boolean", True),
    "int8": ("tinyint", True),
    "int16": ("smallint", True),
    "int32": ("int", True),
    "int64": ("bigint", True),
    "uint8": ("smallint", False),
    "uint16": ("int", False),
    "uint32": ("bigint", False),
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
    "timestamp(s)": ("timestamp", True),
    "timestamp(ms)": ("timestamp", True),
    "timestamp(us)": ("timestamp", True),
    "timestamp(ns)": ("timestamp", True),
    "string": ("string", True),
    "large_string": ("string", True),
    "utf8": ("string", True),
    "large_utf8": ("string", True),
    "binary": ("binary", True),
    "large_binary": ("binary", True),
    "list": ("array", True),
    "list_": ("array", True),
    "large_list": ("array", True),
    "struct": ("struct", True),
}

_glue_to_mojap_type_converter = {
    "boolean": ("bool", True),
    "tinyint": ("int8", True),
    "smallint": ("int16", False),
    "int": ("int32", False),
    "integer": ("int32", False),
    "bigint": ("int64", False),
    "double": ("float64", True),
    "float": ("float32", False),
    "decimal": ("decima128", True),
    "char": ("string", True),
    "varchar": ("string", True),
    "string": ("string", True),
    "binary": ("larg_binary", False),
    "date": ("date64", False),
    "timestamp": ("timestamp(s)", False),
    "array": ("large_list", False),
    "struct": ("struct", False),
}


@dataclass
class CsvOptions:
    """
    Specific options for CSV spec
    """

    serde: str = "lazy"
    skip_header: bool = False
    sep: str = ","
    quote_char: str = '"'
    escape_char: str = "\\"
    compressed: bool = False


@dataclass
class JsonOptions:
    serde: str = "hive"
    compressed: bool = False


@dataclass
class ParquetOptions:
    # compression:str = "SNAPPY"
    compressed: bool = True


SpecOptions = Union[CsvOptions, JsonOptions, ParquetOptions]


@dataclass
class GlueConverterOptions:
    """
    Options Class for the GlueConverter

    deafultcsv_serde (str):
      Jinja template that is used to generate CSV ddl. Defaults to
      lazy serde template. To use the open serde template you can use
      options.set_csv_serde("open").

    json_serde (str):
      Jinja template that is used to generate JSON ddl. Can be set to
      a custom json ddl template but defaults to our recommended one:
      `specs/json_ddl.txt`.

    parquet_template (str):
      Jinja template that is used to generate PARQUET ddl. Can be set to
      a custom json ddl template but defaults to our recommended one:
      `specs/parquet_ddl.txt`.

    default_db_name (str):
      Default database name to default to when defining which database
      the table belongs to. Used when calling `GlueConverter.generate_from_meta`
      method and no database_name is specified.

    default_db_base_path (str):
      Default s3 base path default to when defining the the table exists in S3.
      Used when calling `GlueConverter.generate_from_meta` method and no
      table_location is  specified. When no table_location is specified, the
      output DDL wil define the table location as <default_db_base_path>/<table_name>/.

    ignore_warnings (bool, default=False):
      If converter should not warning users of imperfect type conversions.

    skip_header (bool):
      parameter to csv ddl function

    sep (str):
      parameter to csv ddl function

    quote_char (str):
      parameter to csv ddl function

    escape_char (str):
      parameter to csv ddl function

    line_term_char (str):
      parameter to csv ddl function

    parquet_compression (str):
      parameter to parquet ddl function
    """

    csv = CsvOptions()
    json = JsonOptions()
    parquet = ParquetOptions()
    default_db_name: str = None
    default_db_base_path: str = None
    ignore_warnings: bool = False

    def set_csv_serde(self, serde_name: str):
        allowed_serdes = ["lazy", "open"]
        if serde_name not in allowed_serdes:
            err_msg = (
                f"Input serde_name must be one of {allowed_serdes} "
                f"but got {serde_name}."
            )
            raise ValueError(err_msg)
        else:
            self.csv.serde = serde_name

    def set_json_serde(self, serde_name: str):
        allowed_serdes = ["hive", "openx"]
        if serde_name not in allowed_serdes:
            err_msg = (
                f"Input serde_name must be one of {allowed_serdes} "
                f"but got {serde_name}."
            )
            raise ValueError(err_msg)
        else:
            self.json.serde = serde_name


class GlueConverter(BaseConverter):
    def __init__(self, options: GlueConverterOptions = None):
        """
        Converts metadata objects to a Hive DDL.

        options (GlueConverterOptions, optional): See ?GlueConverterOptions
        for more details. If not set a default GlueConverterOptions is set to
        the options parameter.

        Example:
        from mojap_metadata.converters.glue_converter import (
            GlueConverter,
            GlueConverterOptions,
            _create_open_csv_ddl,
        )
        options = GlueConverterOptions(csv_ddl = _create_open_csv_ddl)
        gc = GlueConverter(options)
        metadata = Metadata.from_json("my-table-metadata.json")
        ddl = gc.generate_from_meta(metadata) # get Glue/Hive DDL
        """
        if options is None:
            options = GlueConverterOptions()

        super().__init__(options)
        self._default_type_converter = _default_type_converter

    def warn_conversion(self, coltype, converted_type, is_fully_supported):
        if converted_type is None:
            raise ValueError(
                f"{coltype} has no equivalent in Athena/Glue " "so cannot be converted"
            )

        if not self.options.ignore_warnings and not is_fully_supported:
            w = (
                f"{coltype} is not fully supported by Athena using best "
                "representation. To supress these warnings set this converters "
                "options.ignore_warnings = True"
            )
            warnings.warn(w)

    def convert_col_type(self, coltype: str) -> str:
        """Converts our metadata types to Athena/Glue versions

        Args:
            coltype (str): str representation of our metadata column types

        Returns:
            str: String representation of athena column type version of `coltype`
        """

        data_type = _unpack_complex_data_type(coltype)

        return _flatten_and_convert_complex_data_type(
            data_type, self.convert_basic_col_type, field_sep=","
        )

    def convert_basic_col_type(self, coltype: str) -> str:
        """Converts our metadata types (non complex ones)
        to etl-manager metadata. Used with the _flatten_and_convert_complex_data_type
        and convert_col_type functions.

        Args:
            coltype (str): str representation of our metadata column types

        Returns:
            str: String representation of athena column type version of `coltype`
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

    def convert_columns(self, metadata: Metadata) -> Tuple[List, List]:
        cols = []
        partitions = []
        for c in metadata.columns:
            if c["name"] in metadata.partitions:
                partitions.append(
                    {"Name": c["name"], "Type": self.convert_col_type(c["type"])}
                )
                if "description" in c:
                    partitions[-1]["Comment"] = c["description"]
            else:
                cols.append(
                    {"Name": c["name"], "Type": self.convert_col_type(c["type"])}
                )
                if "description" in c:
                    cols[-1]["Comment"] = c["description"]
        return cols, partitions

    def generate_from_meta(
        self,
        metadata: Metadata,
        database_name: str = None,
        table_location: str = None,
    ) -> dict:
        """Generates the Hive DDL from our metadata

        Args:
            metadata (Metadata): metadata object from the Metadata class
            database_name (str, optional): database name needed for table DDL.
              If `None` this function will look to the options.default_database_name
              attribute to find a name. Defaults to None.
            table_location (str, optional): S3 location of where table is stored
              needed for table DDL. If `None` this function will look to the
              options.default_database_name attribute to find a name. Defaults
              to None.
        Raises:
            ValueError: If database_name and table_location are not set (and there
              are no default options set)

        Returns:
            dict: for Glue API
        """

        # set database_name to metadata.database_name if none
        database_name = database_name if database_name else metadata.database_name
        # do the same with table_location
        table_location = table_location if table_location else metadata.table_location

        ff = metadata.file_format
        if ff.startswith("csv"):
            opts = self.options.csv

        elif ff.startswith("json"):
            opts = self.options.json

        elif ff.startswith("parquet"):
            opts = self.options.parquet

        else:
            raise ValueError(
                f"No ddl template for type: {ff} in options "
                "(only supports formats starting with csv, json or parquet)"
            )

        if not database_name:
            if self.options.default_db_name:
                database_name = self.options.default_db_name
            else:
                error_msg = (
                    "Either set database_name in the function "
                    "or set a default_db_name in the GlueConverter.options"
                )
                raise ValueError(error_msg)

        if not table_location:
            if self.options.default_db_base_path:
                table_location = os.path.join(
                    self.options.default_db_base_path, f"{metadata.name}"
                )
            else:
                error_msg = (
                    "Either set table_location in the function "
                    "or set a database_base_path in the "
                    "GlueConverter.options.database_base_path"
                )
                raise ValueError(error_msg)

        table_cols, partition_cols = self.convert_columns(metadata)

        spec = generate_spec_from_template(
            database_name=database_name,
            table_name=metadata.name,
            location=table_location,
            spec_opts=opts,
            table_desc=metadata.description,
            columns=table_cols,
            partitions=partition_cols,
        )
        return spec


class GlueTable(BaseConverter):
    def __init__(self, glue_converter_options: GlueConverterOptions = None):
        super().__init__(None)
        self.gc = GlueConverter(glue_converter_options)

    def convert_basic_col_type(self, col_type: str):
        if col_type.startswith("decimal"):
            regex = re.compile(r"decimal(\(\d+,\d+\))")
            bracket_numbers = regex.match(col_type).groups()[0]
            return f"decimal128{bracket_numbers}"
        else:
            return _glue_to_mojap_type_converter[col_type][0]

    def convert_col_type(self, col_type: str):
        data_type = _unpack_complex_data_type(col_type)
        return _flatten_and_convert_complex_data_type(
            data_type, self.convert_basic_col_type, field_sep=","
        )

    def convert_columns(self, columns: List[dict]) -> List[dict]:
        mojap_meta_cols = []
        for col in columns:
            col_type = self.convert_col_type(col["Type"])
            if col["Type"].startswith("decimal") or col["Type"].startswith(
                _metadata_complex_dtype_names
            ):
                full_support = False
            else:
                full_support = _glue_to_mojap_type_converter.get(col["Type"])[1]
            if not full_support:
                warnings.warn(
                    f"type {col['Type']} not fully supported, "
                    "likely due to multiple conversion options"
                )
            # create the column in mojap meta format
            meta_col = {
                "name": col["Name"],
                "type": col_type,
            }
            if col.get("Comment"):
                meta_col["description"] = col.get("Comment")
            mojap_meta_cols.append(meta_col)
        return mojap_meta_cols

    def generate_from_meta(
        self,
        metadata: Union[Metadata, str, dict],
        database_name: str = None,
        table_location: str = None,
        run_msck_repair=False,
    ):
        """
        Creates a glue table from metadata
        arguments:
            - metadata: Metadata object, string path, or dictionary metadata.
            - database_name (optional): name of the glue database the table is to be
            created in. can also be a property of the metadata.
            - table_location (optional): the s3 location of the table. can also be a
            property of the metadata.
            - run_msck_repair (optional): run msck repair table on the created table,
            should be set to True for tables with partitions.
        Raises:
            - ValueError if run_msck_repair table is False, metadata has partitions, and
            options.ignore_warnings is set to False
        """

        # set database_name to metadata.database_name if none
        database_name = database_name if database_name else metadata.database_name
        # do the same with table_location
        table_location = table_location if table_location else metadata.table_location

        glue_client = boto3.client(
            "glue",
            region_name=os.getenv(
                "AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "eu-west-1")
            ),
        )
        metadata = Metadata.from_infer(metadata)
        boto_dict = self.gc.generate_from_meta(
            metadata, database_name=database_name, table_location=table_location
        )
        delete_table_if_exists(database=database_name, table=metadata.name)
        glue_client.create_table(**boto_dict)

        if (
            not run_msck_repair
            and metadata.partitions
            and not self.options.ignore_warnings
        ):
            w = (
                "metadata has partitions and run_msck_reapair is set to false. To "
                "To supress these warnings set this converters "
                "options.ignore_warnings = True"
            )
            warnings.warn(w)
        elif run_msck_repair:
            pydb.read_sql_query(
                f"msck repair table `{database_name}`.`{metadata.name}`"
            )

    def generate_to_meta(self, database: str, table: str) -> Metadata:
        # get the table information
        glue_client = boto3.client("glue")
        resp = glue_client.get_table(DatabaseName=database, Name=table)

        # pull out just the columns
        columns = resp["Table"]["StorageDescriptor"]["Columns"]

        # convert the columns
        mojap_meta_cols = self.convert_columns(columns)

        # check for partitions
        partitions = resp["Table"].get("PartitionKeys")
        if partitions:
            # convert
            part_cols_full = self.convert_columns(partitions)
            # extend the mojap_meta_cols with the partiton cols
            mojap_meta_cols.extend(part_cols_full)
            part_cols = [p["name"] for p in part_cols_full]

            # make a metadata object
            meta = Metadata(name=table, columns=mojap_meta_cols, partitions=part_cols)
        else:
            meta = Metadata(name=table, columns=mojap_meta_cols)

        # get the file format if possible
        try:
            ff = resp["Table"]["StorageDescriptor"]["Parameters"].get("classification")
        except KeyError:
            warnings.warn("unable to parse file format, please manually set")
            ff = None

        if ff:
            meta.file_format = ff.lower()

        return meta


def _get_base_table_spec(spec_name: str, serde_name: str = None) -> dict:
    """Gets a table spec (dict) for a specific name
    prefilled with standard properties / info for that
    specific spec.

    Args:
        spec_name (str): Name of the spec currently -
        'csv', 'json' or 'parquet'

        serde_name (str): Name of the specific serde -
        CSV: 'open' or 'lazy'
        JSON: 'hive' or 'openx'
        PARQUET: None

    Returns:
        dict: A base spec that can be used with boto to create a table.
        Once specific details from metadata are filled into it.
    """
    if serde_name:
        filename = f"{serde_name}_{spec_name}_spec.json"
    else:
        filename = f"{spec_name}_spec.json"

    table_spec = json.load(pkg_resources.open_text(specs, filename))
    return table_spec


def _get_spec_and_serde_name_from_opts(spec_opts) -> Tuple[str, str]:
    """Returns the spec name and serde name for a given option Class
    and parameters

    Args:
        spec_opts ([type]): [description]

    Raises:
        ValueError: [description]

    Returns:
        Tuple[str, str]: [description]
    """
    if isinstance(spec_opts, CsvOptions):
        spec_name = "csv"
        serde_name = spec_opts.serde
    elif isinstance(spec_opts, JsonOptions):
        spec_name = "json"
        serde_name = spec_opts.serde
    elif isinstance(spec_opts, ParquetOptions):
        spec_name = "parquet"
        serde_name = None
    else:
        raise ValueError(
            f"expected opts to be of an options Type not {type(spec_opts)}"
        )

    return spec_name, serde_name


def _convert_opts_into_dict(spec_opts: SpecOptions):
    """Takes the spec_opts and converts it to a dict
    Just used to pass to Template.render().

    Args:
        spec_opts (SpecOptions): One of the SpecOptions
        classes

    Returns:
        dict: A dict representation of the data class
    """
    out_dict = {}
    for k, _ in spec_opts.__annotations__:
        out_dict[k] = getattr(spec_opts, k)
    return out_dict


def generate_spec_from_template(
    database_name,
    table_name,
    location,
    spec_opts: SpecOptions,
    table_desc="",
    columns=[],
    partitions=[],
):
    spec_name, serde_name = _get_spec_and_serde_name_from_opts(spec_opts)

    base_spec = _get_base_table_spec(spec_name, serde_name)
    base_spec["Name"] = table_name
    base_spec["Description"] = table_desc
    base_spec["StorageDescriptor"]["Columns"] = columns
    base_spec["PartitionKeys"] = partitions
    base_spec["StorageDescriptor"]["Location"] = location

    # Do general options
    base_spec["StorageDescriptor"]["Compressed"] = spec_opts.compressed

    # Do CSV options
    if spec_name == "csv":

        csv_param_lu = {
            "sep": {"lazy": "field.delim", "open": "separatorChar"},
            "quote_char": {"lazy": None, "open": "quoteChar"},
            "escape_char": {"lazy": "escape.delim", "open": "escapeChar"},
        }

        if spec_opts.skip_header:
            (
                base_spec["StorageDescriptor"]["SerdeInfo"]["Parameters"][
                    "skip.header.line.count"
                ]
            ) = "1"

        if spec_opts.sep:
            param_name = csv_param_lu["sep"][serde_name]
            (
                base_spec["StorageDescriptor"]["SerdeInfo"]["Parameters"][param_name]
            ) = spec_opts.sep

        if spec_opts.quote_char and serde_name != "lazy":
            (
                base_spec["StorageDescriptor"]["SerdeInfo"]["Parameters"]["quoteChar"]
            ) = spec_opts.quote_char

        if spec_opts.escape_char:
            param_name = csv_param_lu["escape_char"][serde_name]
            (
                base_spec["StorageDescriptor"]["SerdeInfo"]["Parameters"][param_name]
            ) = spec_opts.escape_char

    # Do JSON options
    if spec_name == "json":
        json_col_paths = ",".join([c["Name"] for c in columns])
        (
            base_spec["StorageDescriptor"]["SerdeInfo"]["Parameters"]["paths"]
        ) = json_col_paths

    out_dict = {"DatabaseName": database_name, "TableInput": base_spec}
    return out_dict
