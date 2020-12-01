import os

from mojap_metadata.metadata.metadata import Metadata
from mojap_metadata.converters import BaseConverter
import warnings

from typing import Tuple

from dataclasses import dataclass

# Format generictype: (glue_type, is_fully_supported)
# If not fully supported we decide on best option
# if glue_type is Null then we have no way to safely
# convert it
_default_type_converter = {
    "int8": ("TINYINT", True),
    "int16": ("SMALLINT", True),
    "int32": ("INT", True),
    "int64": ("BIGINT", True),
    "uint8": ("SMALLINT", False),
    "uint16": ("INT", False),
    "uint32": ("BIGINT", False),
    "uint64": (None, False),
    "decimal128": ("DECIMAL", True),
    "float16": ("FLOAT", False),
    "float32": ("FLOAT", True),
    "float64": ("DOUBLE", True),
    "time32": (None, False),
    "time32(s)": (None, False),
    "time32(ms)": (None, False),
    "time64(us)": (None, False),
    "time64(ns)": (None, False),
    "date32": ("DATE", True),
    "date64": ("DATE", True),
    "timestamp(s)": ("TIMESTAMP", True),
    "timestamp(ms)": ("TIMESTAMP", True),
    "timestamp(us)": ("TIMESTAMP", True),
    "timestamp(ns)": ("TIMESTAMP", True),
    "string": ("STRING", True),
    "large_string": ("STRING", True),
    "utf8": ("STRING", True),
    "large_utf8": ("STRING", True),
    "binary": ("BINARY", True),
    "large_binary": ("BINARY", True),
    # Need to do MAPS / STRUCTS
}


def _create_column_definition(columns) -> Tuple[str, str]:
    """
    Converts the columns (with Hive/Athena types and partition flag)
    And converts it a string for the HIVE DDL

    Args:
        columns ([type]): A list of dictionaries with name, type (Athena type),
        description and partition (bool) flags.

    Returns:
        Tuple[str, str]: The column and partition definitions for a HIVE DDL
        as strings
    """
    cols_ddl = []
    part_ddl = []
    for c in columns:
        if c["partition"]:
            part_ddl.append(f"`{c['name']}` {c['type']} COMMENT `{c['description']}`")
        else:
            cols_ddl.append(f"`{c['name']}` {c['type']} COMMENT `{c['description']}`")

    cols_ddl = ",\n".join(cols_ddl)
    part_ddl = ",\n".join(part_ddl)

    return cols_ddl, part_ddl


def _create_start_of_ddl(
    database: str, table: str, columns: list,
):
    """
    Inits the start of the DDL same for all other ddls
    (cols and partition defintions)
    """
    cols, part = _create_column_definition(columns)
    if part:
        partition_section = "PARTITIONED BY (\n {part} \n)"
    else:
        partition_section = ""

    ddl = f"""
    CREATE EXTERNAL TABLE {database}.{table} (
    {cols}
    )
    {partition_section}
    """
    return ddl


def create_lazy_csv_ddl(
    database: str,
    table: str,
    columns: list,
    location: str,
    skip_header=False,
    sep=",",
    quote_char='"',
    escape_char="\\",
    line_term_char="\n",
    **kwargs,
):
    """
    Creates a DDL for CSV data using the Lazy serde
    """
    ddl_start = _create_start_of_ddl(database, table, columns)

    table_properties = ""
    if skip_header:
        table_properties += "TBLPROPERTIES (\n" '"skip.header.line.count"="1"\n' ")"

    ddl_end = f"""
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '{sep}'
    ESCAPED BY '{escape_char}'
    LINES TERMINATED BY '{line_term_char}'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
    '{location}'
    {table_properties};
    """

    ddl = ddl_start + ddl_end
    return ddl


def create_open_csv_ddl(
    database: str,
    table: str,
    columns: list,
    location: str,
    skip_header=False,
    sep=",",
    quote_char='"',
    escape_char="\\",
    **kwargs,
):
    """
    Creates a DDL for CSV data using the OpenCSV serde
    """
    ddl_start = _create_start_of_ddl(database, table, columns)

    table_properties = ""
    if skip_header:
        table_properties += "TBLPROPERTIES (\n" '"skip.header.line.count"="1"\n' ")"

    ddl_end = f"""
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
    'separatorChar' = '{sep}',
    'quoteChar' = '{quote_char}',
    'escapeChar' = '{escape_char}'
    )
    STORED AS INPUTFORMAT
        'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
    '{location}'
    {table_properties};
    """
    ddl = ddl_start + ddl_end
    return ddl


def create_json_ddl(database: str, table: str, columns: list, location: str, **kwargs):
    """
    Creates a DDL for a JSON data
    """
    ddl_start = _create_start_of_ddl(database, table, columns)

    json_col_paths = ",".join([c["name"] for c in columns if not c["partition"]])
    ddl_end = f"""
    ROW FORMAT SERDE
        'org.apache.hive.hcatalog.data.JsonSerDe'
    WITH SERDEPROPERTIES (
        'paths'='{json_col_paths}')
    STORED AS INPUTFORMAT
        'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
        '{location}'
    TBLPROPERTIES (
        'classification'='json'
    )
    """
    ddl = ddl_start + ddl_end
    return ddl


def create_parquet_ddl(
    database: str,
    table: str,
    columns: list,
    location: str,
    compression="SNAPPY",
    **kwargs,
):
    """
    Creates a DDL for a PARQUET data
    """
    ddl_start = _create_start_of_ddl(database, table, columns)

    table_properties = "'classification'='parquet'"
    if compression:
        table_properties += ",\n"
        table_properties += f"'parquet.compression'='{compression}'"

    ddl_end = f"""
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
        '{location}'
    TBLPROPERTIES (
        {table_properties}
    );
    """
    ddl = ddl_start + ddl_end
    return ddl


@dataclass
class GlueConverterOptions:
    """
    Options Class for the GlueConverter

    csv_ddl (function):
        Name of the to create the ddl for metadata data_format = csv. Defaults to
        create_lazy_csv_ddl. But can also set to _create_open_csv_ddl to use the
        OpenCSV Serde. Otherwise can specify your own ddl creation function.

    json_ddl (function):
        Function to create the ddl for metadata data_format = json. Defaults to
        create_json_ddl. Can specify your own ddl creation function.

    parquet_ddl (function):
        Function to create the ddl for metadata data_format = parquet. Defaults
        to create_parquet_ddl. Can specify your own ddl creation function.

    default_db_name (dict):
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

    compression (str):
      parameter to parquet ddl function
    """
    default_db_name: str = None
    default_db_base_path: str = None
    csv_ddl = create_lazy_csv_ddl
    json_ddl = create_json_ddl
    parquet_ddl = create_parquet_ddl
    ignore_warnings: bool = False
    skip_header: bool = False
    sep: str = ","
    quote_char: str = '"'
    escape_char: str = "\\"
    line_term_char: str = "\n"
    compression: str = "SNAPPY"


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
        super().__init__(options)

        self._default_type_converter = _default_type_converter
        if options is None:
            self.options = GlueConverterOptions()
        else:
            self.options = options

    def warn_conversion(self, coltype, converted_type, is_fully_supported):
        if converted_type is None:
            raise ValueError(
                f"{coltype} has no equivalent in Athena/Glue "
                "so cannot be converted"
            )

        if not self.options.ignore_warnings and not is_fully_supported:
            w = (
                f"{coltype} is not fully supported by Athena using best "
                "representation. To supress these warnings set this converters "
                "options.ignore_warnings = True"
            )
            warnings.warn(w)

    def convert_col_type(self, coltype: str):
        """Converts our metadata types to Athena/Glue versions

        Args:
            coltype ([str]): str representation of our metadata column types

        Returns:
            [type]: str representation of athena column type version of `coltype`
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

    def convert_columns(self, metadata: Metadata):
        self._check_meta_set()
        cols = []

        for c in metadata.columns:
            cols.append(
                {
                    "name": c["name"],
                    "type": self.convert_col_type(c["type"]),
                    "description": c["description"],
                    "partition": c["name"] in metadata.partitons,
                }
            )
        return cols

    def generate_from_meta(
        self,
        metadata: Metadata,
        database_name: str = None,
        table_location: str = None,
        **kwargs,
    ) -> str:
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
            str: An SQL string that can be used to create the table in Glue metadata
              store.
        """
        mdf = metadata.data_format
        try:
            ddl_template = getattr(self.options, f"{mdf}_ddl")
        except AttributeError:
            raise ValueError(
                f"No ddl template for type: {mdf} in options "
                "(only supports csv, json or parquet)"
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
            if self.options.database_base_path:
                table_location = os.path.join(self.database_base_path, metadata.name)
            else:
                error_msg = (
                    "Either set table_location in the function "
                    "or set a database_base_path in the "
                    "GlueConverter.options.database_base_path"
                )
                raise ValueError(error_msg)

        columns = self.convert_columns(metadata)

        ddl = ddl_template(
            database=database_name,
            table=metadata.name,
            columns=columns,
            location=table_location,
            skip_header=kwargs.get("skip_header", self.options.skip_header),
            sep=kwargs.get("sep", self.options.sep),
            quote_char=kwargs.get("quote_char", self.options.quote_char),
            escape_char=kwargs.get("escape_char", self.options.escape_char),
            line_term_char=kwargs.get("line_term_char", self.options.line_term_char),
            compression=kwargs.get("compression", self.options.compression),
        )
        return ddl
