import os

from jinja2 import Template

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


def generate_ddl_from_template(
    template: str,
    database: str,
    table: str,
    columns: list,
    partitions: list,
    location: str,
    **kwargs
):
    """generates a HIVE/Glue DDL from a template.

    Args:
        template (str): Jinja template as a string
        database (str): database name
        table (str): table name
        columns (list): List of dictionaries must have name, type
          and description key value bindings
        partitions (list): List of dictionaries must have name, type
          and description key value bindings
        location (str): path to table in S3
        
        **kwargs additional arguments passed to template via Jinja
    """
    t = Template(template)
    ddl = t.render(
        database=database,
        table=table,
        columns=columns,
        partitions=partitions,
        location=location,
        **kwargs,
    )
    return ddl


def create_lazy_csv_ddl(
    database: str,
    table: str,
    columns: list,
    location: str,
    skip_header=False,
    sep=",",
    quote_char='"',
    escape_char="\\\\",
    line_term_char="\n",
    **kwargs
):
    """
    Creates a DDL for CSV data using the Lazy serde
    """
    ddl_start = _create_start_of_ddl(database, table, columns)

    ddl_end = (
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'\n"
        "ROW FORMAT DELIMITED\n"
        f"FIELDS TERMINATED BY '{sep}'\n"
        "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'\n"
        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n"
        f"LOCATION '{location}'\n"
    )

    if skip_header:
        ddl_end += "TBLPROPERTIES (\n"
        ddl_end += "'skip.header.line.count'='1'\n"
        ddl_end += ")\n"

    ddl_end += ";"

    ddl = ddl_start + ddl_end
    return ddl


# def create_open_csv_ddl(
#     database: str,
#     table: str,
#     columns: list,
#     location: str,
#     skip_header=False,
#     sep=",",
#     quote_char='"',
#     escape_char="\\",
#     **kwargs,
# ):
#     """
#     Creates a DDL for CSV data using the OpenCSV serde
#     """
#     ddl_start = _create_start_of_ddl(database, table, columns)

#     table_properties = ""
#     if skip_header:
#         table_properties += "TBLPROPERTIES (\n" '"skip.header.line.count"="1"\n' ")"

#     ddl_end = f"""
#     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
#     WITH SERDEPROPERTIES (
#     'separatorChar' = '{sep}',
#     'quoteChar' = '{quote_char}',
#     'escapeChar' = '{escape_char}'
#     )
#     STORED AS INPUTFORMAT
#         'org.apache.hadoop.mapred.TextInputFormat'
#     OUTPUTFORMAT
#         'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
#     LOCATION
#     '{location}'
# #     {table_properties};
# #     """
# #     ddl = ddl_start + ddl_end
# #     return ddl


# def create_json_ddl(database: str, table: str, columns: list, location: str, **kwargs):
#     """
#     Creates a DDL for a JSON data
#     """
#     ddl_start = _create_start_of_ddl(database, table, columns)

#     json_col_paths = ",".join([c["name"] for c in columns if not c["partition"]])
#     ddl_end = f"""
#     ROW FORMAT SERDE
#         'org.apache.hive.hcatalog.data.JsonSerDe'
#     WITH SERDEPROPERTIES (
#         'paths'='{json_col_paths}')
#     STORED AS INPUTFORMAT
#         'org.apache.hadoop.mapred.TextInputFormat'
#     OUTPUTFORMAT
#         'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
#     LOCATION
#         '{location}'
#     TBLPROPERTIES (
#         'classification'='json'
#     )
#     """
#     ddl = ddl_start + ddl_end
#     return ddl


# def create_parquet_ddl(
#     database: str,
#     table: str,
#     columns: list,
#     location: str,
#     compression="SNAPPY",
#     **kwargs,
# ):
#     """
#     Creates a DDL for a PARQUET data
#     """

#     ddl_start = _create_start_of_ddl(database, table, columns)

#     table_properties = "'classification'='parquet'"
#     if compression:
#         table_properties += ",\n"
#         table_properties += f"'parquet.compression'='{compression}'"

#     ddl_end = f"""
#     ROW FORMAT SERDE
#         'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
#     STORED AS INPUTFORMAT
#         'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
#     OUTPUTFORMAT
#         'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
#     LOCATION
#         '{location}'
#     TBLPROPERTIES (
#         {table_properties}
#     );
#     """
#     ddl = ddl_start + ddl_end
#     return ddl
# 
# 
# def get_ddl_template(name):
#     if file_format.startswith("csv"):
#         if csv_option == "lazy":
#             return create_lazy_csv_ddl
#         elif csv_option == "open":
#             return create_open_csv_ddl
#         else:
#             raise ValueError(
#                 f"csv_option must be 'lazy' or 'open' not {csv_option}"
#             )

#     elif file_format.startswith("json"):
#         return create_json_ddl

#     elif file_format.startswith("parquet"):
#         return create_parquet_ddl

#     else:
#         raise ValueError(
#             f"No ddl template for type: {file_format} in options "
#             "(only supports formats starting with csv, json or parquet)"
#         )


@dataclass
class GlueConverterOptions:
    """
    Options Class for the GlueConverter

    csv_template (str):
      Jinja template that is used to generate CSV ddl. Defaults to
      lazy serde template. To use the open serde template you can use
      options.set_csv_serde("open").

    json_template (str):
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

    compression (str):
      parameter to parquet ddl function
    """
    csv_template = get_default_ddl_template("lazy_csv_ddl")
    json_template = get_default_ddl_template("json_ddl")
    parquet_template = get_default_ddl_template("parquet_ddl")
    default_db_name: str = None
    default_db_base_path: str = None
    ignore_warnings: bool = False
    skip_header: bool = False
    sep: str = ","
    quote_char: str = '"'
    escape_char: str = r"\\"
    line_term_char: str = r"\n"
    compression: str = "SNAPPY"

    def set_csv_serde(self, serde_name: str):
        allowed_serde_name = ["open", "lazy"]
        if serde_name not in allowed_serde_name:
            raise ValueError(
                f"Input serde_name must be one of {allowed_serde_name}. "
                f"Got {serde_name}"
            )
        self.csv_template = get_default_ddl_template(f"{serde_name}_csv_ddl")


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
        cols = []

        for c in metadata.columns:
            cols.append(
                {
                    "name": c["name"],
                    "type": self.convert_col_type(c["type"]),
                    "description": c.get("description", ""),
                    "is_partition": c["name"] in metadata.partitions,
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
