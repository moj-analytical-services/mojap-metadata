from mojap_metadata.metadata.metadata import Metadata
from typing import IO, Union, Tuple
from mojap_metadata.converters import BaseConverter, _dict_merge

from copy import deepcopy

from configs import (
    default_type_converter,
    base_glue_template,
    glue_templates,
)

default_templates

class GlueConverterOptions:
    """
    
    """

    def __init__(self):
        self._options = {
            "data_format": deepcopy(default_serde),
            "type_lookup": deepcopy(type_lookup),
            
        }

    def convert_col_type(self, coltype):
        if coltype.startswith("decimal128"):
            t, is_supported = self._default_type_converter.get("decimal128")
            brackets = coltype.split("(")[1].split(")")[0]
            t = f"{t}({brackets})"
        else:
            t, is_supported = self._default_type_converter.get(coltype, (None, None))

        self.warn_conversion(coltype, t, is_supported)

    def genereate_serde():

    def warn_conversion(
        self,
        coltype,
        converted_type,
        is_supported
    ):
    if converted_type is None:
        pass # Raise Error that this type is not supported

    if not self.ignore_warnings and not is_supported:
        pass # Raise warning that type is not fully supported by glue so using best estimate

    @property
    def options(self):
        return deepcopy(self._options)
    
    @property
    def default_serde(self):
        return self._options["default_serde"]

    @property
    def default_type_converter(type)    
    def update_default_serde(self, update_dict: dict):
        for k, v in update_dict:
            self._options[k] = "v"


class GlueConverter(BaseConverter):
    """
    Base class to be used as standard for parsing in an object, say DDL
    or oracle db connection and then outputting a Metadata class. Not sure
    if needed or will be too strict for generalisation.
    """

    def __init__(self, options={}):
        super().__init__()

        self._options["default_ddl_templates"] = {
            "csv": _create_lazy_csv_ddl,
            "json": _create_json_ddl,
            "parquet": _create_parquet_ddl,
        }
        for k, v in options.get("default_ddl_templates", {}).items():
            self._options["default_ddl_templates"][k] = v

        if "db_name" not in options:
            self._options["db_name"]: = ""

        if "table_location" not in options:
            self._options["table_location"] = ""


    @property
    def options(self):
        return deepcopy(self._options)

    @property
    def list_options(self):
        return list[self._options.keys()]

    def convert_col_type(self, coltype):
        if coltype.startswith("decimal128"):
            t, is_supported = self._default_type_converter.get("decimal128")
            brackets = coltype.split("(")[1].split(")")[0]
            t = f"{t}({brackets})"
        else:
            t, is_supported = self._default_type_converter.get(coltype, (None, None))

        self.warn_conversion(coltype, t, is_supported)
        return t

    def convert_columns(self, metadata: Metadata):
        self._check_meta_set()
        cols = []

        for c in metadata.columns:
            cols.append({
                "name": c["name"],
                "type": self.convert_col_type(c["type"]),
                "description": c["description"],
                "partition": c["name"] in metadata.partitons
            })

        return cols
    

    def _create_column_definition(self, columns) -> Tuple[str, str]:
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


    def generate_from_meta(
        self,
        metadata: Metadata,
        database_name=None,
        table_location=None,
        **kwargs
    ):

    mdf = metadata.data_format
    ddl_template = self._options["default_ddl_templates"].get(mdf)
    if ddl_template is None:
        available_types = self._options["default_ddl_templates"].keys()
        raise ValueError(f"No ddl template for type: {mdf}. Only supports {available_types}.")
    if not database_name:
        database_name = self.database_name
    
    if not table_location:
        table_location = os.path.join(self.database_base_path, metadata.name)
    
    columns = self.convert_columns(metadata)

    ddl = ddl_template(
        database=database_name,
        table=metadata.name,
        columns=columns,
        location=table_location,
        **kwargs
    )
    return ddl
    

def _create_start_of_ddl(:
    database: str,
    table: str,
    columns: list,
):

    cols, part = _create_column_definition(columns)
    if part:
        partition_section = (
            "PARTITIONED BY ( "
            part
            ")"
        )
    else:
        partition_section = ""

    ddl = f"""
    CREATE EXTERNAL TABLE {database}.{table} (
    {cols}
    )
    {part}
    """
    return ddl


def _create_lazy_csv_ddl(
    database: str,
    table: str,
    columns: list,
    location: str,
    skip_header=False,
    sep = ",",
    quote_char = '"',
    escape_char = "\\",
    line_term_char = "\n",
    **kwargs
):
    ddl_start = _create_start_of_ddl(database, table, columns)

    table_properties = ""
    if skip_header:
        table_properties += (
            "TBLPROPERTIES (\n"
            '"skip.header.line.count"="1"\n'
            ")"
        )

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


def _create_open_csv_ddl(
    database: str,
    table: str,
    columns: list,
    location: str,
    skip_header=False,
    sep = ",",
    quote_char = '"',
    escape_char = "\\",
    **kwargs
):
    ddl_start = _create_start_of_ddl(database, table, columns)

    table_properties = ""
    if skip_header:
        table_properties += (
            "TBLPROPERTIES (\n"
            '"skip.header.line.count"="1"\n'
            ")"
        )

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


def _create_json_ddl(
    database: str,
    table: str,
    columns: list,
    location: str,
    **kwargs
):
    ddl_start = _create_start_of_ddl(database, table, columns)
    
    json_col_paths = ",".join(
        [c["name"] for c in columns if not c["partition"]]
    )
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


def _create_parquet_ddl(
    database: str,
    table: str,
    columns: list,
    location: str,
    compression="SNAPPY",
    **kwargs
):
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
