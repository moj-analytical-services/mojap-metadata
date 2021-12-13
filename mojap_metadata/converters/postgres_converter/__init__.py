from typing import DefaultDict
import sqlalchemy

from mojap_metadata import Metadata
import mojap_metadata.converters.postgres_converter.postgres_functions as pg
from mojap_metadata.converters import BaseConverter

_default_type_converter = {
    "int8": "int8",
    "int16": "int16",
    "int32": "int32",
    "int64": "int64",
    "bigint": "int64",
    "int2": "int32",
    "int4": "int32",
    "integer": "int32",
    "smallint": "int32",
    "numeric": "float64",
    "double precision": "float64",
    "text": "string",
    "uuid": "string",
    "character": "string",
    "tsvector": "string",
    "jsonb": "string",
    "varchar": "string",
    "bpchar": "string",
    "date": "date64",
    "boolean": "bool",
    "timestamptz": "timestamp(ms)",
    "timestamp": "timestamp(ms)",
    "datetime": "timestamp(ms)",
    "bool": "bool",
}


class PostgresConverter(BaseConverter):
    def __init__(self):
        """
        Extracts and converts metadata to Metadata format
        """

        super().__init__()
        self._default_type_converter = _default_type_converter

    def convert_to_mojap_type(self, col_type: str) -> str:
        """Converts our postgress datatypes to mojap-metadata types

        Args:
            ct (str): str representation of postgres column types

        Returns:
            str: String representation of our metadata column types
        """

        output = (
            "string"
            if self._default_type_converter.get(col_type) is None
            else self._default_type_converter.get(col_type)
        )
        return output

    def get_object_meta(
        self, connection: sqlalchemy.engine.Engine, table: str, schema: str
    ) -> Metadata:
        """Extracts metadata from table and converts to mojap metadata format

        Args:
            connection: Database connection
            table: table name
            schema: schema name

        Returns:
            Metadata: Metadata object
        """

        rows = pg.list_meta_data(connection, table, schema)
        columns = []

        for col in rows[0]:

            column_type = self.convert_to_mojap_type(str(col[1]))
            columns.append(
                {
                    "name": col[0].lower(),
                    "type": column_type,
                    "description": None if str(col[3]) is None else str(col[3]),
                    "nullable": True if col[2] == "YES" else False,
                }
            )

        d = {"name": table, "columns": columns}

        meta_output = Metadata.from_dict(d)
        return meta_output

    def generate_from_meta(self, connection: sqlalchemy.engine.Engine) -> dict():
        """Extracts metadata for all the schema and tables and returns a list
        of Metadata

        Args:
            connection: Database connection with database details

        Returns:
            Metadata: Metadata object
        """
        meta_list_per_schema = DefaultDict(list)

        schema_names = pg.list_schemas(
            connection
        )  # database name will be passed on in the connection

        for schema in sorted(schema_names):
            table_names = pg.list_tables(connection, schema)

            for table in table_names:
                meta_output = self.get_object_meta(connection, table, schema)
                meta_list_per_schema[f"schema: {schema}"].append(meta_output)

        return meta_list_per_schema
