from typing import DefaultDict
import re
import sqlalchemy
from sqlalchemy import inspect

from mojap_metadata import Metadata
from mojap_metadata.converters import BaseConverter

_sqlalchemy_type_map = {
    "SMALLINT": "int16",
    "BIGINT": "int64",
    "INTEGER": "int32",
    "INT": "int32",
    "REAL": "float16",
    "DOUBLE_PRECISION": "float64",
    "DOUBLE PRECISION": "float64",
    "DOUBLE": "float32",
    "FLOAT": "float64",
    "TEXT": "string",
    "UUID": "string",
    "NCHAR": "string",
    "NVARCHAR": "string",
    "VARCHAR": "string",
    "CHAR": "string",
    "JSON": "string",
    "TIMESTAMP": "timestamp(ms)",
    "DATETIME": "datetime",
    "DATE": "date64",
    "TIME": "timestamp(ms)",
    "BOOLEAN": "bool",
    "BOOL": "bool",
    "BLOB": "binary",
    "CLOB": "binary",
    "LARGEBINARY": "binary",
    "BYTEA": "binary",
    "VARBINARY": "binary",
    "BINARY": "binary",
}


class SQLAlchemyConverter(BaseConverter):
    def __init__(self, connectable):
        """Extracts and converts metadata to Metadata format"""
        super().__init__()
        self.connectable = connectable
        self.inspector = inspect(connectable)
        self._sqlalchemy_type_map = _sqlalchemy_type_map

    def _approx_dtype(self, txt: str) -> str:
        """Find approximate data type value of SQL-Alchemy data type,
        often supplied with precision value that can't be matched.
        Args:......
            txt: SQL-Alchemy data type, with precision value
        Returns:...
            string: mapped data type, from _sqlalchemy_type_map, default 'string'
        """
        dtype = "string"
        for k in self._sqlalchemy_type_map:
            if txt.upper().find(k) >= 0:
                dtype = self._sqlalchemy_type_map.get(k)
                break
        return dtype

    def convert_to_mojap_type(self, col_type: str) -> str:
        """Converts SQL-Alchemy datatypes to mojap-metadata types
        Args:       ct (str):   String representation of source column types
        Returns:    str:        String representation of metadata column types
        """
        if col_type.startswith(("NUMERIC(", "NUMBER(", "DECIMAL(")):
            dtype = re.sub(r"^.*?\(", "decimal128(", col_type).replace(" ", "")
        else:
            dtype = self._approx_dtype(col_type)
        return dtype

    def get_object_meta(self, table: str, schema: str) -> Metadata:
        """for a table, get metadata and convert to mojap metadata format.
            Convert sqlalchemy inpector result.
        Args:...... connection: Database connection, SQL Alchemy
            ....... table: table name
            ....... schema: schema name
        Returns:... Metadata: Metadata object
        """
        rows = self.inspector.get_columns(table, schema)
        columns = []
        for col in rows:
            columns.append(
                {
                    "name": col["name"].lower(),
                    "type": self.convert_to_mojap_type(str(col["type"])),
                    "description": col.get("comment") if col.get("comment") else "None",
                    "nullable": col.get("nullable"),
                }
            )
        pk = self.inspector.get_pk_constraint(table, schema)
        d = {
            "name": table,
            "columns": columns,
            "primary_key": pk["constrained_columns"],
        }

        meta_output = Metadata.from_dict(d)
        return meta_output

    def generate_from_meta(self, schema: str) -> dict():
        """For all the schema and tables and returns a list of Metadata
        Args:...... connection: Database connection with database details
        Returns:... Metadata: Metadata object
        """
        meta_list = DefaultDict(list)
        table_names = self.inspector.get_table_names(schema)
        for table in table_names:
            meta_output = self.get_object_meta(table, schema)
            meta_list[f"schema: {schema}"].append(meta_output)
        return meta_list
