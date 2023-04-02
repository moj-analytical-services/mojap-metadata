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
    "NUMERIC": "decimal128(18,3)",
    "NUMBER": "decimal128(18,3)",
    "DECIMAL": "decimal128(18,3)",
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
        super().__init__()
        self.connectable = connectable
        self.inspector = inspect(connectable)
        self._sqlalchemy_type_map = _sqlalchemy_type_map

    def _approx_dtype(self, txt: str) -> str:
        dtype = "string"
        for k in self._sqlalchemy_type_map:
            if txt.upper().find(k) >= 0:
                dtype = self._sqlalchemy_type_map.get(k)
                break
        return dtype
    
    def _convert_to_decimal(self, txt: str) -> str:
        dtype = re.sub(r"^.*?\(", "decimal128(", txt).replace(" ", "")
        return dtype

    def _get_table_description(self, table, schema) -> str:
        description = ""
        try:
            description = (
                self.inspector.get_table_comment(table, schema=schema)["text"] or ""
            )
        except:
            pass
        return description

    def convert_to_mojap_type(self, col_type: str) -> str:
        if col_type.startswith(("NUMERIC(", "NUMBER(", "DECIMAL(")):
            dtype = self._convert_to_decimal(col_type)
        else:
            dtype = self._approx_dtype(col_type)
        return dtype

    def generate_to_meta(self, table: str, schema: str) -> Metadata:
        rows = self.inspector.get_columns(table, schema)
        columns = []
        for col in rows:
            columns.append(
                {
                    "name": col["name"].lower(),
                    "type": self.convert_to_mojap_type(str(col["type"])),
                    "description": col.get("comment") or "",
                    "nullable": col.get("nullable"),
                }
            )
        pk = self.inspector.get_pk_constraint(table, schema)
        d = {
            "name": table,
            "columns": columns,
            "primary_key": pk["constrained_columns"],
            "database": schema,
            "description": self._get_table_description(table, schema),
        }

        meta_output = Metadata.from_dict(d)
        return meta_output


    def generate_to_meta_list(self, schema: str) -> list():
        schema_metadata = []
        table_names = self.inspector.get_table_names(schema)
        table_names = sorted(table_names)
        for table in table_names:
            table_metadata = self.generate_to_meta(table, schema)
            schema_metadata.append(table_metadata)
        return schema_metadata
