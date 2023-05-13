from typing import Union
from dataclasses import dataclass

from sqlalchemy import inspect
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.types import (
    SmallInteger,
    Integer,
    BigInteger,
    REAL,
    Float,
    Numeric,
    String,
    DateTime,
    Date,
    Boolean,
    _Binary,
)

from mojap_metadata import Metadata
from mojap_metadata.converters import BaseConverter

_sqlalchemy_type_map = {
    SmallInteger: "int16",
    BigInteger: "int64",
    Integer: "int32",
    REAL: "float16",
    Float: "float64",
    String: "string",
    DateTime: "timestamp(ms)",
    Date: "date64",
    Boolean: "bool",
    _Binary: "binary",
}


@dataclass
class SQLAlchemyConverterOptions:
    default_decimal_precision: int = 38
    default_decimal_scale: int = 10


class SQLAlchemyConverter(BaseConverter):
    def __init__(
        self,
        connectable: Union[Engine, Connection],
        options: SQLAlchemyConverterOptions = None,
    ):
        """_Converts SQLAlchemy DDL to metadata object_

        Args:
            connectable (Union[Engine, Connection]): _A SQLAlchemy Engine or Connection_
        """
        if options is None:
            options = SQLAlchemyConverterOptions()

        super().__init__(options)
        self.connectable = connectable
        self.inspector = inspect(connectable)
        self._sqlalchemy_type_map = _sqlalchemy_type_map

    def _get_dtype(self, input_type) -> str:
        dtype = "string"
        for i, k in _sqlalchemy_type_map.items():
            if isinstance(input_type, i):
                dtype = k
                break
        return dtype

    def _convert_to_decimal(self, col_type) -> str:
        if col_type.precision is None:
            dp = self.options.default_decimal_precision
            ds = self.options.default_decimal_scale
            return f"decimal128({dp},{ds})"
        elif col_type.scale is None:
            return f"decimal128({str(col_type.precision)},0)"
        else:
            return f"decimal128({str(col_type.precision)},{str(col_type.scale)})"

    def _get_table_description(self, table, schema) -> str:
        description = ""
        try:
            description = (
                self.inspector.get_table_comment(table, schema=schema)["text"] or ""
            )
        except NotImplementedError:
            pass
        return description

    def convert_to_mojap_type(self, col_type) -> str:
        """_Converts a SQLAlchemy data type into a mojap data type_

        Args:
            col_type (_type_): _A SQLAlchemy data type_

        Returns:
            str: _A mojap data type_
        """
        if isinstance(col_type, Numeric) and not isinstance(col_type, Float):
            dtype = self._convert_to_decimal(col_type)
        else:
            dtype = self._get_dtype(col_type)
        return dtype

    def generate_to_meta(self, table: str, schema: str) -> Metadata:
        """_Generates a Metadata object from a specified table and schema_

        Args:
            table (str): _Table name to generate the metadata for_
            schema (str): _Schema name to generate the metadata for_

        Returns:
            Metadata: _Metadata object_
        """
        rows = self.inspector.get_columns(table, schema)
        columns = []
        for col in rows:
            columns.append(
                {
                    "name": col["name"].lower(),
                    "type": self.convert_to_mojap_type(col["type"]),
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

    def generate_to_meta_list(self, schema: str) -> list:
        """_Generates a list of Metadata objects for all the tables in a schema_

        Args:
            schema (str): _Schema name to generate the metadata for_

        Returns:
            list: _list of Metadata objects_
        """
        schema_metadata = []
        table_names = self.inspector.get_table_names(schema)
        table_names = sorted(table_names)
        for table in table_names:
            table_metadata = self.generate_to_meta(table, schema)
            schema_metadata.append(table_metadata)
        return schema_metadata
