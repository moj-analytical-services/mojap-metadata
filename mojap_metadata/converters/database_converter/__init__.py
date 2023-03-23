""" Database convertor class

    Convertor for data types:
    SQL-Alchemy has it's own Type definitions: sqlalchemy.sql.sqltypes
    https://docs.sqlalchemy.org/en/20/core/type_basics.html#types-sqlstandard
    These are the types that are returned.
    The DMS requires a specific set of definitions differing from what sql-alchemy outputs, we define the convertion mappings here.
    
    Note. Mapping convertions for types. 
     -> SQL-Alchemy  >> _sqlalchemy_type_map

    class sqlalchemy.types.TypeEngine
"""

from typing import DefaultDict
import sqlalchemy
from sqlalchemy.sql import sqltypes

from mojap_metadata import Metadata
import mojap_metadata.converters.database_converter.database_functions as dbfun
from mojap_metadata.converters import BaseConverter

_sqlalchemy_type_map = {
    "SMALLINT": "int16",
    "BIGINT": "int64",
    "INTEGER": "int32",
    "INT": "int32",
    "REAL": 'float24',
    "DOUBLE_PRECISION": "float64",
    "DOUBLE PRECISION": "float64",
    "DOUBLE": "float32",
    "FLOAT":"float64",
    "NUMERIC": "float64",
    "DECIMAL": "decimal",
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
    "BLOB": "blob",
    "CLOB": "clob",
    "VARBINARY": "binary",
    "BINARY": "binary"
}

class DatabaseConverter(BaseConverter):
    def __init__(self):
        """ Extracts and converts metadata to Metadata format
        """
        super().__init__()
        self._sqlalchemy_type_map = _sqlalchemy_type_map

    def _approx_dtype(self, txt: str)-> str:
        """ Find approximate data type value of SQL-Alchemy data type, often supplied with precision value that can't be matched.
            Args:...... txt: SQL-Alchemy data type, with precision value
            Returns:... string: mapped data type, from _sqlalchemy_type_map, default 'string'
        """
        dtype='string'
        for k in self._sqlalchemy_type_map:
            if txt.find(k) >= 0:
                dtype=self._sqlalchemy_type_map.get(k)
                break
        return dtype


    def convert_to_mojap_type(self, col_type: str) -> str:
        """ Converts SQL-Alchemy datatypes to mojap-metadata types
            Args:       ct (str):   String representation of source column types
            Returns:    str:        String representation of metadata column types
        """
        # cType = self._sqlalchemy_type_map.get(col_type)
        # return "string" if cType is None else cType
        return self._approx_dtype(col_type)


    def get_object_meta(
        self, connection: sqlalchemy.engine.Engine, table: str, schema: str
    ) -> Metadata:
        """ for a table, get metadata and convert to mojap metadata format.
            Convert sqlalchemy inpector result.
        Args:...... connection: Database connection
            ....... table: table name
            ....... schema: schema name
        Returns:... Metadata: Metadata object
        """
        rows = dbfun.list_meta_data(connection, table, schema)
        columns = []
        for col in rows:
            columns.append(
                {
                    "name": col['name'].lower(),
                    "type": self.convert_to_mojap_type(str(col['type'])),
                    "description": col.get('comment') if col.get('comment') else 'None',
                    "nullable": col.get('nullable'),
                }
            )
        d = {"name": table, "columns": columns}
        meta_output = Metadata.from_dict(d)
        return meta_output

    def generate_from_meta(self, connection: sqlalchemy.engine.Engine, schema: str) -> dict():
        """ For all the schema and tables and returns a list of Metadata
        Args:...... connection: Database connection with database details
        Returns:... Metadata: Metadata object
        """
        meta_list = DefaultDict(list)
        table_names = dbfun.list_tables(connection, schema)
        for table in table_names:
            meta_output = self.get_object_meta(connection, table, schema)
            meta_list[f"schema: {schema}"].append(meta_output)
        return meta_list


