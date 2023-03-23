import sqlalchemy
from sqlalchemy import inspect

""" see https://docs.sqlalchemy.org/en/20/core/reflection.html#fine-grained-reflection-with-inspector
"""

def list_tables(connection: sqlalchemy.engine.Engine, schema: str = "public") -> list:
    """ List tables in a database.
        method: sqlalchemy.engine.reflection.Inspector.get_table_names(schema: Optional[str] = None, **kw: Any) → List[str]
    """
    insp = inspect(connection)
    response = insp.get_table_names(schema)
    return [r for r in response]


def list_meta_data(connection: sqlalchemy.engine.Engine, table_name: str, schema: str ) -> list:
    """ List metadata for table in the schema declared in the connection
        https://docs.sqlalchemy.org/en/20/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_columns
    """
    insp = inspect(connection)
    return insp.get_columns(table_name, schema)


def list_primary_keys(tableSchema: list) -> list:
    """ Extract Primary Keys from schema 
    """
    if any(d['primary_key'] == 1 for d in tableSchema):
        pk = [d for d in tableSchema if d['primary_key'] == 1 ]
    else:
        pk = []
    return pk