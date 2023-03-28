import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy.engine.reflection import Inspector

""" see https://docs.sqlalchemy.org/en/20/core/reflection.html#fine-grained-reflection-with-inspector
"""

def list_tables(engine: sqlalchemy.engine.Engine, schema: str = "public") -> list:
    """ List tables in a database.
        method: sqlalchemy.engine.reflection.Inspector.get_table_names(schema: Optional[str] = None, **kw: Any) → List[str]
    """
    insp = inspect(engine)
    response = insp.get_table_names(schema)
    return [r for r in response]


def list_meta_data(engine: sqlalchemy.engine.Engine, tableName: str, schema: str ) -> list:
    """ List metadata for table in the schema declared in the connection
        https://docs.sqlalchemy.org/en/20/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_columns
    """
    insp = inspect(engine)
    return insp.get_columns(tableName, schema)


def get_constraint_pk(engine: sqlalchemy.engine.Engine, tableName: str, schema: str = "public") -> dict:
    """ List Primary Keys from Inspector Method 
        https://docs.sqlalchemy.org/en/14/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_pk_constraint
        returns dictionary, {"constrained_columns":[],"name":'*optional'}
    """
    insp = inspect(engine)
    return insp.get_pk_constraint(tableName, schema)
    

def list_primary_keys(tableSchema: list) -> list:
    """ Extract Primary Keys from schema 
    arg. tableSchema: return list from SQLAlchemy inspector method, get_columns
    """
    if any(d['primary_key'] == 1 for d in tableSchema):
        pk = [d for d in tableSchema if d['primary_key'] == 1 ]
    else:
        pk = []
    return pk