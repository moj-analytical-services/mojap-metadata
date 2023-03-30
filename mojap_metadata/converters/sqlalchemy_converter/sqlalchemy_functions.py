import sqlalchemy
from sqlalchemy import inspect

""" see notes for v2
    https://docs.sqlalchemy.org/en/20/core/reflection.html#fine-grained-reflection-with-inspector
    NOTE:
    Deprecated since version 1.4:
    The __init__() method on Inspector is deprecated ... removed in a future release.
    Please use inspect() function on an Engine or Connection to acquire an Inspector.
"""


def list_tables(engine: sqlalchemy.engine.Engine, schema: str = "public") -> list:
    """ List tables in a database.
        https://docs.sqlalchemy.org/en/20/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_table_names
        returns list of strings.
    """
    insp = inspect(engine)
    response = insp.get_table_names(schema)
    return [r for r in response]


def list_meta_data(
        engine: sqlalchemy.engine.Engine,
        tableName: str,
        schema: str) -> list:
    """ List metadata for table in the schema declared in the connection
        https://docs.sqlalchemy.org/en/20/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_columns
        returns list of dictionaries
    """
    insp = inspect(engine)
    return insp.get_columns(tableName, schema)


def get_constraint_pk(
        engine: sqlalchemy.engine.Engine,
        tableName: str,
        schema: str = "public") -> dict:
    """ List Primary Keys from Inspector Method
        https://docs.sqlalchemy.org/en/14/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_pk_constraint
        returns dictionary, {"constrained_columns":[],"name":'*optional'}
    """
    insp = inspect(engine)
    return insp.get_pk_constraint(tableName, schema)
