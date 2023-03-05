import sqlalchemy
from sqlalchemy import text as sqlalchemy_text

def list_schemas(connection: sqlalchemy.engine.Engine):
    """List non-system schemas in a database."""
    # response = connection.execute(
    #     """
    #     SELECT schema_name
    #     FROM information_schema.schemata
    #     """
    # ).fetchall()

    sql=sqlalchemy_text("SELECT schema_name FROM information_schema.schemata")
    res = connection.execute(sql)
    response = res.fetchall()

    system_schemas = (
        "pg_catalog",
        "information_schema",
        "pg_toast",
        "pg_temp_1",
        "pg_toast_temp_1",
    )
    return [r[0] for r in response if r[0] not in system_schemas]


def list_tables(connection: sqlalchemy.engine.Engine, schema: str = "public"):
    """List tables in a database."""
    # # WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'
    # response = connection.execute(
    #     f"""
    #     SELECT tablename
    #     FROM pg_catalog.pg_tables
    #     WHERE schemaname = '{schema}'
    #     """
    # ).fetchall()
    sql=f"""
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = '{schema}'
        """
    res = connection.execute(sqlalchemy_text(sql))
    response = res.fetchall()
    return [r[0] for r in response]


def list_dbs(connection: sqlalchemy.engine.Engine):
    """List databases from a connectionection."""
    # response = connection.execute(
    #     """
    #     SELECT datname
    #     FROM pg_database
    #     """
    # ).fetchall()
    sql="SELECT datname FROM pg_database"
    res = connection.execute(sqlalchemy_text(sql))
    response = res.fetchall()
    return [r[0] for r in response]


def list_meta_data(
    connection: sqlalchemy.engine.Engine, table_name: str, schema: str
) -> list:
    """List metadata  for  table in a particular schema"""

    # response = connection.execute(
    #     f""" SELECT
    #         cols.column_name, cols.data_type,cols.is_nullable,
    #         col_description((table_schema||'.'||table_name)::regclass::oid,
    #         ordinal_position) as column_comment
    #         FROM information_schema.columns cols
    #         WHERE
    #             cols.table_schema  = '{schema}' AND
    #             cols.table_name    = '{table_name}';
    #     """
    # )

    sql=f""" SELECT
            cols.column_name, cols.data_type,cols.is_nullable,
            col_description((table_schema||'.'||table_name)::regclass::oid,
            ordinal_position) as column_comment
            FROM information_schema.columns cols
            WHERE
                cols.table_schema  = '{schema}' AND
                cols.table_name    = '{table_name}';
        """
    response = connection.execute(sqlalchemy_text(sql))
    rows = response.fetchall()
    cols = response.keys()
    return rows, list(cols)
