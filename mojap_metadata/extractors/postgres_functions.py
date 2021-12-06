def list_schemas(conn):
    """List non-system schemas in a database."""
    response = conn.execute(
        """
        SELECT schema_name
        FROM information_schema.schemata
        """
    ).fetchall()
    system_schemas = (
        "pg_catalog",
        "information_schema",
        "pg_toast",
        "pg_temp_1",
        "pg_toast_temp_1",
    )
    return [r[0] for r in response if r[0] not in system_schemas]


def list_tables(conn, schema="public"):
    """List tables in a database."""
    # WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'
    response = conn.execute(
        f"""
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = '{schema}'
        """
    ).fetchall()
    return [r[0] for r in response]


def list_dbs(conn):
    """List databases from a connection."""
    response = conn.execute(
        """
        SELECT datname
        FROM pg_database
        """
    ).fetchall()
    return [r[0] for r in response]


def list_meta_data(conn, table_name, schema) -> list:

    response = conn.execute(
        f""" SELECT
             cols.column_name, cols.data_type, cols.is_nullable, 
             col_description((table_schema||'.'||table_name)::regclass::oid, ordinal_position) as column_comment
            FROM information_schema.columns cols
            WHERE
                cols.table_schema  = '{schema}' AND
                cols.table_name    = '{table_name}';"""
    )
    rows = response.fetchall()
    cols = response.keys()
    return rows, list(cols)
