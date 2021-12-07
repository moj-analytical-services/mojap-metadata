def list_schemas(connection):
    """List non-system schemas in a database."""
    response = connection.execute(
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


def list_tables(connection, schema="public"):
    """List tables in a database."""
    # WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'
    response = connection.execute(
        f"""
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = '{schema}'
        """
    ).fetchall()
    return [r[0] for r in response]


def list_dbs(connection):
    """List databases from a connectionection."""
    response = connection.execute(
        """
        SELECT datname
        FROM pg_database
        """
    ).fetchall()
    return [r[0] for r in response]


def list_meta_data(connection, table_name, schema) -> list:
    """List metadata  for  table in a particular schema"""
    response = connection.execute(
        """ SELECT c.column_name, c.data_type, c.is_nullable, """
        """ col_description((table_schema||'.'||table_name)::regclass::oid, ordinal_position) as column_comment"""
        """ FROM information_schema.columns c
            WHERE c.table_schema='{schema}' AND c.table_name='{table_name}';"""
    )
    rows = response.fetchall()
    cols = response.keys()
    return rows, list(cols)
