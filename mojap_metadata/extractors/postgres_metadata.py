from collections import defaultdict

from mojap_metadata import Metadata
import mojap_metadata.extractors.postgres_functions as pg


def convert_to_mojap_type(ct: str) -> str:
    """Converts our postgress datatypes to mojap-metadata types

    Args:
        ct (str): str representation of postgres column types

    Returns:
        str: String representation of our metadata column types
    """

    if ct in ["int8", "bigint"]:
        t = "int64"
    elif ct in ["int2", "int4", "integer", "smallint"]:
        t = "int32"
    elif ct.startswith("numeric") or ct.startswith("double precision"):
        t = "float64"
    elif (
        ct in ["text", "uuid", "character", "tsvector", "jsonb"]
        or ct.startswith("varchar")
        or ct.startswith("bpchar")
    ):
        t = "string"
    elif ct == "date":
        t = "date64"
    elif ct in ["bool", "boolean"]:
        t = "bool_"
    elif ct in ["timestamptz", "timestamp", "datetime"] or ct.startswith("timestamp"):
        t = "timestamp(ms)"
    else:
        t = "string"
        # warning

    return t


def get_object_meta(connection, table, schema) -> Metadata:
    """Extracts metadata from table and converts to mojap metadata format

    Args:
        connection: Database connection
        table: table name
        schema: schema name

    Returns:
        Metadata: Metadata object
    """

    rows = pg.list_meta_data(connection, table, schema)
    columns = []

    for col in rows[0]:

        column_type = convert_to_mojap_type(str(col[1]))
        columns.append(
            {
                "name": col[0].lower(),
                "type": column_type,
                "description": None if str(col[3]) == None else str(col[3]),
                "nullable": True if col[2] == "YES" else False,
            }
        )

    d = {"name": table, "columns": columns}

    meta_output = Metadata.from_dict(d)
    return meta_output


def get_object_meta_for_all_tables(connection):
    """Extracts metadata from all the schema and tables and returns a list
        of Metadata objects

    Args:
        connection: Database connection with database details specified in connection

    Returns:
        Metadata: Metadata object
    """
    meta_list_per_schema = defaultdict(list)

    schema_names = pg.list_schemas(
        connection
    )  # database name will be passed on in the connection

    for schema in sorted(schema_names):
        table_names = pg.list_tables(connection, schema)

        for table in table_names:
            meta_output = get_object_meta(connection, table, schema)
            meta_list_per_schema[f"schema: {schema}"].append(meta_output)

    return meta_list_per_schema
