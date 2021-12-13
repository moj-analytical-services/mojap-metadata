from sqlalchemy import create_engine


def create_postgres_connection(db_settings: dict, database_name: str = "postgres"):
    """Create SQL Alchemy engine for a postgres database.

    Get connection details from file specified in config.json

    Parameters
    ----------
    db_settings (dict):
        Dictionary of database connection details.

    database_name (str):
        Specify a database to connect to within the RDS instance.
    """
    user = db_settings["user"]
    password = db_settings["password"]
    host = db_settings["host"]
    port = db_settings["port"]
    database_name = database_name
    print(f"Connecting to database {database_name}")

    # See SQLAlchemy docs for creating the db_string
    db_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database_name}"
    return create_engine(db_string, echo=True)  # verbose output (sqlalchemy logging)
