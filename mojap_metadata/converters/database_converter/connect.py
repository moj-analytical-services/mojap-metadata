from sqlalchemy import create_engine


def create_database_connection(
        db_settings: dict, 
        database_name: str, 
        database_dialect_driver: str
        ):
    
    """Create SQL Alchemy engine for a postgres database.

    Get connection details from file specified in config.json

    Parameters
    ----------
    db_settings (dict):
        Dictionary of database connection details.

    database_name (str):
        Specify a database to connect to within the RDS instance.

    database_dialect (str):
        Specify a database dialect for connection string construction.

    example, where the required database name is 'mydata' for a database whose dialect is postgres and the driver is psycopg2: 
        database_name: str = "mydata", 
        database_dialect_driver: str = "postgresql+psycopg2"

    !! NOTE !! accepted dialect is 'postgresql' NOT 'postgres'!!
    
    """
    user = db_settings["user"]
    password = db_settings["password"]
    host = db_settings["host"]
    port = db_settings["port"]
    database_name = database_name
    print(f"Connecting to database {database_name} with dialect and driver {database_dialect_driver}")
    
    # See SQLAlchemy docs for creating the db_string
    #     dialect+driver://username:password@host:port/database
    
    db_string = f"{database_dialect_driver}://{user}:{password}@{host}:{port}/{database_name}"
    
    return create_engine(db_string, echo=True)  # verbose output (sqlalchemy logging)
