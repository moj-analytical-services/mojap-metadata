
# File guide
_2023-03-30 This code was originally copied from the postgres_convertor._

File list;
- `__int__.py`
- `sqlalchemy_functions.py`

also see tests/; 
- `test_sqlalchemy_converter_postgres.py` 
- `test_sqlalchemy_converter_sqlite.py`

## SQL-Alchemy 
Version 1.4, (version 2 is current)

**sqlalchemy_functions.py**  contains database functions for extracting metadata for a given database(schema). 


Using the Inspector class from sql-alchemy.

https://docs.sqlalchemy.org/en/20/core/reflection.html#fine-grained-reflection-with-inspector

This approach was current as of v1.4, and confirms to v2 standard.

## Methods

**`__init__.py`**  contains functions for getting metadata from tables and returning as mojap Metadata objects using mojap_metadata functionality.

Cornerstone method returning all tables, and columns from given database/schema.


    def generate_from_meta(
            self,
            connection: sqlalchemy.engine.Engine,
            schema: str) -> dict():
        """ For all the schema and tables and returns a list of Metadata
        Args:...... connection: Database connection with database details
        Returns:... Metadata: Metadata object
        """



## Connection
*connect.py*, `create_database_connection()` method has been removed. 
Users of the new class `sqlalchemy_converter ` will need to provide database connection for a given dialect and database/schema

### See SQLAlchemy docs for creating the db_string
    dialect+driver://username:password@host:port/database
    
    db_string = f"{database_dialect_driver}://{user}:{password}@{host}:{port}/{database_name}"
    
    engine = sqlalchemy.create_engine(db_string, echo=True) 


### Note: for creating SQL Alchemy engine for a specified database.

Parameters
----------

- database_name (str):
Specify a database to connect to within the RDS instance.

- database_dialect (str):
Specify a database dialect for connection string construction.

Example, where the required database name is 'mydata' for a database whose dialect is postgres and the driver is psycopg2: 

    database_name: str = "mydata", 
    database_dialect_driver: str = "postgresql+psycopg2"

**Accepted dialect is 'postgresql' NOT 'postgres'!!**

### Notes on Oracle

dialect + driver = `oracle+oracledb`

"oracledb" has replace "oracle_cx" drivers.   

To create an oracle SQLAlchemy.engine

    import sys
    import oracledb

    oracledb.version = "8.3.0"
    sys.modules["cx_Oracle"] = oracledb

## Data Types

SQL-Alchemy converts specific dialects into a common type varient. 

Therefore, SQL-Alchemy has it's own Type definitions: `sqlalchemy.sql.sqltypes`
https://docs.sqlalchemy.org/en/14/core/type_basics.html#generic-camelcase-types
    
These are the types that are returned for v1.4. Version 2 has a bigger and more diverse list.
We have kept most of the previous datatypes but added binary types.

DMS and the compare, require a specific set of definitions differing from what sql-alchemy outputs,
we define the convertion mappings here.

    Note. Mapping convertions for types. 
     -> SQL-Alchemy  >> _sqlalchemy_type_map


The default return value is 'string'.

This is returned in the event the type received back from SQL-Alchemy is not found.

Specific types can be instaciated using:
`class sqlalchemy.types.TypeEngine`

### Notes on CASE 
Oracle objects, such as tables and data typee are Uppercase, postgres are lower or camelcase, SQLServer is often a mix. Sometimes they can be case insensitive. Therefore,SQLAlchemy 

