# Postgres Converter

Postgres Converter provides the following functionality

1. Conenction to postgres database
2. Extract the metadata from the tables
3. Convert the extracted ouptut into Metadata object 

- **get_object_meta** (function) takes the table name, schema name then the extracts the metadata from postgres database and 
converts into Metadata object 

- **generate_to_meta:** (function) takes the database connection and returns a list of Metadata object for all the (non-system schemas) schemas and tables from the connection.

**NOTE:** the sqlalchemy converter is more robust and should be the default method for most databases, but the postgres converter is retained for compatibility


## File guide

### connect.py
contains `create_postgres_connection`, which returns anpPostgres database connection

### postgres_functions.py
contains postgres functions for extracting database, tables, schema and  metadata from postgres database 

### __init__.py
contains functions for getting metadata from tables and returning as mojap Metadata objects using mojap_metadata functionality 

