
# File guide
This code was originally copied from the postgres_convertor.
Currently a Work In Progress. 
**connect.py** contains `create_database_connection`, which returns a database connection for a given dialect and database/schema
**database_functions.py**  contains database functions for extracting metadata; database(schema), tables, and columns from given database/schema.
Using the Inspector class from sql-alchemy.
https://docs.sqlalchemy.org/en/20/core/reflection.html#fine-grained-reflection-with-inspector
**__init__py**  contains functions for getting metadata from tables and returning as mojap Metadata objects using mojap_metadata functionality 

