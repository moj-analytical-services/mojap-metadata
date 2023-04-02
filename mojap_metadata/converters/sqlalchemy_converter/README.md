# SQLAlchemy Converter

[Functions](#functions)
[Connection](#connection)
[Data Types](#data-types)
[Dialects](#database-dialects)

Uses the SQLAlchemy [`Inspector`](https://docs.sqlalchemy.org/en/20/core/reflection.html#fine-grained-reflection-with-inspector) class to:

1. Extract metadata from database dialects supported by [SQLAlchemy](https://docs.sqlalchemy.org/en/20/dialects/index.html#dialects)
2. Convert the extracted output into a mojap `Metadata` object

## Functions

- **convert_to_mojap_type()** converts a SQLAlchemy data type into a mojap `Metadata` data type 

- **generate_to_meta()** extracts the metadata for a given table and schema name and returns a `Metadata` object 

- **generate_to_meta_list()** returns a list of `Metadata` objects for all the tables in a given schema sorted by table name

`Inspector` comes with many functions to extract metadata such as [`get_schema_names()`](https://docs.sqlalchemy.org/en/20/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_schema_names).
These functions are **not** recreated in order to limit boiler plate code and maintenance. You can use the `inpector` object which is instantiated by `SQLAlchemyConverter` instead of creating your own one.

## Connection

You will need to provide a SQLAlchemy database engine or connection for a given dialect and database when instantiating a `SQLAlchemyConverter` object for example:

    ```
    from sqlalchemy import create_engine
    from mojap_metadata.converters.sqlalchemy_converter import SQLAlchemyConverter

    engine = create_engine("postgresql+psycopg2://scott:tiger@localhost:5432/mydatabase")
    sqlc = SQLAlchemyConverter(engine)
    ```

See [Engine Configuration](https://docs.sqlalchemy.org/en/20/core/engines.html) for more details and how to configure for the different database dialects.

### Notes on Oracle

"oracledb" has replaced "oracle_cx" drivers.

To create an oracle SQLAlchemy.engine:

    ```
    import sys
    import oracledb

    oracledb.version = "8.3.0"
    sys.modules["cx_Oracle"] = oracledb
    engine = create_engine("oracle://scott:tiger@127.0.0.1:1521/sidname")
    ```

## Data types

SQLAlchemy converts specific dialects into a common type varient. 

Therefore, SQLAlchemy has its own Type definitions [`sqlalchemy.sql.sqltypes`](https://docs.sqlalchemy.org/en/14/core/type_basics.html#generic-camelcase-types).
    
These are the types that are returned for v1.4. Version 2 has a bigger and more diverse list.
We have kept most of the previous datatypes but added binary types.

### Type approximation
There is a class private method called `_approx_dtype`. It's important to note that when adding to the map list, where the value appears matters and affects the output. This method could be more optimal, but for the sake of simplicity, it iterates through the list and attempts a substring search. Therefore the list should start with the most complex/unique variant first and then be followed by the more simplistic varient. 

    "DOUBLE PRECISION": "float64",
    "DOUBLE": "float32",
If this was the other way around, 'double' would match to a returning 'double_precision()' and there would be an incorrect type conversion. 

### Type conflation
In the oracle documentation it conflates int, smallint, numeric, number and decimal. There some inter-operability that makes it confusing. https://www.oracletutorial.com/oracle-basics/oracle-number-data-type/

1. NUMBER(precision=9, scale=0, asdecimal=False) - should return **integer**
2. NUMBER(precision=10, scale=2, asdecimal=True) - should return **decimal**

SQLAlchemy v1.4 does appear to handle this. Where the 'asdecimal' flag triggers a SQLAlchemy type switch.

### Default type
In the event the data-type received back from SQL-Alchemy is not found, the default return value from the private method `_approx_dtype` is 'string'.

### Notes on CASE 
Oracle objects, such as tables and data typee are Uppercase, postgres are lower or camelcase, SQLServer is often a mix. Sometimes they can be case insensitive.

## Database Dialects

The `SQLAlchemyConverter` is tested against three different SQLAlchemy database engines in [`test_sqlachemy.py`](/tests/test_sqlalchemy.py):

1. sqlite
2. duckdb
3. postgres

Whilst all three return a `Metadata` objects with broadly the same features, there are differences. This is because whilst `Inspector` provides a consistent interface, a feature may not be supported by the database or by the sqlalchemy dialect. For example only the postgres dialect recognises the table comment. For more examples of differences have a look at the parameters passed in to `test_generate_to_meta()`.
