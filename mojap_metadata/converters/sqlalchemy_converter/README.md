## SQLAlchemy Converter 

Uses the SQLAlchemy [Inspector](https://docs.sqlalchemy.org/en/20/core/reflection.html#fine-grained-reflection-with-inspector) class to:

1. Extract metadata from database dialects supported by [SQLAlchemy](https://docs.sqlalchemy.org/en/20/dialects/index.html#dialects)
2. Convert the extracted ouptut into a `Metadata` object

## Functions

- **convert_to_mojap_type()** converts a SQLAlchemy data type into the generic mojap Metadata data type 

- **get_object_meta()** extracts the metadata and returns a `Metadata` object for a given table and schema name

- **generate_to_meta()** returns a list of `Metadata` objects for all the tables in a given schema name

Functions which already exist in Inspector such [`get_schema_names()`](https://docs.sqlalchemy.org/en/20/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_schema_names) are **not** recreated in order to limit boiler plate code.

## Connection

You will need to provide a SQLAlchemy database engine or connection for a given dialect and database when instantiating a `SQLAlchemyConverter` object.

    ```
    from sqlalchemy import create_engine

    engine = create_engine("postgresql+psycopg2://scott:tiger@localhost:5432/mydatabase")
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

Therefore, SQLAlchemy has it's own Type definitions [`sqlalchemy.sql.sqltypes`](https://docs.sqlalchemy.org/en/14/core/type_basics.html#generic-camelcase-types).
    
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
