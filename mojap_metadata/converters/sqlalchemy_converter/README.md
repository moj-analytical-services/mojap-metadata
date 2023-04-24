# SQLAlchemy Converter

[Functions](#functions)
[Connection](#connection)
[Data Types](#data-types)
[Testing](#testing)

Uses the SQLAlchemy [`Inspector`](https://docs.sqlalchemy.org/en/20/core/reflection.html#fine-grained-reflection-with-inspector) class to:

1. Extract metadata from database dialects supported by [SQLAlchemy](https://docs.sqlalchemy.org/en/20/dialects/index.html#dialects)
2. Convert the extracted output into a mojap `Metadata` object

Currently installs SQLAlchemy v1.4 but compatible with SQLAlchemy [v2.0](https://docs.sqlalchemy.org/en/20/changelog/migration_20.html)

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

"oracledb" has replaced ["oracle_cx" drivers](https://oracle.github.io/python-oracledb/)

To create an oracle SQLAlchemy.engine:

    ```
    import sys
    import oracledb

    oracledb.version = "8.3.0"
    sys.modules["cx_Oracle"] = oracledb
    engine = create_engine("oracle://scott:tiger@127.0.0.1:1521/sidname")
    ```

## Data types

SQLAlchemy converts specific dialects into a common type variant. 

Therefore, SQLAlchemy has its own Type definitions [`sqlalchemy.sql.sqltypes`](https://docs.sqlalchemy.org/en/14/core/type_basics.html).

There are three categories:
    
1. “CamelCase” datatypes are to the greatest degree possible database agnostic.
2. “UPPERCASE” datatypes are always inherited from a particular “CamelCase” datatype, and always represent an exact datatype.
3. Backend-specific “UPPERCASE” datatypes are either fully specific to those databases, or add additional arguments that are specific to those databases.

### Type approximation
There is a class private method called `_get_dtype` which infers the corresponding mojap data type by comparing with the SQlAlchemy instance type. Order is important: the mojap data type will correspond to the instance type that first gets matched. 
This is approximate and the mapping might need to be modified as less familiar data types are encountered.

### Type conflation
In the oracle documentation it conflates int, smallint, numeric, number and decimal. There some inter-operability that makes it confusing. https://www.oracletutorial.com/oracle-basics/oracle-number-data-type/

1. NUMBER(precision=9, scale=0, asdecimal=False) - should return **integer**
2. NUMBER(precision=10, scale=2, asdecimal=True) - should return **decimal**

SQLAlchemy v1.4 does appear to handle this. Where the 'asdecimal' flag triggers a SQLAlchemy type switch.

### Default type
In the event the data-type received back from SQL-Alchemy is not found, the default return value from the private method `_approx_dtype` is 'string'.

## Testing

The `SQLAlchemyConverter` is tested against the following database dialects in [`test_sqlachemy_converter.py`](/tests/test_sqlalchemy_converter.py):

1. sqlite
2. duckdb
3. postgres
4. oracle

Whilst all return a `Metadata` objects with broadly the same features, there are differences. This is because whilst `Inspector` provides a consistent interface, a feature may not be supported by the database or by the sqlalchemy dialect. 
For example only the postgres dialect recognises the table comment. For more examples of differences have a look at the parameters passed in to `test_generate_to_meta()`.

The sqlite and duckdb databases are in memory and can be tested directly.

The postgres and oracle dialects are tested in the [test-sqlalchemy.yml] GitHub action by creating [service containers](https://docs.github.com/en/actions/using-containerized-services/about-service-containers) and specifying the relevant Docker image. It should be straightforward to add more service containers and test more dialects.

### Oracle Container

The oracle dialect is tested against the [Oracle Database Express Edition Container](https://github.com/gvenzl/oci-oracle-xe).

To run the test locally first create a docker oracle container:

``` bash
source tests/scripts/.sqlalchemy_envs.sh
sh tests/scripts/.oracle_image.sh
```

If you are using an M1 please follow these [instructions](https://github.com/gvenzl/oci-oracle-xe#oracle-xe-on-apple-m-chips).

### Oracle Container

The postgres dialect is tested against the [Postgres Container](https://hub.docker.com/_/postgres).

To run the test locally first create a docker postgres container:

``` bash
source tests/scripts/.sqlalchemy_envs.sh
sh tests/scripts/.postgres_image.sh
```
