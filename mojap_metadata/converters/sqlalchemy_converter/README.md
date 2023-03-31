
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
Oracle objects, such as tables and data typee are Uppercase, postgres are lower or camelcase, SQLServer is often a mix. Sometimes they can be case insensitive. Therefore,SQLAlchemy 

# Future enhancements

## optimisation
As previously mentioned, private method `_approx_dtype` iterates through the whole type list for every table column. It does short-circuit, but this search is clearly sub-optimal and will not scale. If the list increases in size and in large tables (salesforce i'm looking at you) it might negativly affect performance. At the moment, the latency will be unnoticable and is well within tolerance. However, speed is not the only issue. Accuracy is also affected by this approach. It is a substring match, so it is suceptible to confusion in the future if there are sql-alchemy changes to accomidate more dialects. A more robust approach might be to match using the object. Specific types can be instaciated using:
`class sqlalchemy.types.TypeEngine`. This needs more investigation.

Removed from the convertion methods (that exists in the postgres convertor) was a method to extract all non-system schema from an instance. This is still technically possible. If it becomes desirable, it might be wise to refactor the code first.
## refactoring
Either fold the Functions defined in `sqlalchemy_functions.py` into the file containing the class, or purge the sqlalchemy_converter class `__init__.py` of sqlalchemy dependencies. That aforementioned class would then inherit from a newly defined wrapper class for sqlalchemy that standardizes the Engine connection and alllows you to abstract the specific functinonality required for the metadata convertion.

Engines can be created a few different ways. A new class method could handle or at least greatly simplify the engine creation and subsiquent connections required for the other methods.




