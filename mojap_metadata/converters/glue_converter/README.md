# Glue Converter

The `GlueConverter` takes our schemas and converts them to a dictionary that can be passed to an [AWS boto glue client](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html) to create a table in the [AWS Glue Data Catalogue](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html).

**generate_from_meta:** Generates the Hive DDL from the metadata.
- _metadata:_ A metadata object from the Metadata class
- _database\_name:_ (optional) The database to create the table in. Can also be a property of the metadata.
- _table\_location:_ (optional) The S3 location where the table is stored. Needed for table DDL. Can also be a property of the metadata.
    
If `primary_key` or `glue_table_properties` are included in the table schema, the `generate_from_meta` method will update the dictionary with these values. Note the following details:
- The key-value pairs in `glue_table_properties` must be strings e.g. `"key1":"3"`, `"key2":"True"`, `"key3":"column1"`, `"key4":"['column1','column2']"`.
- Properties that have specific uses in AWS (defined in `_glue_table_properties_aws`) will not be updated in the dictionary by this method. Raises a warning if these properties are defined in `glue_table_properties`.
- The primary key value will be taken from the `primary_key` parameter in the schema, not from `glue_table_properties`. 

```python
import boto3
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueConverter

d = {
    "name": "test",
    "database_name": "test_db",
    "table_location": "s3://bucket/test_db/test/",
    "columns": [
        {"name": "c1", "type": "int64"},
        {"name": "c2", "type": "string"},
        {"name": "c3", "type": "struct<k1: string, k2:list<int64>>"}
    ],
    "file_format": "jsonl",
    "glue_table_properties": {
        "checkpoint_col": "c1", 
        "update_type": "c3"
    }
}
meta = Metadata.from_dict(d)

gc = GlueConverter()
boto_dict = gc.generate_from_meta(meta, )
boto_dict = gc.generate_from_meta(meta, database_name="test_db", table_location="s3://bucket/test_db/test/")

print(boto_dict) 

glue_client = boto3.client("glue")
# Would deploy glue schema based on our metadata. Creates a new table in Glue Data Catalog, will fail if the table already exists.
glue_client.create_table(**boto_dict) 
# Updates an existing table in Glue Data Catalog, fails if the table does not exist.
glue_client.update_table(**boto_dict) 
```

Included alongside `GlueConverter` is `GlueTable` which can generate a Glue Table directly from a schema, and also generate a Metadata object from a Glue Table. It has two methods:

**generate_from_meta:** Generates a Glue table from the provided metadata object, dictionary, or string path. If the table already exists, this method will delete it before recreating it.
- _metadata:_ The metadata object, dict, or string path that is to be overlaid.
- _database\_name:_ (optional) A kwarg, the name of the Glue database to store the table. This can also be a property of the metadata object, dict, or file.
- _table\_location:_ (optional) A kwarg, the location of the table data. This can also be a property of the metadata object, dict, or file.

If `primary_key` or `glue_table_properties` are included in the table schema, the `generate_from_meta` method will update the Glue table with these values. Note the following details:
- The key-value pairs in `glue_table_properties` must be strings e.g. `"key1":"3"`, `"key2":"True"`, `"key3":"column1"`, `"key4":"['column1','column2']"`.
- Properties that have specific uses in AWS (defined in `_glue_table_properties_aws`) will not be updated in the Glue table by this method. Raises a warning if these properties are defined in `glue_table_properties`.
- The primary key value will be taken from the `primary_key` parameter in the schema, not from `glue_table_properties`. 

```python
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueConverter

d = {
    "name": "test",
    "database_name": "test_db",
    "table_location": "s3://bucket/test_db/test/",
    "columns": [
        {"name": "c1", "type": "int64"},
        {"name": "c2", "type": "string"},
        {"name": "c3", "type": "struct<k1: string, k2:list<int64>>"}
    ],
    "file_format": "jsonl",
    "glue_table_properties": {
        "checkpoint_col": "c1", 
        "update_type": "c3"
    }
}
meta = Metadata.from_dict(d)

gt = GlueTable()
# Deletes the table if it already exists before recreating it.
gt.generate_from_meta(meta, )
gt.generate_from_meta(meta, database_name="test_db", table_location="s3://bucket/test_db/test/")
```

**generate_to_meta:** Generates a Metadata object for a specified table from Glue.
- _database:_ The name of the Glue database.
- _table:_ The name of the Glue table.
- _glue\_table\_properties:_ (optional) The table properties to get from the Glue Catalog. Default value is `None`. Set to `"*"` to get all Glue table properties.
- _update\_primary\_key:_ (optional) Default value is `False`. Set to `True` to update the `primary_key` value in the metadata with the `primary_key` table property from Glue Data Catalog if it exists. 

Note the following details:
- Possible `glue_table_properties` values are:
    - `None`: default value, nothing happens
    - `"*"`: gets all glue table properties in the Glue Data Catalog
    - `["property_a", "primary_key"]`: gets `property_a` and `primary_key` from glue table properties in the Glue Data Catalog and updates the metadata with these key-value pairs in `glue_table_properties`.
- Raises a warning if a property specified in `glue_table_properties` does not exist in the glue table properties in the Glue Data Catalog.
- Raises an error if `"primary_key"` value is not a list in glue table properties in Glue Data Catalog e.g. must follow the format `"primary_key":"['<column_name>']"`.

```python
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueConverter

d = {
    "name": "test",
    "database_name": "test_db",
    "table_location": "s3://bucket/test_db/test/",
    "primary_key": ["c2"],
    "columns": [
        {"name": "c1", "type": "int64"},
        {"name": "c2", "type": "string"},
        {"name": "c3", "type": "struct<k1: string, k2:list<int64>>"}
    ],
    "file_format": "jsonl",
    "glue_table_properties": {
        "checkpoint_col": "c1", 
        "update_type": "c3"
    }
}
meta = Metadata.from_dict(d)

gt = GlueTable()
# Deletes the table if it already exists before recreating it.
gt.generate_from_meta(meta, )
meta = gt.generate_to_meta(database="test_db", table="test", glue_table_properties="*", get_primary_key=True)

print(meta.to_dict())
```