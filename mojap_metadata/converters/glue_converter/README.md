# Glue Converter

The `GlueConverter` takes our schemas and converts them to a dictionary that be passed to the glue_client to deploy a schema on AWS Glue.

**generate_from_meta:** generates the Hive DDL from the metadata:
    - _metadata:_ metadata object from the Metadata class
    - _table\_location:_ (optional) database name needed for table DDL
    - _database\_name:_ (optional) S3 location of where table is stored needed for table DDL
    - _primary\_key\_property:_ (optional) The name of the key to write the primary_key value to in the table_parameters in the table DDL. Defaults to "primary_key" 
    
    If primary_key or/and glue_table_properties are populated in the table schema/metadata, _generate\_from\_meta_ will update the metadata dictionary with these items. The key-value pairs in glue_table_properties must be of type string. Any properties that have special uses in AWS will not be updated - these parameters are defined in _glue_table_properties_aws. The primary key value will be taken from the primary_key parameter in the metadata, not from glue_table_properties. 

```python
import boto3
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueConverter

d = {
    "name": "test",
    "columns": [
        {"name": "c1", "type": "int64"},
        {"name": "c2", "type": "string"},
        {"name": "c3", "type": "struct<k1: string, k2:list<int64>>"}
    ],
    "file_format": "jsonl"
}
meta = Metadata.from_dict(d)

gc = GlueConverter()
boto_dict = gc.generate_from_meta(meta, )
boto_dict = gc.generate_from_meta(meta, database_name="test_db", table_location="s3://bucket/test_db/test/")

print(boto_dict) 

glue_client = boto3.client("glue")
glue_client.create_table(**boto_dict) # Would deploy glue schema based on our metadata
```

Included alongside `GlueConverter` is `GlueTable`. It has two methods:

- **generate_from_meta:** generates a glue table from the metadata object, dict, or string path, takes the following arguments:
    - _metadata:_ the metadata object, dict, or string path that is to be overlaid
    - _table\_location:_ a kwarg, the location of the table data. This can also be a property of the metadata object, dict, or file
    - _database\_name:_ a kwarg, the name of the glue database to put the table. This can also be a property of the metadata object, dict, or file
    - _primary\_key\_property:_ (optional) The name of the key to write the primary_key value to in the table_parameters in the table DDL. Defaults to "primary_key"
    
    If primary_key or/and glue_table_properties are populated in the table schema/metadata, _generate\_from\_meta_ will update the  Glue Catalog with these key-value pairs. The key-value pairs in glue_table_properties must be of type string. Any properties that have special uses in AWS will not be updated - these parameters are defined in _glue_table_properties_aws. The primary key value will be taken from the primary_key parameter in the metadata, not from glue_table_properties. 

- **generate_to_meta:** generates a Metadata object for a specified table from glue, takes the following arguments:
    - _database:_ the name of the glue database
    - _table:_ the name of the glue table from the glue database
    - _glue\_table\_properties:_ (optional) the table properties to retrieve from the Glue Catalog, default value is None. Set to "*" to retrieve all glue table properties.
    - _primary\_key\_property:_ (optional) The name of the key to write the primary_key value to in the metadata dictionary. Defaults to "primary_key".

If glue_table_properties are provided, _generate\_to\_meta_ will check they exist in the table parameters (if not, will raise a warning) and retrieve these and add glue_table_properties key to the metadata with the key-value pairs. If glue_table_properties is set to "*" then it will get all the available table parameters. An exception to this is the primary_key as it's value is updated in the metadata seperately instead of the glue_table_properties key. The primary_key value in the metadata is always updated if it exists in the table parameters.