# Glue Converter


The `GlueConverter` takes our schemas and converts them to a dictionary that be passed to the glue_client to deploy a schema on AWS Glue.

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

included alongside `GlueConverter` is `GlueTable` that can overlay a metadata object, dictionary, or path to metadata file. it has one method:
- **generate_from_meta:** generates a glue table from the metadata object, dict, or string path, takes the following arguments:
    - _metadata:_ the metadata object, dict, or string path that is to be overlaid
    - _table\_location:_ a kwarg, the location of the table data. This can also be a property of the metadata object, dict, or file
    - _database\_name:_ a kwarg, the name of the glue database to put the table. This can also be a property of the metadata object, dict, or file