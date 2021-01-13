# mojap-metadata

This python package allows users to read and alter our metadata schemas (using the metadata module) as well as convert our metadata schemas to other schema definitions utilised by other tools (these are defined in the converters module and are defined as Converters).


## Installation

> Make sure you are using a new version of pip (>=20.0.0)

```bash
pip install git+https://github.com/moj-analytical-services/mojap-metadata
```

To install additional dependencies that will be used by the converters (e.g. `etl-manager` and `arrow` extras)

```bash
pip install 'mojap-metadata[etl-manager,arrow] @ git+https://github.com/moj-analytical-services/mojap-metadata'
```

<hr>

# Metadata 

This module creates a class called `Metadata` which allows you to interact with our agnostic metadata schemas. The `Metadata` class deals with parsing, manipulating and validating metadata json schemas.

## The Schema

Our metadata schemas are used to define a table. The idea of these schemas are to define the contexts of a table with generic metadata schemas. If you want to use this schema to interact with Oracle, PyArrow or AWS Glue for example, then you can create a Converter class to take the metadata and converter it to a schema that works with that tool (or vice versa).

When adding a parameter to the metadata config first thing is to look if it exists in [json-schema](https://json-schema.org/understanding-json-schema/index.html). For example `enum`, `pattern` and `type` are parameters in our column types but come from json schema naming definitions.

An example of a basic metadata schema:

```json
{
    "$schema" : "$schema": "https://moj-analytical-services.github.io/metadata_schema/mojap_metadata/v1.0.0.json",
    "name": "employees",
    "description": "table containing employee information",
    "file_format": "parquet",
    "columns": [
        {
            "name": "employee_id",
            "type": "int64",
            "type_desc": "integer",
            "description": "an ID for each employee",
            "minimum": 1000,
            "maximum": 9999
        },
        {
            "name": "employee_name",
            "type": "string",
            "type_string": "string",
            "description": "name of the employee"
        },
        {
            "name": "employee_dob",
            "type": "date64",
            "type_desc": "date",
            "description": "date of birth for the employee in ISO format",
            "pattern": "^\\d{4}-([0]\\d|1[0-2])-([0-2]\\d|3[01])$"
        }
    ]
}
```

### Schema Properties

- **name:** String that can be whatever you want to name the table. Best to avoid spaces as most systems do not like that but it will let you do this.

- **file_format:** String denoting the file format.

- **columns:** List of objects where each object descibes a column in your table. Each column object must have at least a `name` and a (`type` or `type_description`).

    - **name:** String denoting the name of the column.
    - **type:** String specifing the type the data is in. We use data types from the [Apache Arrow project](https://arrow.apache.org/docs/python/api/datatypes.html). We use their type names as it seems to comprehensively cover most of the data types we deal with.
    - **type_category:** These group different sets of `type` properties into a single superset. These are: `integer`, `float`, `string`, `timestamp`, `bool`, `list`, `struct`. For example we class `int8, int16, int32, int64, uint8, uint16, uint32, uint64` as `integer`. It allows users to give more generic types if your data is not coming from a system or output with strict types (i.e. data exported from Excel or an unknown origin). The Metadata class has default type values for each given `type_category`. See the `default_type_category_lookup` attribute of the `Metadata` class to see said defaults. This field is required if `type` is not set.
    - **description:** Description of the column.
    - **enum:** List of what values that column can take. _(Same as the standardised json schema keyword)._
    - **pattern**: Regex pattern that value has to to match (for string type_categories only). _(Same as the standardised json schema keyword)._
    - **minLength / maxLength**: The minimum and maximum length of the string (for string type_categories only). _(Same as the standardised json schema keyword)._
    - **minimum / maximum**: The minumum and maximum value a numerical type can take (for integer and float type_categories only).
- **partitions**: List of what columns in your dataset are partitions.

#### Additional Schema Parameters

We allow users to add addition parameters to the table schema object or any of the columns in the schema. If there are specific parameters / tags you want to add to your schema it should still pass validation (as long as the additional parameters are not the same name of ones already used in the schema).

## Usage

```python
from mojap_metadata import Metadata

# Generate basic Metadata Table from dict
meta1 = Metadata(name="test", columns=[{"name": "c1", "type": "int64"}, {"name": "c2", "type": "string"}])

print(meta1.name) # test
print(meta1.columns[0]) # {"name": "c1", "type": "int64"}
print(meta1.description) # ""

# Generate meta from dict
d = {
    "name": "test",
    "columns": [
        {"name": "c1", "type": "int64"},
        {"name": "c2", "type": "string"}
    ]
}
meta2 = Metadata.from_dict(d)

# Read / write to json
meta3 = Metadata.read_json("path/to/metadata_schema.json")
meta3.name = "new_table"
meta3.to_json("path/to/new_metadata_schema.json")
```

<hr>

# Converters

Converters takes a Metadata object and generates something else from it (or can convert something to a Metadata object). Most of the time your converter will convert our schema into another systems schema. 

# Usage

For example the `ArrowConverter` takes our schemas and converts them to a pyarrow schema:

```python
from mojap_metadata import Metadata
from mojap_metadata.converters.arrow_converter import ArrowConverter

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

ac = ArrowConverter()
arrow_schema = ac.generate_from_meta(meta)

print(arrow_schema) # Could use this schema to read in data as arrow dataframe and cast it to the correct types
```

Another example is the `GlueConverter` which takes our schemas and converts them to a dictionary that be passed to the glue_client to deploy a schema on AWS Glue.

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

gc = ArrowConverter()
boto_dict = gc.generate_from_meta(meta, )
boto_dict = gc.generate_from_meta(meta, database_name="test_db", table_location="s3://bucket/test_db/test/")

print(boto_dict) 

glue_client = boto3.client("glue")
glue_client.create_table(**boto_dict) # Would deploy glue schema based on our metadata
```

All converter classes are sub classes of the `mojap_metadata.converters.BaseConverter`. This `BaseConverter` has no actual functionality but is a boilerplate class that ensures standardised attributes  for all added `Converters` these are:

- **generate_from_meta:** (function) takes a Metadata object and returns whatever the converter is producing .

- **generate_to_meta:** (function) takes Any object (normally another schema for another system or package) and returns our Metadata object. (i.e. the reverse of generate_from_meta).

- **options:** (Data Class) that are the options for the converter. The base options have a `suppress_warnings` parameter but it doesn't mean call converters use this. To get a better understanding of setting options see the `GlueConverter` class or the `tests/test_glue_converter.py` to see how they are set.


## Further Usage

See the [mojap-aws-tools repo](https://github.com/moj-analytical-services/mojap-aws-tools-demo) which utilises the converters a lot in different tutorials.

## Contributing and Design Considerations

Each new converter (if not expanding on existing converters) should be added as a new submodule within the parent `converters` module. This is especially true if the new converter has additional package dependencies. By design the standard install of this package is fairly lightweight. However if you needed the `ArrowConverter` you would need to install the additional package dependencies for the arrow converter:

```bash
pip install 'mojap-metadata[arrow] @ git+https://github.com/moj-analytical-services/mojap-metadata'
```

This means we can continuely add converters (as submodules) and add optional package dependencies ([see pyproject.toml](./pyproject.toml) ) without making the default install any less lightweight. `mojap_metadata` would only error if someone tries to import a converter subclass that with having the additional dependencies dependencies installed).
