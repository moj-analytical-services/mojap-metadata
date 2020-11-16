# mojap-metadata

Draft project on creating our own metadata package

This package currently has two submodules.

<hr>

# Metadata 

Look it is one word! This module defines the generic metadata schemas that everything else is based off of. We use this generalised metadata to define our data in a centralised standard. This is basically the exact same as etl_manager.meta (but with the glue database creation and DDL creation taken out of it that bit is now in the converters section)

The `Metadata` class deals with parsing, manipulating and validating metadata configs. 

When adding a parameter to the metadata config first thing is to look if it exists in [json-schema](https://json-schema.org/understanding-json-schema/index.html). For example `enum`, `pattern` and `type` are parameters in our column types but come from json schema naming definitions.

An example of the metadata:

```json
{
    "$schema" : "https://moj-analytical-services.github.io/metadata_schema/table/v2.0.0.json",
    "name": "employees",
    "description": "table containing employee information",
    "data_format": "parquet",
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
            "type_desc: "date",
            "description": "date of birth for the employee"
        }
    ]
}
```

## Table parameters

### name

name can be whatever

### data_format

Type of data should be a lot looser than before can be anything.

## Column Parameters

The `columns` parameter of the metadata is an array of objects where each object can have the following parameters.

### name

Name of the column


### type

The allowable datatypes should allow names from this list of datatypes from [Arrows agnostic datatypes](https://arrow.apache.org/docs/python/api/datatypes.html).

### type_desc

This should be an overaching superclass of the types values and a smaller group of types: `integer`, `double`, `string`, `date`, `datetime`, `boolean`, `array`, `object`. For example from the Arrow list we would class `int8, int16, int32, int64, uint8, uint16, uint32, uint64` as `integer`. This type_desc would also allow us to map older metadata from `etl-manager` to this `metadata`. Users should be allowed to infer the `type` from the `type_desc` (e.g. if user just says integer will might want to class the `type` as `int64` by default. 

### description

Description of the column

### enum

What values that column can take

### pattern

Regex pattern that value has to to match (strings only)

### minLength / maxLength

The minimum and maximum length of the string (strings only)

### format

[A] Format of the data (string only). Needs thought can basically use as a wild card but worth considering how we use for dates/datetimes/times as most likely what this would be used for. For example if you wanted to specify an ISO date format would you use `%Y-%m-%d` or `YYYY-MM-DD`.

### partitions

[A] Think we should keep this as a lot of our data is partitioned and it would be useful for our metadata to describe said partitions.

### minimum / maximum

The minumum and maximum value a numerical type can take (integer / double only)

## Other Column params for consideration 

- **primary-key / foriegn-key relationships:** Is this useful (doesn't work perfectly if the metadata schemas are at table level only). You would need an ID property of the table itself to reference which other table column it matches to.

- **ID parameter:** Unique ID of the table might just be useful to have this maybe in some format of a unique name like `repo/metadata/<folder>/<table_name>`. Which is essentially the file path to the table.

<hr>

# Converters


Converters takes a Metadata object and generates something else from it (or can convert something to a Metadata object). What is converted or generated can be wide ranging. An example of these can be:

- Glue / Oracle / Postgres schemas
- A pandas, Spark, arrow schema

With the above we should be careful to decide what is in scope and not in scope. I think we should try to keep the converters as lightweight as possible.

The idea of this submodule is to add more and more converters. Converters have two base functions (see the `mojap_metadata.converters.base_converter`). One takes a thing and converts outputs the Metadata object with definitions based on said "thing". The other takes the a Metadata object and produces a "thing". I don't know if we want to be specific by the word thing as atm it is quite broad in the converters I've created as examples (worth noting how each converter is a Child of the base_converter that does nothing):

- **pandas_converter:** Either infers what the metadata should be from a given dataframe or casts the columns in a dataframe to match the specified metadata. (Question it feels like this should produce a "pandas schema" for another package to do the conversion. This is a grey area where scoping could go either way - up for discussion!)
<br>
- **glue_converter:**: This is the other part of the etl_manager TableMeta class that I have gutted out of Metadata class here. It either takes the Glue DDL and converts it to our metadata or creates the glue DDL from our metadata. If we were to do this I'd imagine internals of etl_manager would just use the converters from here and the metadata class to then actually push the table DDLs to glue (as that _should_ be out of scope for the converters)

## Glue converter (as an example of package scope)

Takes the metadata and then generates a glue schema from the metadata. The metadata object is generalised so **restrictions on what is allowable is imposed by the Glue Converter class not the Metadata class**. This is very different to how etl-manager worked. 

The Glue Converter class can by default infer a lot about the transformation (like etl-manager does). However, it should allow far more flexibility in user definitions of how the Metadata maps to Glue. For example if the `Metadata.data_type` is `csv` the GlueConverter should default to a standard hive parser for CSV but you should also be able to alter the default and/or specify a custom hive parser for each table. Another example of this would be table types / table names. Users should be able to customise their Glue Converter. Converters should also be able to read/write configs as well as define their parameterisation as a config ([see base converter methods](base_converter.py)).

Another note on scoping is that the GlueConverter should only generate the glue schemas not interact with our infrastructure. You would expect that the etl_manager package (or its replacement) would have the ability to be given a glue table schema and then push that to the aws glue service ([this is what the boto3 glue client does](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.create_table) and what etl-manager calls under the hood). You would expect etl-manager to be able to accept our metadata schemas via the GlueConverter class. It should also accept the table schema or the output from the GlueConverter which would also be a compliant table schema for Glue. Then the additional functionality of `delete database, refresh partitions` etc would be part of etl-manager (as you wouldn't expect the GlueConverter to do that). I think keeping the scope to converters to only converting to/from our metadata and other schemas/configs will allow us to keep this package appropriately scoped (no doubt this will not be as clear cut in particular circumstances). 

<hr>

# Package Management

Converters are seperated out so that you can only install what you need. Let's say we had an Oracle converter that we integrated to this package that required the oracle_cx package. You can imagine that this might start to get unweildy the more converters we add in for postgres, mysql, pandas, etc. However, all we would do in this case is the following:

1. Make sure that additional packages are only referenced in the converter that uses them. Note in that Pandas is only imported in the `pandas_converter` and boto3 (other was stuff) is only used in the `glue_converter` module. When you install this package it won't come with pandas or boto3. It will only come with the bare packages needed to for the metadata submodule. If you want to install the extras then you specify it by using the `extras` option in your package install...

2. If you look at the `pyproject.toml` you'll see there is the option to install additional packages for your converter needs. If you needed the pandas_converter functionality you can then `pip install mojap-metadata[pandas]`. For glue `pip install mojap-metadata[glue]` or both pandas and glue `pip install mojap-metadata[glue,pandas]`.

>FYI not stating we should use poetry - I just know how to create extras defintions in there.
