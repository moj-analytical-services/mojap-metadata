# AWS Iceberg Converter

This converter allows you to make use of an `IcebergMetadata` object, which is a child class of the standard mojap `Metadata` class, and use it to create or update an Iceberg table in AWS using AWS Athena.

The converter can also use the details stored in the AWS Glue catalog to generate a `IcebergMetadata` object.

## `IcebergMetadata` Class

The `IcebergMetadata` class is a child class of the standard mojap `Metadata` class. In addition to the properties of the `Metadata` class, `IcebergMetadata` objects also have following properties:

- `table_type`: This will specify that the object represents and `iceberg` table format
- `noncurrent_columns`: These are columns that have once formed part of the Iceberg table but have either been removed or updated
- `partition_transforms`: These are a list of partition transforms to apply to the identified partition columns in the table, as found under `partitions`. This property is a list, which is required to be of the same length as `partitions`, with each value being one of `identity`, `year`, `day`, `hour`, `bucket[N]` and `truncate[W]` where `N` and `W` are integers. For more information on partition transforms see [here](https://iceberg.apache.org/spec/#partition-transforms).

## AWS Athena Iceberg SQL Converter

The `AthenaIcebergSqlConverter` class, a child class of the `GlueConverter`, allows you to take an `IcebergMetadata` object and use it to generate SQL queries necessary to either a) create an empty Iceberg metadata using AWS Athena or b) update an existing Iceberg table registered with the AWS Glue Catalog by either removing, updating or adding columns. For updating an Iceberg table you will need an `IcebergMetadata` object that represents the current state of the table.

The main methods of the converter are listed below:

- **generate_from_meta**: Takes an `IcebergMetadata` object and generates a list of SQL queries necessary to create a new or update an existing Iceberg table. If `create_not_alter` is set to `True` then the list will contain a single query for creating a table. If `create_not_alter` is set to `False` then an `IcebergMetadata` object or path must be given to `existing_metadata` and a list of queries will be given to add, remove or update the Iceberg table. Note that for updating existing column types (i.e. schema evolution) only `integer` or `int` to `bigint` and `float` to `double` are currently supported.
- **generate_create_from_meta**: Generates a SQL statement to create an empty Iceberg table in AWS based on a `IcebergMetadata` object.
- **generate_alter_from_meta**: Generates a list of SQL queries to alter an existing Iceberg table in AWS based on an `IcebergMetadata` object and another `IcebergMetadata` object which represents the current state of the Iceberg table in AWS.

## AWS Athena Iceberg Table Converter

The `AthenaIcebergTable` class, a child class of the `GlueTable` converter, allows you to either create a `IcebergMetadata` object from an existing Iceberg table in AWS or allows you to create a new or update an existing Iceberg table from an `IcebergMetadata` object. As per other converters, this converter has two main methods:

- **generate_from_meta**: Creates or updates and AWS Iceberg table based on an `IcebergMetadata` object.By default an error will be thrown if the Iceberg table represented already exists and neither `delete_table_if_exists` nor `alter_table_if_exists` is set to `True`. If the Iceberg table the metadata represents already exists in AWS and `delete_table_if_exists` is set to `True` then the existing table will be deleted and an empty Iceberg table created in its place. If the Iceberg table already exists and `alter_table_if_exists` then the Iceberg table's schema will be updated to reflect the `IcebergMetadata` object given.
- **generate_to_meta**: Creates an `IcebergMetadata` object from an existing Athena Iceberg table. Requires the database name for which the Iceberg table belongs, as well as the name of the Iceberg table in question.

## Further information

For more information on how Iceberg integrates with AWS please see the following links:

- [Athena documentation](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
- [awswrangler tutorial](https://aws-sdk-pandas.readthedocs.io/en/stable/tutorials/039%20-%20Athena%20Iceberg.html)
- [Athena Iceberg Workshop](https://catalog.us-east-1.prod.workshops.aws/workshops/9981f1a1-abdc-49b5-8387-cb01d238bb78/en-US/90-athena-acid)
- [Iceberg evaluation](https://github.com/moj-analytical-services/iceberg-evaluation)
- [Iceberg official site](https://iceberg.apache.org/)
