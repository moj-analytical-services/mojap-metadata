from duckdb import table
from mojap_metadata import Metadata
from mojap_metadata.converters.glue_converter import GlueTable, GlueConverter
import boto3

database_name = "table_properties_mock"
table_metapath = "tables_properties_testing/mock_table.json"
table_location = "s3://table-properties-testing-ln/"

# meta = Metadata.from_json(table_metapath)
gt = GlueTable()
gt.generate_from_meta(
    metadata=table_metapath,
    database_name=database_name,
    table_location=table_location,
    table_properties=True,
    run_msck_repair=True
)

# client = boto3.client("glue")

# table = client.get_table(DatabaseName=database_name, Name="mock_table")
# print(table["Table"]["Parameters"])
