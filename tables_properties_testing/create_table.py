from mojap_metadata.converters.glue_converter import GlueTable

database_name = "table_properties_mock"
table_metapath = "tables_properties_testing/mock_table.json"
table_location = "s3://table-properties-testing-ln/"

gt = GlueTable()

gt.generate_from_meta(
    metadata=table_metapath,
    database_name=database_name,
    table_location=table_location,
    table_properties=True
)

# meta = gt.generate_to_meta(database=database_name, table="mock_table")
# print(meta.to_dict())