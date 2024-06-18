from mojap_metadata.converters.glue_converter import GlueTable, GlueConverter
from mojap_metadata import Metadata

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

print(meta.to_dict().get("primary_key"))


# d = {
#     "name": "test",
#     "database_name": "test_db",
#     "table_location": "s3://bucket/test_db/test/",
#     "columns": [
#         {"name": "c1", "type": "int64"},
#         {"name": "c2", "type": "string"},
#         {"name": "c3", "type": "struct<k1: string, k2:list<int64>>"}
#     ],
#     "file_format": "jsonl",
#     "glue_table_properties": {
#         "checkpoint_col": "c1", 
#         "update_type": "c3"
#     }
# }
# meta = Metadata.from_dict(d)

# gt = GlueTable()
# # Deletes the table if it already exists before recreating it.
# gt.generate_from_meta(meta, )
# gt.generate_from_meta(meta, database_name="test_db", table_location="s3://bucket/test_db/test/")

# from mojap_metadata import Metadata
# from mojap_metadata.converters.glue_converter import GlueConverter

# d = {
#     "name": "test",
#     "database_name": "test_db",
#     "table_location": "s3://bucket/test_db/test/"
#     "columns": 
#     [
#         {"name": "c1", "type": "int64"},
#         {"name": "c2", "type": "string"},
#         {"name": "c3", "type": "struct<k1: string, k2:list<int64>>"}
#     ],
#     "file_format": "jsonl"
# }

# meta = Metadata.from_dict(d)

# d = {
#     "name": "test",
#     "database_name": "test_db",
#     "table_location": "s3://bucket/test_db/test/"
#     "columns": [
#         {"name": "c1", "type": "int64"},
#         {"name": "c2", "type": "string"},
#         {"name": "c3", "type": "struct<k1: string, k2:list<int64>>"}
#     ],
#     "file_format": "jsonl",
#     "glue_table_properties": {
#         "checkpoint_col": "c1", 
#         "update_type": "c3"
#     }
# }
# meta = Metadata.from_dict(d)

# gt = GlueTable()
# # Deletes the table if it already exists before recreating it.
# gt.generate_from_meta(meta, )
# # gt.generate_from_meta(meta, database_name="test_db", table_location="s3://bucket/test_db/test/")


# # database_name = "table_properties_mock"
# # table_metapath = "tables_properties_testing/mock_table.json"
# # table_location = "s3://table-properties-testing-ln/"

# # gt = GlueTable()
# # gc = GlueConverter()

# # gt.generate_from_meta(
# #     metadata=Metadata.from_json(table_metapath),
# # )

# # meta = gt.generate_to_meta(database="table_properties_mock", table="mock_table", glue_table_properties=["test"], get_primary_key=True)
# # print(meta.primary_key)
# # print(meta._data.get("glue_table_properties"))
# # meta.to_json("tables_properties_testing/schema_mock.json")

# # gt.generate_from_meta(
# #     metadata=table_metapath,
# #     database_name=database_name,
# #     table_location=table_location,
# #     table_properties=True
# # )


