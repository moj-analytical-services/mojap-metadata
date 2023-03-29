# import pytest
# import json

# import pydbtools as pydb

# from tests.helper import assert_meta_col_conversion, valid_types, get_meta
# from moto import mock_glue
# from mojap_metadata.converters.glue_converter import (
#     GlueConverter,
#     GlueTable,
#     GlueConverterOptions,
#     _default_type_converter,
# )


# @pytest.mark.parametrize(argnames="meta_type", argvalues=valid_types)
# def test_converter_accepts_type(meta_type):
#     """
#     If new type is added to tests.valid_types then it may fail this test

#     Args:
#         meta_type ([type]): str
#     """
#     emc = GlueConverter()
#     emc.options.ignore_warnings = True
#     unsupported_types = [k for k, v in _default_type_converter.items() if v[0] is None]
#     unsupported_types = tuple(unsupported_types)
#     if not meta_type.startswith(unsupported_types):
#         _ = emc.convert_col_type(meta_type)


# @pytest.mark.parametrize(
#     argnames="meta_type,glue_type,expect_raises",
#     argvalues=[
#         ("bool", "boolean", None),
#         ("bool_", "boolean", None),
#         ("int8", "tinyint", None),
#         ("int16", "smallint", None),
#         ("int32", "int", None),
#         ("int64", "bigint", None),
#         ("uint8", "smallint", "warning"),
#         ("uint16", "int", "warning"),
#         ("uint32", "bigint", "warning"),
#         ("uint64", None, "error"),
#         ("float16", "float", "warning"),
#         ("float32", "float", None),
#         ("float64", "double", None),
#         ("decimal128(0,38)", "decimal(0,38)", None),
#         ("decimal128(1,2)", "decimal(1,2)", None),
#         ("time32(s)", None, "error"),
#         ("time32(ms)", None, "error"),
#         ("time64(us)", None, "error"),
#         ("time64(ns)", None, "error"),
#         ("timestamp(s)", "timestamp", None),
#         ("timestamp(ms)", "timestamp", None),
#         ("timestamp(us)", "timestamp", None),
#         ("timestamp(ns)", "timestamp", None),
#         ("date32", "date", None),
#         ("date64", "date", None),
#         ("string", "string", None),
#         ("large_string", "string", None),
#         ("utf8", "string", None),
#         ("large_utf8", "string", None),
#         ("binary", "binary", None),
#         ("binary(128)", "binary", None),
#         ("large_binary", "binary", None),
#         ("struct<num:int64>", "struct<num:bigint>", None),
#         ("list_<int64>", "array<bigint>", None),
#         ("list<int64>", "array<bigint>", None),
#         ("list_<list_<int64>>", "array<array<bigint>>", None),
#         ("list_<list<int64>>", "array<array<bigint>>", None),
#         ("large_list<int64>", "array<bigint>", None),
#         ("large_list<large_list<int64>>", "array<array<bigint>>", None),
#         ("struct<num:int64,newnum:int64>", "struct<num:bigint,newnum:bigint>", None),
#         ("struct<num:int64, newnum:int64>", "struct<num:bigint,newnum:bigint>", None),
#         (
#             "struct<num:int64, arr:list_<int64>>",
#             "struct<num:bigint,arr:array<bigint>>",
#             None,
#         ),
#         (
#             "list_<struct<num:int64,desc:string>>",
#             "array<struct<num:bigint,desc:string>>",
#             None,
#         ),
#         ("struct<num:int64,desc:string>", "struct<num:bigint,desc:string>", None),
#         ("list_<decimal128(38,0)>", "array<decimal(38,0)>", None),
#         (
#             "struct<a:timestamp(s),b:struct<f1: int32, f2: string, f3:decimal128(3,5)>>",  # noqa
#             "struct<a:timestamp,b:struct<f1:int,f2:string,f3:decimal(3,5)>>",
#             None,
#         ),
#         (
#             "struct<k1:list<string>, k2:string, k3:string, k4:string, k5:list<string>, k6:string>",  # noqa
#             "struct<k1:array<string>,k2:string,k3:string,k4:string,k5:array<string>,k6:string>",  # noqa
#             None,
#         ),
#     ],
# )
# def test_meta_to_glue_type(meta_type, glue_type, expect_raises):
#     assert_meta_col_conversion(GlueConverter, meta_type, glue_type, expect_raises)


# @pytest.mark.parametrize(
#     argnames="spec_name,serde_name,expected_file_name",
#     argvalues=[
#         ("csv", "lazy", "test_simple_lazy_csv"),
#         ("csv", "open", "test_simple_open_csv"),
#         ("json", "hive", "test_simple_hive_json"),
#         ("json", "openx", "test_simple_openx_json"),
#         ("parquet", None, "test_simple_parquet"),
#     ],
# )
# def test_generate_from_meta(spec_name, serde_name, expected_file_name):
#     md = get_meta(spec_name)

#     gc = GlueConverter()
#     if spec_name == "csv":
#         gc.options.set_csv_serde(serde_name)

#     if spec_name == "json":
#         gc.options.set_json_serde(serde_name)

#     opts = GlueConverterOptions(
#         default_db_base_path="s3://bucket/", default_db_name="test_db"
#     )

#     gc_default_opts = GlueConverter(opts)

#     table_path = "s3://bucket/test_table"

#     # DO DICT TEST
#     spec = gc.generate_from_meta(md, database_name="test_db", table_location=table_path)
#     spec_default_opts = gc_default_opts.generate_from_meta(
#         md,
#     )
#     assert spec == spec_default_opts

#     with open(f"tests/data/glue_converter/{expected_file_name}.json") as f:
#         expected_spec = json.load(f)

#     assert spec == expected_spec


# @mock_glue
# @pytest.mark.parametrize(
#     "gc_kwargs,add_to_meta",
#     [
#         ({}, {"table_location": "s3://bucket/meta/", "database_name": "meta"}),
#         ({"table_location": "s3://bucket/kwarg/", "database_name": "kwarg"}, {}),
#         (
#             {"table_location": "s3://bucket/kwarg/", "database_name": "kwarg"},
#             {"table_location": "s3://bucket/meta/", "database_name": "meta"},
#         ),
#     ],
# )
# def test_meta_or_kwarg_location_and_name(gc_kwargs: dict, add_to_meta: dict):
#     """
#     This will test the two optional metadata properties "table_location" and
#     "database_name" and that the glue converter correctly converts to a glue schema in 3
#     states: either present, both present
#     """
#     gc = GlueConverter()
#     # get the metadata with any additional properties
#     md = get_meta("csv", add_to_meta)
#     # convert it
#     boto_dict = gc.generate_from_meta(md, **gc_kwargs)
#     # get the correct dictionary to assert
#     expected_in_boto_dict = gc_kwargs if gc_kwargs else add_to_meta
#     # assert
#     assert expected_in_boto_dict == {
#         "table_location": boto_dict["TableInput"]["StorageDescriptor"]["Location"],
#         "database_name": boto_dict["DatabaseName"],
#     }


# def test_gluetable_generate_from_meta(glue_client, monkeypatch):
#     meta = get_meta(
#         "csv",
#         {"database_name": "cool_database", "table_location": "s3://buckets/are/cool"},
#     )

#     monkeypatch.setattr(pydb, "start_query_execution_and_wait", lambda x: None)
#     glue_client.create_database(DatabaseInput={"Name": meta.database_name})

#     # ignore the warnings as I don't want to run msck repair table
#     gt = GlueTable()
#     gt.options.ignore_warnings = True
#     gt.generate_from_meta(meta)

#     table = glue_client.get_table(DatabaseName=meta.database_name, Name=meta.name)
#     assert table["ResponseMetadata"]["HTTPStatusCode"] == 200


# def test_gluetable_msck_warnings(glue_client, monkeypatch):
#     meta = get_meta(
#         "csv",
#         {"database_name": "cool_database", "table_location": "s3://buckets/are/cool"},
#     )
#     monkeypatch.setattr(pydb, "start_query_execution_and_wait", lambda x: None)
#     glue_client.create_database(DatabaseInput={"Name": meta.database_name})
#     gt = GlueTable()
#     with pytest.warns(Warning):
#         gt.generate_from_meta(meta)


# def test_gluetable_generate_to_meta(glue_client, monkeypatch):
#     meta = get_meta(
#         "csv",
#         {"database_name": "cool_database", "table_location": "s3://buckets/are/cool"},
#     )

#     monkeypatch.setattr(pydb, "start_query_execution_and_wait", lambda x: None)
#     # create the mock table and generate the meta from the mock table
#     with mock_glue():
#         glue_client.create_database(DatabaseInput={"Name": meta.database_name})
#         gt = GlueTable()
#         gt.generate_from_meta(meta)
#         meta_generated = gt.generate_to_meta("cool_database", "test_table")

#     # check that all the column types and partitions are correctly generated
#     gen_cols_match = meta_generated.columns == meta.columns
#     gen_partitions_match = meta_generated.partitions == meta.partitions

#     assert (True, True) == (gen_cols_match, gen_partitions_match)


# @pytest.mark.parametrize(
#     "glue_type,expected_mojap_type",
#     [
#         ("boolean", "bool"),
#         ("tinyint", "int8"),
#         ("smallint", "int16"),
#         ("int", "int32"),
#         ("integer", "int32"),
#         ("bigint", "int64"),
#         ("double", "float64"),
#         ("float", "float32"),
#         ("decimal(15, 2)", "decimal128(15, 2)"),
#         ("decimal(15)", "decimal128(15)"),
#         ("char(2)", "string"),
#         ("varchar(10)", "string"),
#         ("string", "string"),
#         ("binary", "binary"),
#         ("date", "date64"),
#         ("timestamp", "timestamp(s)"),
#         ("array<integer>", "large_list<int32>"),
#         ("struct<name:varchar(10), age:integer>", "struct<name:string,age:int32>"),
#     ]
# )
# def test_glue_to_mojap_exhaustive_conversion(glue_type: str, expected_mojap_type: str):
#     boto3_col = [{"Name": "cool_column", "Type": glue_type}]
#     gtc = GlueTable()
#     mojap_meta_cols = gtc.convert_columns(boto3_col)
#     assert mojap_meta_cols[0]["type"] == expected_mojap_type
