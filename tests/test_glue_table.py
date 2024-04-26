import pytest

from tests.helper import get_meta
from moto import mock_glue
from mojap_metadata.converters import glue_converter
from mojap_metadata.converters.glue_converter import GlueTable


# Testing default behavior remains unchanged.
def test_basic_functionality_generate_from_meta(glue_client, monkeypatch):
    meta = get_meta(
        "csv",
        {"database_name": "cool_database", "table_location": "s3://buckets/are/cool"},
    )

    monkeypatch.setattr(
        glue_converter, "_start_query_execution_and_wait", lambda *args, **kwargs: None
    )
    glue_client.create_database(DatabaseInput={"Name": meta.database_name})

    # ignore the warnings as I don't want to run msck repair table
    gt = GlueTable()
    gt.options.ignore_warnings = True
    # has_glue_table_properties default is False
    gt.generate_from_meta(meta)

    glue_table_properties_expected = {"classification": "csv"}

    table = glue_client.get_table(DatabaseName=meta.database_name, Name=meta.name)
    assert table["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert table["Table"]["Parameters"] == glue_table_properties_expected


# Testing that the AWS Glue table created is as expected.
def test_gluetable_generate_from_meta(glue_client, monkeypatch):
    meta = get_meta(
        "csv",
        {
            "database_name": "cool_database",
            "table_location": "s3://buckets/are/cool",
            "primary_key": ["my_timestamp", "my_int"],
            "glue_table_properties": {
                "classification": "json",
                "primary_key": ["column1"],
                "extraction_timestamp_col": 10,
                "checkpoint_col": "value3",
                "update_type": True,
                "test_column": ["value1", "value2"],
            },
        },
    )

    monkeypatch.setattr(
        glue_converter, "_start_query_execution_and_wait", lambda *args, **kwargs: None
    )
    glue_client.create_database(DatabaseInput={"Name": meta.database_name})

    # ignore the warnings as I don't want to run msck repair table
    gt = GlueTable()
    gt.options.ignore_warnings = True
    # setting has_glue_table_properties to True
    gt.generate_from_meta(meta)

    glue_table_properties_expected = {
        "classification": "csv",
        "primary_key": "['my_timestamp', 'my_int']",
        "extraction_timestamp_col": "10",
        "checkpoint_col": "value3",
        "update_type": "True",
        "test_column": "['value1', 'value2']",
    }

    table = glue_client.get_table(DatabaseName=meta.database_name, Name=meta.name)
    assert table["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert table["Table"]["Parameters"] == glue_table_properties_expected


# Testing that a TypeError is given if glue_table_properties are not type dict
def test_table_properties_error_generate_from_meta(glue_client, monkeypatch):
    meta = get_meta(
        "csv",
        {
            "database_name": "cool_database",
            "table_location": "s3://buckets/are/cool",
            "glue_table_properties": [
                {
                    "classification": "json",
                    "primary_key": ["column1"],
                    "extraction_timestamp_col": 10,
                    "checkpoint_col": "value3",
                    "update_type": True,
                    "test_column": ["value1", "value2"],
                }
            ],
        },
    )
    monkeypatch.setattr(
        glue_converter, "_start_query_execution_and_wait", lambda *args, **kwargs: None
    )
    glue_client.create_database(DatabaseInput={"Name": meta.database_name})
    gt = GlueTable()
    with pytest.raises(TypeError):
        gt.generate_from_meta(meta, run_msck_repair=True, update_table_properties=True)


# Testing that there is no impact on run_msck_repair and partitioning
def test_parititioning_generate_from_meta(glue_client, monkeypatch):
    meta = get_meta(
        "csv",
        {
            "database_name": "cool_database",
            "table_location": "s3://buckets/are/cool",
            "glue_table_properties": {
                "extraction_timestamp_col": 10,
                "checkpoint_col": "value3",
                "update_type": True,
                "test_column": ["value1", "value2"],
            },
        },
    )

    monkeypatch.setattr(
        glue_converter, "_start_query_execution_and_wait", lambda *args, **kwargs: None
    )
    glue_client.create_database(DatabaseInput={"Name": meta.database_name})

    gt = GlueTable()
    gt.generate_from_meta(meta, run_msck_repair=True)

    paritions_expected = [
        {"Name": "my_timestamp", "Type": "timestamp", "Comment": "Partition column"}
    ]

    glue_table_properties_expected = {
        "classification": "csv",
        "extraction_timestamp_col": "10",
        "checkpoint_col": "value3",
        "update_type": "True",
        "test_column": "['value1', 'value2']",
    }

    table = glue_client.get_table(DatabaseName=meta.database_name, Name=meta.name)
    assert table["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert table["Table"]["PartitionKeys"] == paritions_expected
    assert table["Table"]["Parameters"] == glue_table_properties_expected


# Testing default behavior remains unchanged.
def test__basic_functionality_generate_to_meta(glue_client, monkeypatch):
    meta = get_meta(
        "csv",
        {"database_name": "cool_database", "table_location": "s3://buckets/are/cool"},
    )

    monkeypatch.setattr(
        glue_converter, "_start_query_execution_and_wait", lambda *args, **kwargs: None
    )
    # create the mock table and generate the meta from the mock table
    with mock_glue():
        glue_client.create_database(DatabaseInput={"Name": meta.database_name})
        gt = GlueTable()
        gt.generate_from_meta(meta)
        meta_generated = gt.generate_to_meta("cool_database", "test_table")

    # check that all the column types and partitions are correctly generated
    gen_cols_match = meta_generated.columns == meta.columns
    gen_partitions_match = meta_generated.partitions == meta.partitions

    assert (True, True) == (gen_cols_match, gen_partitions_match)


# Testing behavior when table parameters are populated in Glue
def test__glue_table_properties_generate_to_meta(glue_client, monkeypatch):
    meta = get_meta(
        "csv",
        {
            "database_name": "cool_database",
            "table_location": "s3://buckets/are/cool",
            "primary_key": ["my_timestamp", "my_int"],
            "glue_table_properties": {
                "classification": "json",
                "primary_key": ["column1"],
                "extraction_timestamp_col": 10,
                "checkpoint_col": "value3",
                "update_type": True,
                "test_column": ["value1", "value2"],
            },
        },
    )

    monkeypatch.setattr(
        glue_converter, "_start_query_execution_and_wait", lambda *args, **kwargs: None
    )
    # create the mock table and generate the meta from the mock table
    with mock_glue():
        glue_client.create_database(DatabaseInput={"Name": meta.database_name})
        gt = GlueTable()
        gt.generate_from_meta(meta)
        meta_generated = gt.generate_to_meta("cool_database", "test_table")
        meta_dict = meta_generated.to_dict()

    # check that all the column types and partitions are correctly generated
    gen_cols_match = meta_generated.columns == meta.columns
    gen_partitions_match = meta_generated.partitions == meta.partitions

    glue_table_properties_expected = {
        "extraction_timestamp_col": 10,
        "checkpoint_col": "value3",
        "update_type": True,
        "test_column": ["value1", "value2"],
    }

    primary_key_expected = ["my_timestamp", "my_int"]

    assert (True, True) == (gen_cols_match, gen_partitions_match)
    assert (meta_dict.get("glue_table_properties")) == glue_table_properties_expected
    assert (meta_dict.get("primary_key")) == primary_key_expected
