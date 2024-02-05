import pytest

from tests.helper import get_meta
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
    # table_properties default is False
    gt.generate_from_meta(meta)

    additional_table_properties_expected = {"classification": "csv"}

    table = glue_client.get_table(DatabaseName=meta.database_name, Name=meta.name)
    assert table["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert table["Table"]["Parameters"] == additional_table_properties_expected


# Testing that the AWS Glue table created is as expected.
def test_gluetable_generate_from_meta(glue_client, monkeypatch):
    meta = get_meta(
        "csv",
        {
            "database_name": "cool_database",
            "table_location": "s3://buckets/are/cool",
            "additional_table_properties": {
                "primary_key": "value1",
                "extraction_timestamp_col": "value2",
                "checkpoint_col": "value3",
                "update_type": "value4",
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
    # setting table_properties to True
    gt.generate_from_meta(meta, table_properties=True)

    additional_table_properties_expected = {
        "classification": "csv",
        "extraction_timestamp_col": "value2",
        "primary_key": "value1",
        "checkpoint_col": "value3",
        "update_type": "value4",
    }

    table = glue_client.get_table(DatabaseName=meta.database_name, Name=meta.name)
    assert table["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert table["Table"]["Parameters"] == additional_table_properties_expected


# Testing that providing no additional table properties but
# setting table_properties parameter to True gives a warning
def test_table_properties_warnings_generate_from_meta(glue_client, monkeypatch):
    meta = get_meta(
        "parquet",
        {"database_name": "cool_database", "table_location": "s3://buckets/are/cool"},
    )
    monkeypatch.setattr(
        glue_converter, "_start_query_execution_and_wait", lambda *args, **kwargs: None
    )
    glue_client.create_database(DatabaseInput={"Name": meta.database_name})
    gt = GlueTable()
    with pytest.warns(UserWarning):
        gt.generate_from_meta(meta, run_msck_repair=True, table_properties=True)


# Testing that there is no impact on run_msck_repair and partitioning
def test_parititioning_generate_from_meta(glue_client, monkeypatch):
    meta = get_meta(
        "csv",
        {
            "database_name": "cool_database",
            "table_location": "s3://buckets/are/cool",
            "additional_table_properties": {
                "primary_key": "value1",
                "extraction_timestamp_col": "value2",
                "checkpoint_col": "value3",
                "update_type": "value4",
            },
        },
    )

    monkeypatch.setattr(
        glue_converter, "_start_query_execution_and_wait", lambda *args, **kwargs: None
    )
    glue_client.create_database(DatabaseInput={"Name": meta.database_name})

    gt = GlueTable()
    gt.generate_from_meta(meta, run_msck_repair=True, table_properties=True)

    paritions_expected = [
        {"Name": "my_timestamp", "Type": "timestamp", "Comment": "Partition column"}
    ]

    additional_table_properties_expected = {
        "classification": "csv",
        "extraction_timestamp_col": "value2",
        "primary_key": "value1",
        "checkpoint_col": "value3",
        "update_type": "value4",
    }

    table = glue_client.get_table(DatabaseName=meta.database_name, Name=meta.name)
    assert table["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert table["Table"]["PartitionKeys"] == paritions_expected
    assert table["Table"]["Parameters"] == additional_table_properties_expected
