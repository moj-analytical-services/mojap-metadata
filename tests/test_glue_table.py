import pytest
import json

from tests.helper import assert_meta_col_conversion, valid_types, get_meta
from moto import mock_glue
from mojap_metadata.converters import glue_converter
from mojap_metadata.converters.glue_converter import (
    GlueConverter,
    GlueTable,
    GlueConverterOptions,
    _default_type_converter,
)

# Basic Functionality Test: Verify that when table_properties is False, no additional properties are added to the table. This tests the default behavior remains unchanged.
def test_gluetable_generate_from_meta_basic(glue_client, monkeypatch):
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
    assert table["Table"]["Parameters"] == additional_table_properties_expected
    assert table["ResponseMetadata"]["HTTPStatusCode"] == 200


# Effect on AWS Glue Table Test: Ideally, you would test that an AWS Glue table created with table_properties set to True 
# and provided with valid additional properties correctly reflects these properties in AWS Glue. This might involve 
# integration testing with AWS Glue APIs to verify the table's properties post-creation.
def test_gluetable_generate_from_meta_table(glue_client, monkeypatch):
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
    # table_properties default is False
    gt.generate_from_meta(meta, table_properties=True)

    additional_table_properties_expected = {
        "classification": "csv",
        "extraction_timestamp_col": "value2",
        "primary_key": "value1",
        "checkpoint_col": "value3",
        "update_type": "value4",
    }

    table = glue_client.get_table(DatabaseName=meta.database_name, Name=meta.name)
    assert table["Table"]["Parameters"] == additional_table_properties_expected
    assert table["ResponseMetadata"]["HTTPStatusCode"] == 200


# Property Addition Test: Confirm that when table_properties is True and valid additional_table_properties are provided in the metadata, these properties are correctly added to the boto_dict. This verifies that the new functionality works as intended.

# Missing Properties Warning Test: Check that when table_properties is True but no additional_table_properties are found in the metadata, a warning is issued. This test ensures that your error handling and user notifications are functioning correctly.

# Metadata Formats Test: Since your generate_from_meta function accepts metadata as Metadata, a string path, or a dictionary, you should test that the table_properties functionality works correctly across these different input formats.

# Error Handling Test: Test how your code handles scenarios where the metadata is malformed or the AWS environment variables are not set correctly. While not directly related to table_properties, ensuring robust error handling is crucial.

# Partition and Repair Behavior Test: Since your code behaves differently based on the presence of partitions and whether run_msck_repair is set, it would be wise to test the interaction between table_properties and these parameters. Ensure that the additional properties don't interfere with the partitioning logic or the msck repair table command execution.