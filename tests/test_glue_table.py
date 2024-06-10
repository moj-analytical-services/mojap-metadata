import jsonschema
import pytest

from tests.helper import get_meta
from moto import mock_glue
from mojap_metadata.converters import glue_converter
from mojap_metadata.converters.glue_converter import GlueTable


def setup_glue_table(glue_client, monkeypatch, meta):
    monkeypatch.setattr(
        glue_converter, "_start_query_execution_and_wait", lambda *args, **kwargs: None
    )
    glue_client.create_database(DatabaseInput={"Name": meta.database_name})

    gt = GlueTable()
    gt.options.ignore_warnings = True
    gt.generate_from_meta(meta)

    return glue_client.get_table(DatabaseName=meta.database_name, Name=meta.name)


def setup_glue_table_and_generate_meta(
    glue_client, monkeypatch, meta, properties_to_get=None, update_primary_key=False
):
    monkeypatch.setattr(
        glue_converter, "_start_query_execution_and_wait", lambda *args, **kwargs: None
    )

    with mock_glue():
        glue_client.create_database(DatabaseInput={"Name": meta.database_name})
        gt = GlueTable()
        gt.generate_from_meta(meta)
        return gt.generate_to_meta(
            "cool_database",
            "test_table",
            glue_table_properties=properties_to_get,
            update_primary_key=update_primary_key,
        )


# Testing generate_from_meta behaviour
@pytest.mark.parametrize(
    "meta, expected_properties",
    [
        (
            get_meta(
                "csv",
                {
                    "database_name": "cool_database",
                    "table_location": "s3://buckets/are/cool",
                },
            ),
            {"classification": "csv"},
        ),
        (
            get_meta(
                "csv",
                {
                    "database_name": "cool_database",
                    "table_location": "s3://buckets/are/cool",
                    "primary_key": ["my_timestamp", "my_int"],
                    "glue_table_properties": {
                        "extraction_timestamp_col": "10",
                        "checkpoint_col": "value3, value4",
                        "update_type": "True",
                        "test_column": "['value1', 'value2']",
                    },
                },
            ),
            {
                "classification": "csv",
                "primary_key": "['my_timestamp', 'my_int']",
                "extraction_timestamp_col": "10",
                "checkpoint_col": "value3, value4",
                "update_type": "True",
                "test_column": "['value1', 'value2']",
            },
        ),
    ],
)
def test_glue_table_generate_from_meta(
    glue_client, monkeypatch, meta, expected_properties
):

    table = setup_glue_table(glue_client, monkeypatch, meta)
    assert table["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert table["Table"]["Parameters"] == expected_properties


# Testing a warning is raised
def test_glue_table_raise_warning_generate_from_meta(glue_client, monkeypatch):
    meta = get_meta(
        "parquet",
        {
            "database_name": "cool_database",
            "table_location": "s3://buckets/are/cool",
            "primary_key": ["my_timestamp", "my_int"],
            "glue_table_properties": {
                "primary_key": "this_should_be_excluded",
                "classification": "this should be excluded",
                "projection.*": "['this should be excluded']",
                "extraction_timestamp_col": "10",
                "checkpoint_col": "value3, value4",
                "update_type": "True",
                "test_column": "['value1', 'value2']",
            },
        },
    )

    expected_properties = {
        "classification": "parquet",
        "primary_key": "['my_timestamp', 'my_int']",
        "extraction_timestamp_col": "10",
        "checkpoint_col": "value3, value4",
        "update_type": "True",
        "test_column": "['value1', 'value2']",
    }

    with pytest.warns(UserWarning):
        table = setup_glue_table(glue_client, monkeypatch, meta)

        assert table["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert table["Table"]["Parameters"] == expected_properties


# Testing that a jsonschema ValidationError is returned
@pytest.mark.parametrize(
    "glue_table_properties",
    [
        (
            [
                {
                    "extraction_timestamp_col": "10",
                    "checkpoint_col": "value3, value4",
                    "update_type": "True",
                    "test_column": "['value1', 'value2']",
                }
            ]
        ),
        (
            {
                "extraction_timestamp_col": 10,
                "checkpoint_col": "value3, value4",
                "update_type": True,
                "test_column": ["value1", "value2"],
            }
        ),
    ],
)
def test_glue_table_json_validation_error_generate_from_meta(glue_table_properties):
    with pytest.raises(jsonschema.exceptions.ValidationError):
        meta = get_meta(
            "csv",
            {
                "database_name": "cool_database",
                "table_location": "s3://buckets/are/cool",
                "glue_table_properties": glue_table_properties,
            },
        )

        return meta


# Testing generate_from_meta with run_msck_repair and partitioning properties
def test_glue_table_parititioning_generate_from_meta(glue_client, monkeypatch):
    meta = get_meta(
        "csv",
        {
            "database_name": "cool_database",
            "table_location": "s3://buckets/are/cool",
            "glue_table_properties": {
                "extraction_timestamp_col": "10",
                "checkpoint_col": "value3, value4",
                "update_type": "True",
                "test_column": "['value1', 'value2']",
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
        "checkpoint_col": "value3, value4",
        "update_type": "True",
        "test_column": "['value1', 'value2']",
    }

    table = glue_client.get_table(DatabaseName=meta.database_name, Name=meta.name)
    assert table["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert table["Table"]["PartitionKeys"] == paritions_expected
    assert table["Table"]["Parameters"] == glue_table_properties_expected


# Testing generate_to_meta behaviour
@pytest.mark.parametrize(
    "meta, properties_to_get, update_primary_key, "
    "expected_properties, expected_primary_key",
    [
        (
            get_meta(
                "csv",
                {
                    "database_name": "cool_database",
                    "table_location": "s3://buckets/are/cool",
                },
            ),
            [],
            False,
            None,
            [],
        ),
        (
            get_meta(
                "csv",
                {
                    "database_name": "cool_database",
                    "table_location": "s3://buckets/are/cool",
                    "primary_key": ["my_timestamp", "my_int"],
                    "glue_table_properties": {
                        "extraction_timestamp_col": "10",
                        "checkpoint_col": "value3, value4",
                        "update_type": "True",
                        "test_column": "['value1', 'value2']",
                    },
                },
            ),
            ["extraction_timestamp_col", "checkpoint_col"],
            False,
            {
                "extraction_timestamp_col": "10",
                "checkpoint_col": "value3, value4",
            },
            [],
        ),
        (
            get_meta(
                "csv",
                {
                    "database_name": "cool_database",
                    "table_location": "s3://buckets/are/cool",
                    "primary_key": ["my_timestamp", "my_int"],
                    "glue_table_properties": {
                        "extraction_timestamp_col": "10",
                        "checkpoint_col": "value3, value4",
                        "update_type": "True",
                        "test_column": "['value1', 'value2']",
                    },
                },
            ),
            "*",
            True,
            {
                "classification": "csv",
                "extraction_timestamp_col": "10",
                "checkpoint_col": "value3, value4",
                "update_type": "True",
                "test_column": "['value1', 'value2']",
                "primary_key": "['my_timestamp', 'my_int']",
            },
            ["my_timestamp", "my_int"],
        ),
        (
            get_meta(
                "csv",
                {
                    "database_name": "cool_database",
                    "table_location": "s3://buckets/are/cool",
                    "primary_key": ["my_timestamp", "my_int"],
                    "glue_table_properties": {
                        "extraction_timestamp_col": "10",
                        "checkpoint_col": "value3, value4",
                        "update_type": "True",
                        "test_column": "['value1', 'value2']",
                    },
                },
            ),
            ["primary_key"],
            "True",
            {"primary_key": "['my_timestamp', 'my_int']"},
            ["my_timestamp", "my_int"],
        ),
        (
            get_meta(
                "csv",
                {
                    "database_name": "cool_database",
                    "table_location": "s3://buckets/are/cool",
                    "primary_key": ["my_timestamp", "my_int"],
                    "glue_table_properties": {
                        "extraction_timestamp_col": "10",
                        "checkpoint_col": "value3, value4",
                        "update_type": "True",
                        "test_column": "['value1', 'value2']",
                    },
                },
            ),
            ["extraction_timestamp_col", "checkpoint_col", "primary_key"],
            False,
            {
                "extraction_timestamp_col": "10",
                "checkpoint_col": "value3, value4",
                "primary_key": "['my_timestamp', 'my_int']",
            },
            [],
        ),
    ],
)
def test_glue_table_generate_to_meta(
    glue_client,
    monkeypatch,
    meta,
    properties_to_get,
    update_primary_key,
    expected_properties,
    expected_primary_key,
):
    meta_generated = setup_glue_table_and_generate_meta(
        glue_client, monkeypatch, meta, properties_to_get, update_primary_key
    )
    meta_dict = meta_generated.to_dict()

    assert meta_generated.columns == meta.columns
    assert meta_generated.partitions == meta.partitions
    assert meta_dict.get("glue_table_properties") == expected_properties
    assert meta_dict.get("primary_key") == expected_primary_key


# Testing warning is generated when glue_table_property is not in Glue Catalog.
def test__glue_table_warning_generate_to_meta(glue_client, monkeypatch):
    meta = get_meta(
        "csv",
        {
            "database_name": "cool_database",
            "table_location": "s3://buckets/are/cool",
            "primary_key": ["my_timestamp", "my_int"],
            "glue_table_properties": {
                "classification": "should_be_excluded",
                "test_column": "['value1', 'value2']",
            },
        },
    )
    properties_to_get = ["key_which_does_not_exist", "primary_key", "classification"]
    expected_properties = {
        "classification": "csv",
        "primary_key": "['my_timestamp', 'my_int']",
    }
    expected_primary_key = ["my_timestamp", "my_int"]

    with pytest.warns(UserWarning):
        meta_generated = setup_glue_table_and_generate_meta(
            glue_client, monkeypatch, meta, properties_to_get, update_primary_key=True
        )
        meta_dict = meta_generated.to_dict()

        assert meta_generated.columns == meta.columns
        assert meta_generated.partitions == meta.partitions
        assert meta_dict.get("glue_table_properties") == expected_properties
        assert meta_dict.get("primary_key") == expected_primary_key
