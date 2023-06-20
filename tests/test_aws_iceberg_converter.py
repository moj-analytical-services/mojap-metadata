import json
import os
import re
from copy import deepcopy

import awswrangler as wr
import boto3
import botocore
import pandas as pd
import pytest
from moto import mock_glue, mock_s3

import mojap_metadata.converters.aws_iceberg_converter as ic
from mojap_metadata.converters.aws_iceberg_converter import (
    AthenaIcebergSqlConverter,
    AwsIcebergTable,
)
from mojap_metadata.converters.aws_iceberg_converter.exceptions import (
    UnsupportedIcebergSchemaEvolution,
)
from mojap_metadata.converters.aws_iceberg_converter.iceberg_metadata import (
    IcebergMetadata,
)


def standardise_sql(x) -> str:
    return re.sub("\\s{1,}|\\n", " ", x).strip()


def mock_get_test_iceberg_metadata(iceberg_metadata_filepath: str, *args, **kwargs):
    if not iceberg_metadata_filepath.startswith("s3://"):
        raise ValueError(iceberg_metadata_filepath)

    with open("tests/data/aws_iceberg_converter/iceberg_metadata.json", "r") as f:
        iceberg_meta = json.load(f)

    return iceberg_meta


def mock_get_table(*args, **kwargs):
    with open("tests/data/aws_iceberg_converter/glue_response.json", "r") as f:
        resp = json.load(f)

    return resp


@pytest.fixture(scope="module")
def sql_converter():
    yield AthenaIcebergSqlConverter()


@pytest.fixture(scope="module")
def iceberg_metadata_dictionary():
    with open("tests/data/aws_iceberg_converter/metadata.json", "r") as f:
        meta_dict = json.load(f)
    yield meta_dict


def test_iceberg_metadata(iceberg_metadata_dictionary):
    meta = IcebergMetadata.from_dict(iceberg_metadata_dictionary)
    assert meta.partition_transforms == ["identity"]
    assert meta.table_type == "iceberg"
    assert meta.noncurrent_columns == []


@pytest.mark.parametrize(
    [
        "name",
        "transform",
        "expected",
    ],
    [
        (
            "column1",
            "identity",
            "column1",
        ),
        ("column2", "year", "year(column2)"),
        (
            "column3",
            "day",
            "day(column3)",
        ),
        (
            "column4",
            "hour",
            "hour(column4)",
        ),
        (
            "column5",
            "bucket[5]",
            "bucket(5, column5)",
        ),
        (
            "column6",
            "truncate[6]",
            "truncate(column6, 6)",
        ),
    ],
)
def test_combine_partition_name_and_transform(name, transform, expected):
    assert (
        AthenaIcebergSqlConverter._combine_partition_name_and_transform(
            name=name,
            transform=transform,
        )
        == expected
    )


def test_sql_convert_columns(sql_converter, iceberg_metadata_dictionary):
    meta = IcebergMetadata.from_dict(iceberg_metadata_dictionary)

    expected_columns = [
        ("my_int", "int", "This is an integer"),
        ("my_double", "float", ""),
        ("my_date", "date", ""),
        ("my_decimal", "decimal(10,2)", ""),
        ("my_timestamp", "timestamp", "Partition column"),
    ]
    expected_partitions = ["my_timestamp"]

    columns, partitions = sql_converter.convert_columns(meta)

    sorted_columns = sorted(columns, key=lambda x: x[0], reverse=False)
    sorted_expected_columns = sorted(
        expected_columns, key=lambda x: x[0], reverse=False
    )

    assert sorted_columns == sorted_expected_columns and sorted(partitions) == sorted(
        expected_partitions
    )


def test_find_removed_columns(iceberg_metadata_dictionary):
    existing_meta = IcebergMetadata.from_dict(iceberg_metadata_dictionary)

    meta = deepcopy(existing_meta)
    _ = meta.remove_column("my_decimal")

    assert AthenaIcebergSqlConverter._find_removed_columns(meta, existing_meta) == [
        "my_decimal"
    ]


@pytest.mark.parametrize("error", [True, False])
def test_find_new_or_updated_columns(sql_converter, iceberg_metadata_dictionary, error):
    existing_meta = IcebergMetadata.from_dict(iceberg_metadata_dictionary)

    meta = deepcopy(existing_meta)
    meta.update_column({"name": "my_new_column", "type": "string"})
    meta.update_column({"name": "my_double", "type": "float64"})
    meta.update_column({"name": "my_int", "type": "int64"})

    if error:
        meta.update_column({"name": "my_date", "type": "string"})

        with pytest.raises(UnsupportedIcebergSchemaEvolution):
            _ = sql_converter._find_new_or_updated_columns(
                meta,
                existing_meta,
            )

    else:
        add_columns, changed_columns = sql_converter._find_new_or_updated_columns(
            meta,
            existing_meta,
        )

        sorted_add_columns = sorted(add_columns, key=lambda x: x[0], reverse=False)
        sorted_changed_columns = sorted(
            changed_columns, key=lambda x: x[0], reverse=False
        )

        assert sorted_add_columns == [
            ("my_new_column", "string")
        ] and sorted_changed_columns == [
            (
                "my_double",
                "double",
            ),
            (
                "my_int",
                "bigint",
            ),
        ]


def test_generate_alter_change_queries():
    changed_columns = [
        ("my_column", "bigint"),
        ("my_other_column", "double"),
    ]

    sql_list = AthenaIcebergSqlConverter._generate_alter_change_queries(
        changed_columns,
        "my_table",
        "my_database",
    )

    expected_sql_list = [
        """
        ALTER TABLE my_database.my_table
        CHANGE my_column my_column bigint
        """,
        """
        ALTER TABLE my_database.my_table
        CHANGE my_other_column my_other_column double
        """,
    ]

    assert [standardise_sql(s) for s in sql_list] == [
        standardise_sql(s) for s in expected_sql_list
    ]


def test_generate_alter_add_query():
    add_columns = [
        ("my_column", "bigint"),
        ("my_other_column", "double"),
    ]

    sql = AthenaIcebergSqlConverter._generate_alter_add_query(
        add_columns,
        "my_table",
        "my_database",
    )

    expected_sql = """
    ALTER TABLE my_database.my_table ADD COLUMNS (
        my_column bigint,
        my_other_column double
    )
    """

    assert standardise_sql(sql) == standardise_sql(expected_sql)


def test_generate_alter_drop_queries():
    drop_columns = ["my_column", "my_other_column"]

    sql_list = AthenaIcebergSqlConverter._generate_alter_drop_queries(
        drop_columns,
        "my_table",
        "my_database",
    )

    expected_sql_list = [
        """
        ALTER TABLE my_database.my_table DROP my_column
        """,
        """
        ALTER TABLE my_database.my_table DROP my_other_column
        """,
    ]

    assert [standardise_sql(s) for s in sql_list] == [
        standardise_sql(s) for s in expected_sql_list
    ]


@pytest.mark.parametrize("partitioned", [True, False])
def test_generate_create_from_meta(
    sql_converter, iceberg_metadata_dictionary, partitioned
):
    database_name = "my_database"
    table_location = "s3://my-bucket/my-database/test_table"
    meta = deepcopy(iceberg_metadata_dictionary)

    if not partitioned:
        meta["partitions"] = []
        meta["partition_transforms"] = []

    sql = sql_converter.generate_create_from_meta(
        metadata=meta,
        database_name=database_name,
        table_location=table_location,
    )

    expected_sql = "".join(
        [
            """
            CREATE TABLE my_database.test_table (
                my_int int COMMENT 'This is an integer',
                my_double float COMMENT '',
                my_date date COMMENT '',
                my_decimal decimal(10,2) COMMENT '',
                my_timestamp timestamp COMMENT 'Partition column'
            )
            """,
            """
            PARTITIONED BY (
                my_timestamp
            )
            """
            if partitioned
            else "",
            """
            LOCATION 's3://my-bucket/my-database/test_table'
            TBLPROPERTIES (
                'table_type'='iceberg',
                'format'='parquet'
            )
            """,
        ]
    )

    assert standardise_sql(sql) == standardise_sql(expected_sql)


def test_generate_alter_from_meta(sql_converter, iceberg_metadata_dictionary):
    existing_metadata = deepcopy(iceberg_metadata_dictionary)
    metadata_dict = deepcopy(iceberg_metadata_dictionary)
    meta = IcebergMetadata.from_dict(metadata_dict)

    database_name = "my_database"

    # Add new column
    meta.update_column({"name": "my_new_column", "type": "string"})

    # Update columns
    meta.update_column({"name": "my_double", "type": "float64"})
    meta.update_column({"name": "my_int", "type": "int64"})

    # Drop columns
    meta.remove_column("my_date")
    meta.remove_column("my_decimal")

    queries = sql_converter.generate_alter_from_meta(
        metadata=meta,
        existing_metadata=existing_metadata,
        database_name=database_name,
    )

    expected_queries = [
        """
        ALTER TABLE my_database.test_table ADD COLUMNS (
            my_new_column string
        )
        """,
        """
        ALTER TABLE my_database.test_table
        CHANGE my_int my_int bigint
        """,
        """
        ALTER TABLE my_database.test_table
        CHANGE my_double my_double double
        """,
        """
        ALTER TABLE my_database.test_table DROP my_date
        """,
        """
        ALTER TABLE my_database.test_table DROP my_decimal
        """,
    ]

    assert [standardise_sql(s) for s in queries] == [
        standardise_sql(s) for s in expected_queries
    ]


@pytest.mark.parametrize(
    [
        "create_not_alter",
        "provide_existing_metadata",
        "expected_queries",
    ],
    [
        (
            True,
            None,
            [
                """
                CREATE TABLE my_database.test_table (
                    my_int int COMMENT 'This is an integer',
                    my_double float COMMENT '',
                    my_date date COMMENT '',
                    my_decimal decimal(10,2) COMMENT '',
                    my_timestamp timestamp COMMENT 'Partition column'
                )
                PARTITIONED BY (
                    my_timestamp
                )
                LOCATION 's3://my-bucket/my-database/test_table'
                TBLPROPERTIES (
                    'table_type'='iceberg',
                    'format'='parquet'
                )
                """,
            ],
        ),
        (
            False,
            True,
            [
                """
                ALTER TABLE my_database.test_table ADD COLUMNS (
                    my_new_column string
                )
                """,
                """
                ALTER TABLE my_database.test_table
                CHANGE my_int my_int bigint
                """,
                """
                ALTER TABLE my_database.test_table
                CHANGE my_double my_double double
                """,
                """
                ALTER TABLE my_database.test_table DROP my_date
                """,
                """
                ALTER TABLE my_database.test_table DROP my_decimal
                """,
            ],
        ),
        (
            False,
            False,
            None,
        ),
    ],
)
def test_generate_from_meta(
    sql_converter,
    iceberg_metadata_dictionary,
    create_not_alter,
    provide_existing_metadata,
    expected_queries,
):
    database_name = "my_database"
    table_location = "s3://my-bucket/my-database/test_table"

    if create_not_alter is False and provide_existing_metadata is False:
        with pytest.raises(ValueError, match="existing_metadata must be specified"):
            _ = sql_converter.generate_from_meta(
                metadata=iceberg_metadata_dictionary,
                database_name=database_name,
                table_location=table_location,
                create_not_alter=create_not_alter,
            )

    else:
        existing_metadata = deepcopy(iceberg_metadata_dictionary)
        metadata_dict = deepcopy(iceberg_metadata_dictionary)
        meta = IcebergMetadata.from_dict(metadata_dict)
        kwargs = {}

        if create_not_alter is False:
            # Add new column
            meta.update_column({"name": "my_new_column", "type": "string"})

            # Update columns
            meta.update_column({"name": "my_double", "type": "float64"})
            meta.update_column({"name": "my_int", "type": "int64"})

            # Drop columns
            meta.remove_column("my_date")
            meta.remove_column("my_decimal")

            kwargs["existing_metadata"] = existing_metadata

        kwargs["metadata"] = meta
        kwargs["database_name"] = database_name
        kwargs["table_location"] = table_location
        kwargs["create_not_alter"] = create_not_alter

        queries = sql_converter.generate_from_meta(**kwargs)

        assert [standardise_sql(s) for s in queries] == [
            standardise_sql(s) for s in expected_queries
        ]


def test_get_partitions_from_iceberg_metadata(monkeypatch):
    monkeypatch.setattr(
        ic,
        "read_json_from_s3",
        mock_get_test_iceberg_metadata,
    )

    assert AwsIcebergTable._get_partitions_from_iceberg_metadata(
        "s3://my-bucket/my_database/my_table/metadata/metadata.json"
    ) == (["status"], ["identity"])


@pytest.mark.parametrize(
    [
        "partition_specs",
        "current_partition_spec_id",
        "expected",
    ],
    [
        (
            [
                {
                    "spec-id": 0,
                    "fields": [
                        {
                            "name": "status",
                            "transform": "identity",
                            "source-id": 6,
                            "field-id": 1,
                        },
                    ],
                },
            ],
            0,
            [
                {
                    "name": "status",
                    "transform": "identity",
                    "source-id": 6,
                    "field-id": 1,
                },
            ],
        )
    ],
)
def test_get_current_partition_spec(
    partition_specs, current_partition_spec_id, expected
):
    assert (
        AwsIcebergTable._get_current_partition_spec(
            partition_specs,
            current_partition_spec_id,
        )
        == expected
    )


def test_generate_to_meta(monkeypatch):
    monkeypatch.setattr(botocore.client.BaseClient, "_make_api_call", mock_get_table)
    monkeypatch.setattr(
        ic,
        "read_json_from_s3",
        mock_get_test_iceberg_metadata,
    )
    table = AwsIcebergTable()
    meta = table.generate_to_meta("test_database", "test_iceberg_table")

    assert (
        meta.to_dict()
        == IcebergMetadata.from_json(
            "tests/data/aws_iceberg_converter/expected_glue_metadata.json"
        ).to_dict()
    )


@mock_glue
@mock_s3
@pytest.mark.parametrize(
    ["table_exists", "delete_table_if_exists", "expected"],
    [
        (False, False, False),
        (True, False, True),
        (True, True, False),
    ],
)
def test_pre_generation_setup(
    table_exists,
    delete_table_if_exists,
    expected,
):
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"

    if table_exists:
        s3_client = boto3.client("s3")

        _ = s3_client.create_bucket(
            Bucket="bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
        )

        wr.catalog.create_database("test_database")

        wr.s3.to_parquet(
            df=pd.DataFrame({"col": [1, 2, 3], "col2": ["A", "A", "B"]}),
            path="s3://bucket/prefix",
            dataset=True,
            partition_cols=["col2"],
            database="test_database",
            table="test_table",
        )

    output = AwsIcebergTable._pre_generation_setup(
        "test_database",
        "test_table",
        "s3://bucket/prefix",
        delete_table_if_exists,
    )

    assert output == expected

    if delete_table_if_exists:
        assert not wr.catalog.does_table_exist("test_database", "test_table")
        assert not wr.s3.list_objects("s3://bucket/prefix")
