import json
import os

import boto3
import psycopg2
import pytest
import testing.postgresql
from moto import mock_glue
from sqlalchemy import create_engine

from mojap_metadata import Metadata
from mojap_metadata.converters.aws_iceberg_converter import AthenaIcebergSqlConverter


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    mocked_envs = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ]
    for menv in mocked_envs:
        os.environ[menv] = "testing"

    yield  # Allows us to close down envs on exit

    for menv in mocked_envs:
        del os.environ[menv]


@pytest.fixture(scope="function")
def glue_client(aws_credentials):
    with mock_glue():
        yield boto3.client("glue", region_name="eu-west-1")


@pytest.fixture(scope="session")
def postgres_connection():

    psql = testing.postgresql.Postgresql(port=5433)
    info = psql.dsn()

    connection = psycopg2.connect(
        user=info["user"],
        host=info["host"],
        database=info["database"],
        port=info["port"],
        password="postgres",  # pragma: allowlist secret
    )
    connection.autocommit = True
    engine = create_engine(psql.url())
    yield engine, psql, connection  # Allows us to close down envs on exit
    psql = psql.stop()
    connection.close()


# test input and expected metadata for the Metadata.column_names_to_lower
# and columns_names_to_upper tests
@pytest.fixture(scope="function")
def meta_input():
    meta = Metadata(
        columns=[
            {"name": "A", "type": "int8"},
            {"name": "b", "type": "string"},
            {"name": "C", "type": "date32"},
            {"name": "D", "type": "date32"},
            {"name": "e", "type": "date32"},
        ]
    )
    return meta


@pytest.fixture(scope="function")
def expected_meta_out_lower():
    meta = Metadata(
        columns=[
            {"name": "a", "type": "int8"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "date32"},
            {"name": "d", "type": "date32"},
            {"name": "e", "type": "date32"},
        ]
    )
    return meta


@pytest.fixture(scope="function")
def expected_meta_out_upper():
    meta = Metadata(
        columns=[
            {"name": "A", "type": "int8"},
            {"name": "B", "type": "string"},
            {"name": "C", "type": "date32"},
            {"name": "D", "type": "date32"},
            {"name": "E", "type": "date32"},
        ]
    )
    return meta


@pytest.fixture(scope="module")
def sql_converter():
    yield AthenaIcebergSqlConverter()


@pytest.fixture(scope="module")
def iceberg_metadata_dictionary():
    with open("tests/data/aws_iceberg_converter/metadata.json", "r") as f:
        meta_dict = json.load(f)
    yield meta_dict
