import boto3
import os
import pytest
import psycopg2

from moto import mock_glue
import testing.postgresql
from sqlalchemy import create_engine


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
        password="postgres",
    )
    connection.autocommit = True
    engine = create_engine(psql.url())
    yield engine, psql, connection  # Allows us to close down envs on exit
    psql = psql.stop()
    connection.close()
