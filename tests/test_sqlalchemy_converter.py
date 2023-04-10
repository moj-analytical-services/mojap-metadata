import sys
import os
import logging
import pytest
from sqlalchemy import Column, Table, MetaData, create_engine
from sqlalchemy.types import (
    String,
    Integer,
    Float,
    DECIMAL,
    NUMERIC,
    Boolean,
    LargeBinary,
)
from mojap_metadata.converters.sqlalchemy_converter import SQLAlchemyConverter


""" Logging
    https://docs.sqlalchemy.org/en/20/core/engines.html#configuring-logging
    $ pytest tests/test_sqlalchemy.py --log-cli-level=INFO -vv
"""

logging.basicConfig(filename="db.log")
logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)
table_name = "my_table"
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

# The following dialects are tested by default, skipped if value is different to 'True'
test_sqlite = os.getenv("TEST_SQLITE", "True") == "True"
test_duckdb = os.getenv("TEST_DUCKDB", "True") == "True"

# The following dialects are skipped by default, tested if value is set to 'True'
test_oracle = os.getenv("TEST_ORACLE", "False") == "True"
test_postgres = os.getenv("TEST_POSTGRES", "False") == "True"


# Create engines
def create_sqlachemy_engine(dialect):
    engine = None
    if dialect == "sqlite":
        engine = create_engine("sqlite:///:memory:")
    elif dialect == "duckdb":
        engine = create_engine("duckdb:///:memory:")
    elif dialect == "oracle":
        import oracledb
        oracledb.version = "8.3.0"
        sys.modules["cx_Oracle"] = oracledb
        engine = create_engine(
            f"oracle://{user}:{password}@localhost:1521/?service_name=XEPDB1"
        )
    elif dialect == "postgres":
        engine = create_engine(
            f"postgresql://postgres:{password}@localhost:5432/postgres"
        )
    return engine


def create_tables(connectable, schema):

    metadata = MetaData()
    Table(
        table_name,
        metadata,
        Column("my_string_10", String(10), primary_key=True, nullable=False),
        Column("my_int", Integer(), comment="this is the comment"),
        Column("my_string_255", String(255), default="Active"),
        Column("my_bool", Boolean(), default=False),
        Column("my_float", Float(precision=10, decimal_return_scale=2)),
        Column("my_decimal", DECIMAL(precision=38, scale=0, asdecimal=True)),
        Column("my_numeric", NUMERIC(precision=15, scale=2)),
        Column("my_binary", LargeBinary()),
        comment="this is my table",
        schema=schema,
    )
    Table(
        "table2",
        metadata,
        Column("id", String(10), primary_key=True),
        schema=schema,
    )
    metadata.create_all(connectable)


def expected_metadata(
    schema,
    table_description,
    column_description,
    primary_key,
    float_type,
    bool_type,
):
    return {
        "$schema": "https://moj-analytical-services.github.io/metadata_schema/\
mojap_metadata/v1.3.0.json",
        "name": table_name,
        "database": schema,
        "description": table_description,
        "file_format": "",
        "sensitive": False,
        "primary_key": primary_key,
        "partitions": [],
        "columns": [
            {
                "name": "my_string_10",
                "type": "string",
                "description": "",
                "nullable": False,
            },
            {
                "name": "my_int",
                "type": "int32",
                "description": column_description,
                "nullable": True,
            },
            {
                "name": "my_string_255",
                "type": "string",
                "description": "",
                "nullable": True,
            },
            {
                "name": "my_bool",
                "type": bool_type,
                "description": "",
                "nullable": True,
            },
            {
                "name": "my_float",
                "type": float_type,
                "description": "",
                "nullable": True,
            },
            {
                "name": "my_decimal",
                "type": "decimal128(38,0)",
                "description": "",
                "nullable": True,
            },
            {
                "name": "my_numeric",
                "type": "decimal128(15,2)",
                "description": "",
                "nullable": True,
            },
            {
                "name": "my_binary",
                "type": "binary",
                "description": "",
                "nullable": True,
            },
        ],
    }


@pytest.mark.parametrize(
    "dialect,schema,table_description,column_description,"
    "primary_key,float_type,bool_type",
    [
        pytest.param(
            "sqlite",
            "main",
            "",
            "",
            ["my_string_10"],
            "float64",
            "bool",
            marks=(pytest.mark.skipif(test_sqlite is False, reason="skip sqlite test")),
        ),
        pytest.param(
            "duckdb",
            "main",
            "",
            "",
            [],
            "float16",
            "bool",
            marks=(pytest.mark.skipif(test_duckdb is False, reason="skip duckdb test")),
        ),
        pytest.param(
            "postgres",
            "public",
            "this is my table",
            "this is the comment",
            ["my_string_10"],
            "float16",
            "bool",
            marks=(
                pytest.mark.skipif(test_postgres is False, reason="skip postgres test")
            ),
        ),
        pytest.param(
            "oracle",
            user,
            "this is my table",
            "this is the comment",
            ["my_string_10"],
            "float64",
            "int32",
            marks=(pytest.mark.skipif(test_oracle is False, reason="skip oracle test")),
        ),
    ],
)
def test_generate_to_meta(
    dialect,
    schema,
    table_description,
    column_description,
    primary_key,
    float_type,
    bool_type,
):

    connectable = create_sqlachemy_engine(dialect)
    create_tables(connectable, schema)

    sqlc = SQLAlchemyConverter(connectable)
    metadata = sqlc.generate_to_meta(table=table_name, schema=schema)
    assert metadata.to_dict() == expected_metadata(
        schema=schema,
        table_description=table_description,
        column_description=column_description,
        primary_key=primary_key,
        float_type=float_type,
        bool_type=bool_type,
    )

    metadata_list = sqlc.generate_to_meta_list(schema=schema)
    assert len(metadata_list) == 2
    assert metadata_list[0].name == table_name
    assert metadata_list[1].name == "table2"
