import logging
import pytest
from sqlalchemy import Column, Table, MetaData, create_engine, inspect
from sqlalchemy.types import (
    Integer,
    Float,
    String,
    DateTime,
    Date,
    Boolean,
    LargeBinary,
    VARCHAR,
    TIMESTAMP,
    DECIMAL,
    BIGINT,
    NUMERIC,
)
from mojap_metadata.converters.sqlalchemy_converter import SQLAlchemyConverter

""" Logging
    https://docs.sqlalchemy.org/en/20/core/engines.html#configuring-logging
    $ pytest tests/test_sqlalchemy.py --log-cli-level=INFO -vv
"""

logging.basicConfig(filename="db.log")
logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)

table_name = "my_table"
sqlite_engine = create_engine("sqlite:///:memory:")
duckdb_engine = create_engine("duckdb:///:memory:")


def create_tables(connectable):
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
    )
    Table(
        'table2', 
        metadata,
        Column('id', String(10), primary_key=True),
    )   
    metadata.create_all(connectable)


def expected_metadata(
    schema, table_description, column_description, primary_key, float_type
):
    return {
        "$schema": "https://moj-analytical-services.github.io/metadata_schema/mojap_metadata/v1.3.0.json",
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
                "type": "bool",
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
    "connectable,schema,table_description,column_description,primary_key,float_type",
    [
        (sqlite_engine, "main", "", "", ["my_string_10"], "float64"),
        (duckdb_engine, "main", "", "", [], "float16"),
        (
            "postgres_engine",
            "public",
            "this is my table",
            "this is the comment",
            ["my_string_10"],
            "float16",
        ),
    ],
)
def test_generate_to_meta(
    connectable,
    schema,
    table_description,
    column_description,
    primary_key,
    float_type,
    postgres_connection,
):
    if connectable == "postgres_engine":
        connectable = postgres_connection[0]
    create_tables(connectable)

    # To check the sqlalchemy data types:
    # insp = inspect(connectable)
    # logging.info(insp.get_table_comment(table_name,schema=schema))
    # for i in insp.get_columns(table_name,schema):
    #     logging.info(i)
    #     logging.info(i["type"])

    sqlc = SQLAlchemyConverter(connectable)
    metadata = sqlc.generate_to_meta(table=table_name, schema=schema)
    assert metadata.to_dict() == expected_metadata(
        schema=schema,
        table_description=table_description,
        column_description=column_description,
        primary_key=primary_key,
        float_type=float_type,
    )

    # To check the mojap metadata types:
    # for i in table_meta.to_dict()["columns"]:
    #     logging.info(i)

    # To check that the metadata objects can be converted:
    # from mojap_metadata.converters.etl_manager_converter import EtlManagerConverter
    # emc = EtlManagerConverter()
    # my_lookup_dict = {"": "parquet"}
    # etl_meta = emc.generate_from_meta(metadata=metadata, file_format_mapper=my_lookup_dict.get)
    # etl_meta.write_to_json(f"{connectable.dialect}.json")

    metadata_list = sqlc.generate_to_meta_list(schema=schema)
    assert len(metadata_list) == 2
    assert metadata_list[0].name == table_name
    assert metadata_list[1].name == "table2"

@pytest.mark.parametrize(
    "inputtype,expected",
    [
        (Integer(), "int32"),
        (BIGINT, "int64"),
        (Float(precision=10, decimal_return_scale=2), "float64"),
        (String(), "string"),
        (String(length=4000), "string"),
        (VARCHAR(length=255), "string"),
        (Date(), "date64"),
        (Boolean(), "bool"),
        (DateTime(), "datetime"),
        (TIMESTAMP(timezone=False), "timestamp(ms)"),
        (NUMERIC(precision=15, scale=2), "decimal128(15,2)"),
        (DECIMAL(precision=8, scale=0), "decimal128(8,0)"),
        (NUMERIC, "decimal128(18,3)"),
        ("Unknown", "string"),
    ],
)
def test_convert_to_mojap_type(inputtype: type, expected: str):
    engine = create_engine("sqlite:///:memory:")
    pc = SQLAlchemyConverter(engine)
    actual = pc.convert_to_mojap_type(str(inputtype))
    assert actual == expected
