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
    )
    Table(
        "table_2",
        metadata,
        Column("id", String(), primary_key=True),
    )
    metadata.create_all(connectable)


def expected_object_meta(description, primary_key, float_type):
    return {
        "name": table_name,
        "columns": [
            {
                "name": "my_string_10",
                "type": "string",
                "description": "None",
                "nullable": False,
            },
            {
                "name": "my_int",
                "type": "int32",
                "description": description,
                "nullable": True,
            },
            {
                "name": "my_string_255",
                "type": "string",
                "description": "None",
                "nullable": True,
            },
            {
                "name": "my_bool",
                "type": "bool",
                "description": "None",
                "nullable": True,
            },
            {
                "name": "my_float",
                "type": float_type,
                "description": "None",
                "nullable": True,
            },
            {
                "name": "my_decimal",
                "type": "decimal128(38,0)",
                "description": "None",
                "nullable": True,
            },
            {
                "name": "my_numeric",
                "type": "decimal128(15,2)",
                "description": "None",
                "nullable": True,
            },
            {
                "name": "my_binary",
                "type": "binary",
                "description": "None",
                "nullable": True,
            },
        ],
        "$schema": "https://moj-analytical-services.github.io/metadata_schema/mojap_metadata/v1.3.0.json",
        "description": "",
        "file_format": "",
        "sensitive": False,
        "primary_key": primary_key,
        "partitions": [],
    }


def compare_meta(
    connectable,
    schema,
    description="this is the comment",
    primary_key=["my_string_10"],
    float_type="float16",
):
    create_tables(connectable)
    # insp = inspect(connectable)
    # for i in insp.get_columns(table_name,schema):
    #     logging.info(i)
    #     logging.info(i["type"])

    pc = SQLAlchemyConverter(connectable)
    object_meta = pc.get_object_meta(table=table_name, schema=schema)
    # for i in object_meta.to_dict()["columns"]:
    #     logging.info(i)
    assert object_meta.to_dict() == expected_object_meta(
        description=description,
        primary_key=primary_key,
        float_type=float_type,
    )

    # from mojap_metadata.converters.etl_manager_converter import EtlManagerConverter
    # emc = EtlManagerConverter()
    # my_lookup_dict = {"": "parquet"}
    # etl_meta = emc.generate_from_meta(metadata=object_meta, file_format_mapper=my_lookup_dict.get)
    # etl_meta.write_to_json(f"{connectable.dialect}.json")

    metaOutput = pc.generate_from_meta(schema=schema)
    for i in metaOutput.items():
        e2 = f"schema name not {schema} >> actual value passed = {i[0]}"
        assert i[0] == f"schema: {schema}", e2
        e3 = f"len not 2 >> actual value passed = {len(i[1])}"
        assert len(i[1]) == 2, e3


def test_sqlalchemy():
    """
    NOTE SQLite doesn't appear to support table column description or smaller float types.
    """
    engine = create_engine("sqlite:///:memory:")
    compare_meta(
        connectable=engine, schema="main", description="None", float_type="float64"
    )


def test_duckdb():
    """
    NOTE Duckdb doesn't appear to support table column description or PK extraction.
    """
    engine = create_engine("duckdb:///:memory:")
    compare_meta(
        connectable=engine,
        schema="main",
        primary_key=[],
        description="None",
    )


def test_postgres(postgres_connection):
    engine = postgres_connection[0]
    compare_meta(connectable=engine, schema="public")


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
        ("Unknown", "string"),
    ],
)
def test_convert_to_mojap_type(inputtype: type, expected: str):
    engine = create_engine("sqlite:///:memory:")
    pc = SQLAlchemyConverter(engine)
    actual = pc.convert_to_mojap_type(str(inputtype))
    assert actual == expected
