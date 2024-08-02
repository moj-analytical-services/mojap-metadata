import pytest
import logging
from sqlalchemy import create_engine
from sqlalchemy.types import (
    String,
    NCHAR,
    VARCHAR,
    CLOB,
    Integer,
    BIGINT,
    Float,
    DECIMAL,
    NUMERIC,
    TIMESTAMP,
    DATE,
    DATETIME,
    Boolean,
    LargeBinary,
    BLOB,
    VARBINARY,
    JSON,
)
from sqlalchemy import Column, Table, MetaData, create_engine
from mojap_metadata.converters.sqlalchemy_converter import (
    SQLAlchemyConverter,
    SQLAlchemyConverterOptions,
)

logging.basicConfig(filename="db.log")
logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)


@pytest.mark.parametrize(
    "inputtype,expected",
    [
        (Integer(), "int32"),
        (BIGINT(), "int64"),
        (String(), "string"),
        (String(length=4000), "string"),
        (VARCHAR(length=255), "string"),
        (NCHAR(length=10), "string"),
        (CLOB(), "string"),
        (DATE(), "date64"),
        (DATETIME(), "timestamp(ms)"),
        (TIMESTAMP(timezone=False), "timestamp(ms)"),
        (Boolean(), "bool"),
        (BLOB(), "binary"),
        (VARBINARY(), "binary"),
        (LargeBinary(), "binary"),
        (Float(precision=10, decimal_return_scale=2), "float64"),
        (NUMERIC(precision=15, scale=2), "decimal128(15,2)"),
        (DECIMAL(precision=8, scale=0), "decimal128(8,0)"),
        (DECIMAL(), "decimal128(38,10)"),
        (DECIMAL(10), "decimal128(10,0)"),
        (JSON(), "string"),
        ("Unknown", "string"),
    ],
)
def test_convert_to_mojap_type(inputtype: type, expected: str):
    engine = create_engine("sqlite:///:memory:")
    pc = SQLAlchemyConverter(engine)
    actual = pc.convert_to_mojap_type(inputtype)
    assert actual == expected


@pytest.mark.parametrize(
    "inputtype,expected",
    [
        (DECIMAL(precision=8, scale=0), "decimal128(8,0)"),
        (DECIMAL(), "decimal128(30,2)"),
        (DECIMAL(10), "decimal128(10,0)"),
    ],
)
def test_convert_to_mojap_type_decimal_default(inputtype: type, expected: str):
    engine = create_engine("sqlite:///:memory:")
    opt = SQLAlchemyConverterOptions(
        default_decimal_precision=30, default_decimal_scale=2
    )
    pc = SQLAlchemyConverter(engine, options=opt)
    actual = pc.convert_to_mojap_type(inputtype)
    assert actual == expected
