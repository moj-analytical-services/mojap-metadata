import pytest
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
    DateTime,
    Date,
    TIMESTAMP,
    DATE,
    DATETIME,
    Boolean,
    LargeBinary,
    BLOB,
    VARBINARY,
    JSON,
)
from mojap_metadata.converters.sqlalchemy_converter import SQLAlchemyConverter


@pytest.mark.parametrize(
    "inputtype,expected",
    [
        (Integer(), "int32"),
        (BIGINT(), "int64"),
        (Float(precision=10, decimal_return_scale=2), "float64"),
        (String(), "string"),
        (String(length=4000), "string"),
        (VARCHAR(length=255), "string"),
        (NCHAR(length=10), "string"),
        (CLOB(), "string"),
        (Date(), "date64"),
        (DateTime(), "datetime"),
        (DATE(), "date64"),
        (DATETIME(), "datetime"),
        (TIMESTAMP(timezone=False), "timestamp(ms)"),
        (Boolean(), "bool"),
        (BLOB(), "binary"),
        (VARBINARY(), "binary"),
        (LargeBinary(), "binary"),
        (NUMERIC(precision=15, scale=2), "decimal128(15,2)"),
        (DECIMAL(precision=8, scale=0), "decimal128(8,0)"),
        (JSON(), "string"),
        ("Unknown", "string"),
    ],
)
def test_convert_to_mojap_type(inputtype: type, expected: str):
    engine = create_engine("sqlite:///:memory:")
    pc = SQLAlchemyConverter(engine)
    actual = pc.convert_to_mojap_type(inputtype)
    assert actual == expected
