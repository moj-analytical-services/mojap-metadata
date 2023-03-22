import pandas as pd
import pytest

from mojap_metadata.converters.database_converter import DatabaseConverter
from sqlalchemy.types import Integer, Float, String, DateTime, Date, Boolean
from pathlib import Path
from sqlalchemy import text

TEST_ROOT = Path(__file__).resolve().parent


def load_data(postgres_connection):

    path = TEST_ROOT / "data/postgres_extractor"
    files = sorted(Path.glob(path, "postgres*.csv"))
    engine = postgres_connection[0]

    for file in files:
        # Read file
        tabledf = pd.read_csv(str(file), index_col=None)

        # Create table
        filename = str(file).rsplit("/")[-1].replace(".csv", "")

        # Define datatype
        dtypedict = {
            "my_int": Integer,
            "my_float": Float,
            "my_decimal": Float,
            "my_bool": Boolean,
            "my_website": String,
            "my_email": String,
            "my_datetime": DateTime,
            "my_date": Date,
            "primary_key": Integer,
        }

        tabledf.to_sql(
            filename,
            engine,
            if_exists="replace",
            index=False,
            dtype=dtypedict,
        )

        # Sample comment for column for testing
        
        engine.connect().execute(text(
            """COMMENT ON COLUMN public.postgres_table1.my_int"""
            """ IS 'This is the int column';COMMIT;"""
        ))

        # Sample NULLABLE column for testing
        engine.connect().execute(text(
            """ ALTER TABLE public.postgres_table1 ALTER """
            """ COLUMN primary_key SET NOT NULL;COMMIT;"""
        ))


def test_connection(postgres_connection):
    pgsql = postgres_connection[1]
    engine = postgres_connection[0]
    assert engine is not None
    assert ("system is ready to accept connections") in pgsql.read_bootlog()


def test_dsn_and_url(postgres_connection):
    pgsql = postgres_connection[1]
    expected = {
        "port": 5433,
        "host": "127.0.0.1",
        "user": "postgres",
        "database": "test",
    }

    assert pgsql.dsn() == expected
    assert pgsql.url() == "postgresql://postgres@127.0.0.1:5433/test"


def test_meta_data_object_list(postgres_connection):
    engine = postgres_connection[0]

    with engine.connect() as conn:
        load_data(postgres_connection)

        pc = DatabaseConverter('postgres')
        output = pc.generate_from_meta(conn)

        for i in output.items():
            assert len(i[1]) == 2
            assert i[0] == "schema: public"


def test_meta_data_object(postgres_connection):

    expected = {
        "name": "postgres_table1",
        "columns": [
            {
                "name": "my_int",
                "type": "int32",
                "description": "This is the int column",
                "nullable": True,
            },
            {
                "name": "my_float",
                "type": "float64",
                "description": "None",
                "nullable": True,
            },
            {
                "name": "my_decimal",
                "type": "float64",
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
                "name": "my_website",
                "type": "string",
                "description": "None",
                "nullable": True,
            },
            {
                "name": "my_email",
                "type": "string",
                "description": "None",
                "nullable": True,
            },
            {
                "name": "my_datetime",
                "type": "string",
                "description": "None",
                "nullable": True,
            },
            {
                "name": "my_date",
                "type": "date64",
                "description": "None",
                "nullable": True,
            },
            {
                "name": "primary_key",
                "type": "int32",
                "description": "None",
                "nullable": False,
            },
        ],
        "$schema": "https://moj-analytical-services.github.io/metadata_schema/\
mojap_metadata/v1.3.0.json",
        "description": "",
        "file_format": "",
        "sensitive": False,
        "primary_key": [],
        "partitions": [],
    }

    engine = postgres_connection[0]
    
    with engine.connect() as conn:

        load_data(postgres_connection)

        pc = DatabaseConverter('postgres')
        meta_output = pc.get_object_meta(conn, "postgres_table1", "public")

        assert expected == meta_output.to_dict()

        assert len(meta_output.columns) == 9
        assert meta_output.columns[0]["description"] == "This is the int column"
        assert meta_output.column_names == [
            "my_int",
            "my_float",
            "my_decimal",
            "my_bool",
            "my_website",
            "my_email",
            "my_datetime",
            "my_date",
            "primary_key",
        ]


@pytest.mark.parametrize(
    "inputtype,expected",
    [
        ("int8", "int8"),
        ("integer", "int32"),
        ("numeric", "float64"),
        ("double precision", "float64"),
        ("text", "string"),
        ("uuid", "string"),
        ("character", "string"),
        ("tsvector", "string"),
        ("jsonb", "string"),
        ("varchar", "string"),
        ("bpchar", "string"),
        ("date", "date64"),
        ("bool", "bool"),
        ("datetime", "timestamp(ms)"),
        ("tt", "string"),
    ],
)
def test_convert_to_mojap_type(inputtype: str, expected: str):
    pc = DatabaseConverter('postgres')
    actual = pc.convert_to_mojap_type(inputtype)
    assert actual == expected
