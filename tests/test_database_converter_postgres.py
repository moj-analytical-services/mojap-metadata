import pandas as pd
import pytest
# from mojap_metadata.converters.postgres_converter import PostgresConverter
from mojap_metadata.converters.database_converter import DatabaseConverter
from sqlalchemy.types import Integer, Float, String, DateTime, Date, Boolean
from pathlib import Path
from sqlalchemy import text as sqlAlcText
""" Logging... to switch off, in conftest.py, toggle line 51 'echo=False' on postgres_connection 
    https://docs.sqlalchemy.org/en/20/core/engines.html#configuring-logging
"""
# import logging

# logging.basicConfig(filename='db.log')
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

TEST_ROOT = Path(__file__).resolve().parent


def load_data(postgres_connection):
    """ For loading the data and updating the table with the constraints and metadata
        https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Connection.execute
    """    
        
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
        qryDesc = sqlAlcText("COMMENT ON COLUMN postgres_table1.my_int IS 'This is the int column';")
        
        # Sample NULLABLE column for testing
        qryPk = sqlAlcText("ALTER TABLE public.postgres_table1 ALTER COLUMN primary_key SET NOT NULL;")

        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            res1 = connection.execute(qryDesc)
            res2 = connection.execute(qryPk)
            connection.commit()

            # pc = DatabaseConverter('postgres')
            # meta_output = pc.get_object_meta(connection, "postgres_table1", "public")
            # print('test001', meta_output.to_dict())

            # pc = PostgresConverter()
            # meta_output = pc.get_object_meta(connection, "postgres_table1", "public")
            # print('test002', meta_output.to_dict())
        

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


def tst_pass(outcome: bool = True):
    assert outcome

def test_meta_data_object_list(postgres_connection):
    engine = postgres_connection[0]

    with engine.connect() as conn:
        load_data(postgres_connection)

        pc = DatabaseConverter('postgres')
        output = pc.generate_from_meta(conn)
        
        for i in output.items():
            # if len(i[1]) == 2:
            #     tst_pass()
            # else:
            #     print('length not 2:',len(i[1]))

            # if i[0] == "schema: public":
            #     tst_pass()
            # else:
            #     print('schema name not "public":', i[0])


            assert len(i[1]) == 2
            assert i[0] == "schema: public", f'schema name not "public" >> actual value passed = {i[0]}'


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

    load_data(postgres_connection)
    
    engine = postgres_connection[0]
    with engine.connect() as conn:

        pc = DatabaseConverter('postgres')
        meta_output = pc.get_object_meta(conn, "postgres_table1", "public")
        # print(meta_output.to_dict())
        
        assert len(meta_output.columns) == 9, f'number of columns not 9, actual length = {len(meta_output.columns)}'
        
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
            ], f'columns names miss-match >> passed {meta_output.column_names}'
        
        assert (
            expected == meta_output.to_dict(), 
            f'expected dictionary not received, actual >> {meta_output.to_dict()}'
        )

        # assert meta_output.columns[0]["description"] == "This is the int column", f'column description missmatch, expecting "This is the int column" >> {meta_output.columns[0]}'
        
        


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
