import pandas as pd
import pytest
# from mojap_metadata.converters.postgres_converter import PostgresConverter
from mojap_metadata.converters.database_converter import DatabaseConverter
from pathlib import Path
import sqlalchemy as sa
from sqlalchemy import text as sqlAlcText, DDL, event
from sqlalchemy.schema import CreateSchema
from sqlalchemy.types import Integer, Float, String, DateTime, Date, Boolean, VARCHAR, TIMESTAMP, Numeric, Double, DOUBLE_PRECISION

""" Logging... comment out to switch off 
    https://docs.sqlalchemy.org/en/20/core/engines.html#configuring-logging
    $ pytest tests/test_database_converter_postgres.py --log-cli-level=INFO
"""
import logging

logging.basicConfig(filename='db.log')
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

TEST_ROOT = Path(__file__).resolve().parent

def create_schema(engine: sa.engine.Engine, schemaName: str):
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
    # event.listen(engine, 'before_create', DDL("CREATE SCHEMA IF NOT EXISTS schema001"))
        connection.execute(CreateSchema(name=schemaName,if_not_exists=True))

def load_data(postgres_connection):
    """ For loading the data and updating the table with the constraints and metadata
        https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Connection.execute
    """    
    
    path = TEST_ROOT / "data/postgres_extractor"
    files = sorted(Path.glob(path, "postgres*.csv"))
    engine = postgres_connection[0]
    
    create_schema(engine, 'schema001')

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
            schema='schema001',
            if_exists="replace",
            index=False,
            dtype=dtypedict,
        )

        # Sample comment for column for testing
        qryDesc = sqlAlcText("COMMENT ON COLUMN schema001.postgres_table1.my_int IS 'This is the int column';")
        
        # Sample NULLABLE column for testing
        qryPk = sqlAlcText("ALTER TABLE schema001.postgres_table1 ALTER COLUMN primary_key SET NOT NULL;")

        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            res1 = connection.execute(qryDesc)
            res2 = connection.execute(qryPk)
            connection.commit()
        

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

        pc = DatabaseConverter()
        
        output = pc.generate_from_meta(conn, 'schema001')

        for i in output.items():
            assert len(i[1]) == 2
            assert i[0] == "schema: schema001", f'schema name not "public" >> actual value passed = {i[0]}'


def test_meta_data_object(postgres_connection):

    expected = {
        'name': 'postgres_table1',
        'columns': [
            {
                'name': 'my_int',
                'type': 'int32',
                'description': 'This is the int column',
                'nullable': True
            },
            {
                'name': 'my_float',
                'type': 'float64',
                'description': 'None',
                'nullable': True
            },
            {
                'name': 'my_decimal',
                'type': 'float64',
                'description': 'None',
                'nullable': True
            },
            {
                'name': 'my_bool',
                'type': 'bool',
                'description': 'None',
                'nullable': True
            },
            {
                'name': 'my_website',
                'type': 'string',
                'description': 'None',
                'nullable': True
            },
            {
                'name': 'my_email',
                'type': 'string',
                'description': 'None',
                'nullable': True
            },
            {
                'name': 'my_datetime',
                'type': 'timestamp(ms)',
                'description': 'None',
                'nullable': True
            },
            {
                'name': 'my_date',
                'type': 'date64',
                'description': 'None',
                'nullable': True
            },
            {
                'name': 'primary_key',
                'type': 'int32',
                'description': 'None',
                'nullable': False
            },
        ],
        '$schema': 'https://moj-analytical-services.github.io/metadata_schema/mojap_metadata/v1.3.0.json',
        'description': '',
        'file_format': '',
        'sensitive': False,
        'primary_key': [],
        'partitions': [],
    }

    load_data(postgres_connection)
    
    engine = postgres_connection[0]
    with engine.connect() as conn:

        pc = DatabaseConverter()
        meta_output = pc.get_object_meta(conn, "postgres_table1", "schema001")

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
        
        assert meta_output.columns[0]["description"] == "This is the int column", f'column description missmatch, expecting "This is the int column" >> {meta_output.columns[0]}'
        
        assert expected == meta_output.to_dict(), f'expected dictionary not received, actual >> {meta_output.to_dict()}'
        
        

""" 
    This test is not dialect specific, it confirms the mojap meta type convertion.
    It could probably go in a seperate test file...
"""
@pytest.mark.parametrize(
    "inputtype,expected",
    [
        (Integer(), "int32"),
        (Float(precision=10, decimal_return_scale=2), "float64"),
        (String(), "string"),
        (String(length=4000), "string"),
        (VARCHAR(length=255), "string"),
        (Date(), "date64"),
        (Boolean(), "bool"),
        (DateTime(), "datetime"),
        (TIMESTAMP(timezone=False), "timestamp(ms)"),
        (Numeric(precision=15, scale=2), "float64"),
        (Double(decimal_return_scale=4), "float32"),
        (DOUBLE_PRECISION(precision=10), "float64")
    ],
)
def test_convert_to_mojap_type(inputtype: type, expected: str):
    pc = DatabaseConverter()

    actual = pc.convert_to_mojap_type(str(inputtype))

    assert actual == expected
