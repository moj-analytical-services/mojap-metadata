import pytest
import pandas as pd


from mojap_metadata.extractors import postgres_metadata as m
from pathlib import Path
from sqlalchemy.types import *


TEST_ROOT = Path(__file__).resolve().parent


class TestDBClass:
    # @pytest.fixture(scope="module", autouse=True)
    def load_data(self, postgres_connection):

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
                "my_bool": String,
                "my_website": String,
                "my_email": String,
                "my_datetime": DateTime,
                "my_date": Date,
                "primary_key": Integer,
            }
            tabledf.to_sql(
                filename,
                postgres_connection[0],
                if_exists="replace",
                index=False,
                dtype=dtypedict,
            )

            # Sample comment for column for testing
            engine.connect().execute(
                f"""COMMENT ON COLUMN public.postgres_table1.my_int IS 'This is the int column';COMMIT;"""
            )

    def test_connection(self, postgres_connection):
        pgsql = postgres_connection[1]
        assert postgres_connection[0] is not None
        assert ("system is ready to accept connections") in pgsql.read_bootlog()

    def test_dsn_and_url(self, postgres_connection):
        pgsql = postgres_connection[1]
        expected = {
            "port": 5433,
            "host": "127.0.0.1",
            "user": "postgres",
            "database": "test",
        }

        assert pgsql.dsn() == expected
        assert pgsql.url() == "postgresql://postgres@127.0.0.1:5433/test"

    def test_meta_data_object_list(self, postgres_connection):
        conn = postgres_connection[0].connect()
        self.load_data(postgres_connection)
        output = m.get_object_meta_for_all_tables(conn)

        for i in output.items():
            print(i)
            assert len(i[1]) == 2
            assert i[0] == "schema: public"

    def test_meta_data_object(self, postgres_connection):
        conn = postgres_connection[0].connect()
        meta = m.get_object_meta(conn, "postgres_table1", "public")
        assert len(meta.columns) == 9
        assert meta.columns[0]["description"] == "This is the int column"
        assert meta.column_names == [
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


# Update read me file

# Add comments  to function
# Add datatype  whilw creating table
