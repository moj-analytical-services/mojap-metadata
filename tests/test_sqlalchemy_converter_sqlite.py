import sqlalchemy as sa

from mojap_metadata.converters.sqlalchemy_converter import SQLAlchemyConverter
from mojap_metadata.converters.sqlalchemy_converter import sqlalchemy_functions as df

""" Logging...
    https://docs.sqlalchemy.org/en/20/core/engines.html#configuring-logging
    $ pytest tests/test_sqlalchemy_converter_sqlite.py --log-cli-level=INFO
"""
import logging
logging.basicConfig(filename='db.log')
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


def create_tables() -> sa.engine.Engine:
    """ For loading the data and updating the table with the constraints and metadata
        NOTE. 'Comment' renders a SQL comment in the generated script.
        It is not comparative to the column attribute 'description'.
        SQLite doesn't appear to support table column description.
        https://www.sqlite.org/lang_createtable.html
    """
    engine = sa.create_engine('sqlite://')

    metadata = sa.MetaData()
    sa.Table(
        'people', metadata,
        sa.Column('id', sa.Integer(), primary_key=True, comment='this is the pk'),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('state', sa.String(255), default="Active"),
        sa.Column('flag', sa.Boolean(), default=False)
    )
    sa.Table(
        'places', metadata,
        sa.Column('id', sa.Integer(), primary_key=True, comment='this is the pk'),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('state', sa.String(255), default="Active"),
        sa.Column('flag', sa.Boolean(), default=False)
    )
    metadata.create_all(engine)
    return engine


def test_get_meta_data():
    """ Check table metadata is read properly
        NOTE. 'Comment' is not copied into 'description'.
        SQLite doesn't appear to support table column description.
        https://www.sqlite.org/lang_createtable.html
    """
    expected = {
        'name': 'people',
        'columns': [
            {
                'name': 'id',
                'type': 'int32',
                'description': 'None',
                'nullable': False
            },
            {
                'name': 'name',
                'type': 'string',
                'description': 'None',
                'nullable': False
            },
            {
                'name': 'state',
                'type': 'string',
                'description': 'None',
                'nullable': True
            },
            {
                'name': 'flag',
                'type': 'bool',
                'description': 'None',
                'nullable': True
            }
        ],
        '$schema': 'https://moj-analytical-services.github.io/metadata_schema/\
mojap_metadata/v1.3.0.json',
        'description': '',
        'file_format': '',
        'sensitive': False,
        'primary_key': ['id'],
        'partitions': []
    }

    engine = create_tables()
    with engine.connect() as conn:
        pc = SQLAlchemyConverter()
        metaOutput = pc.get_object_meta(conn, "people", "main")
        e1 = f'Column names do not match. output: {metaOutput.column_names}'

        assert metaOutput.column_names == ['id', 'name', 'state', 'flag'], e1
        assert metaOutput.to_dict() == expected


def test_generate_from_meta():
    engine = create_tables()
    with engine.connect() as conn:
        pc = SQLAlchemyConverter()
        metaOutput = pc.generate_from_meta(conn, "main")

        for i in metaOutput.items():
            e2 = f'schema name not "main" >> actual value passed = {i[0]}'

            assert len(i[1]) == 2
            assert i[0] == "schema: main", e2


def test_list_tables():
    engine = create_tables()
    assert df.list_tables(engine, 'main') == ['people', 'places']
