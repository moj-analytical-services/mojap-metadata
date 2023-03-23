import pandas as pd
import pytest
import sqlalchemy as sa

from mojap_metadata.converters.database_converter import DatabaseConverter

from sqlalchemy.types import Integer, Float, String, DateTime, Date, Boolean
from pathlib import Path
from sqlalchemy import text as sqlAlcText, exists, select
""" Logging... 
    https://docs.sqlalchemy.org/en/20/core/engines.html#configuring-logging
    $ pytest tests/test_database_converter_sqlite.py --log-cli-level=INFO
"""
import logging

logging.basicConfig(filename='db.log')
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)



import mojap_metadata.converters.database_converter.database_functions as df

engine = sa.create_engine('sqlite://')

def create_tables():
    """ For loading the data and updating the table with the constraints and metadata
    """        
   
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT")  as connection:
        
        logging.info(f"Is Existing? {engine.dialect.has_table(connection, 'schema1.people')}")
        if not engine.dialect.has_table(connection, 'schema1.people'): 

            connection.execute(sqlAlcText("ATTACH DATABASE 'testdb' AS schema1;"))

            metadata = sa.MetaData(engine)
            people = sa.Table('people', metadata,
                    sa.Column('id', sa.Integer(),primary_key=True, comment='this is the pk'),
                    sa.Column('name', sa.String(255), nullable=False),
                    sa.Column('state', sa.String(255), default="Active"),
                    sa.Column('flag', sa.Boolean(), default=False),
                    schema='schema1'
                    )

            places = sa.Table('places', metadata,
                    sa.Column('id', sa.Integer(),primary_key=True, comment='this is the pk'),
                    sa.Column('name', sa.String(255), nullable=False),
                    sa.Column('state', sa.String(255), default="Active"),
                    sa.Column('flag', sa.Boolean(), default=False),
                    schema='schema1'
                    )
            metadata.create_all(engine) 

            logging.info(f"Is Existing now? {engine.dialect.has_table(connection, 'schema1.people')}")

def drop_tables():
    """delete test database instance"""
    metadata = sa.MetaData(engine)
    metadata.drop_all(engine)

    


def test_list_tables():

    drop_tables()
    create_tables()

    
    pc = DatabaseConverter()
    
    logging.info(df.list_tables(engine, 'schema1'))
    logging.info(df.list_meta_data(engine, 'people', 'schema1'))
    
    logging.info(pc.get_object_meta(engine, 'people', 'schema1').to_dict())

    
    assert df.list_tables(engine, 'schema1') == ['people', 'places']

    # columns = []

    # for col in df.list_meta_data(engine, 'people', 'schema1'):
    #     # dType=self._approx_dtype(col['type'])
    #     # columnType = self.convert_to_mojap_type(dType)
    #     columns.append(
    #         {
    #             "name": col['name'].lower(),
    #             # "type": columnType,
    #             "description": col.get('comment'),
    #             "nullable": col.get('nullable'),
    #         }
    #     )
    # logging.info(columns)


       
   
