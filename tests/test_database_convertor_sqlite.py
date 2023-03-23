import pandas as pd
import pytest
import sqlalchemy as sa

from mojap_metadata.converters.database_converter import DatabaseConverter

from sqlalchemy.types import Integer, Float, String, DateTime, Date, Boolean
from pathlib import Path
from sqlalchemy import text as sqlAlcText, exists, select
""" Logging... 
    https://docs.sqlalchemy.org/en/20/core/engines.html#configuring-logging
    pytest tests/test_database_convertor_sqlite.py --log-cli-level=INFO
"""
import logging

logging.basicConfig(filename='db.log')
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)



import mojap_metadata.converters.database_converter.database_functions as df

engine = sa.create_engine('sqlite://')

# def test_schema_exists():
#     """ check if schema has already been created """
#     return exists(
#         select([('schema_name')]).select_from("information_schema.schemata").where("schema_name = 'schema1'")
#         )

def create_tables():
    """ For loading the data and updating the table with the constraints and metadata
    """        
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT")  as connection:
        
        connection.execute(sqlAlcText("ATTACH DATABASE 'testdb' AS schema1;"))

        metadata = sa.MetaData(engine)
        people = sa.Table('people', metadata,
                sa.Column('id', sa.Integer(),primary_key=True, comment='this is the pk'),
                sa.Column('name', sa.String(255), nullable=False),
                sa.Column('state', sa.String(255), default="Active"),
                sa.Column('flag', sa.Boolean(), default=False),
                schema='schema1'
                )
        # metadata.create_all(engine) 

        places = sa.Table('places', metadata,
                sa.Column('id', sa.Integer(),primary_key=True, comment='this is the pk'),
                sa.Column('name', sa.String(255), nullable=False),
                sa.Column('state', sa.String(255), default="Active"),
                sa.Column('flag', sa.Boolean(), default=False),
                schema='schema1'
                )
        metadata.create_all(engine) 

# def setup_tests():
#     if not test_schema_exists(): 
#         create_tables()
    


# def test_list_schema():
#     setup_tests()
#     assert df.list_tables(engine, 'schema1') == ['main', 'schema1']



def test_list_tables():
    create_tables()

    pc = DatabaseConverter('sqlite')
    logging.info(df.list_schemas(engine, 'sqlite'))
    logging.info(df.list_tables(engine, 'schema1'))
    logging.info(df.list_meta_data(engine, 'people', 'schema1'))
    
    logging.info(pc.get_object_meta(engine, 'people', 'schema1'))
    
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


       
   
