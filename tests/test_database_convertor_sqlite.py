import pandas as pd
import pytest
import sqlalchemy as sa

from mojap_metadata.converters.database_converter import DatabaseConverter

from sqlalchemy.types import Integer, Float, String, DateTime, Date, Boolean
from pathlib import Path
from sqlalchemy import text as sqlAlcText
""" Logging... to switch off, in conftest.py, toggle line 51 'echo=False' on postgres_connection 
    https://docs.sqlalchemy.org/en/20/core/engines.html#configuring-logging
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

        

        




def test_meta_data_object_list():
    
    with engine.connect() as conn:
        create_tables()

        pc = DatabaseConverter('sqlite')
        logging.info(df.list_schemas(engine, 'sqlite'))
        
        # output = pc.generate_from_meta(conn)
        
        # for i in output.items():
        #     # if len(i[1]) == 2:
        #     #     tst_pass()
        #     # else:
        #     #     print('length not 2:',len(i[1]))

        #     # if i[0] == "schema: public":
        #     #     tst_pass()
        #     # else:
        #     #     print('schema name not "public":', i[0])

        #     print("test00a", i)
        #     assert len(i[1]) == 1
        #     assert i[0] == "schema: public", f'schema name not "public" >> actual value passed = {i[0]}'


# def test_meta_data_object():
    
#     expected = {
#         "name": "People",
#         "columns": [
#             {
#                 "name": "Id",
#                 "type": "int32",
#                 "description": "",
#                 "nullable": False,
#             },
#             {
#                 "name": "Name",
#                 "type": "character",
#                 "description": "None",
#                 "nullable": True,
#             },
#             {
#                 "name": "State",
#                 "type": "character",
#                 "description": "None",
#                 "nullable": True,
#             },
#             {
#                 "name": "Flag",
#                 "type": "bool",
#                 "description": "None",
#                 "nullable": True,
#             }
#         ],
#         "$schema": "https://moj-analytical-services.github.io/metadata_schema/\
# mojap_metadata/v1.3.0.json",
#         "description": "",
#         "file_format": "",
#         "sensitive": False,
#         "primary_key": [],
#         "partitions": [],
#     }

    
    
#     with engine.connect() as conn:
#         create_tables()

#         pc = DatabaseConverter('sqlite')
#         meta_output = pc.get_object_meta(conn, "People", "public")
#         # print(meta_output.to_dict())
        
#         assert len(meta_output.columns) == 4, f'number of columns not 4, actual length = {len(meta_output.columns)}'
        
#         assert meta_output.column_names == [
#                 "Id",
#                 "Name",
#                 "State",
#                 "Flag"
#             ], f'columns names miss-match >> passed {meta_output.column_names}'
        
#         assert (
#             expected == meta_output.to_dict(), 
#             f'expected dictionary not received, actual >> {meta_output.to_dict()}'
#         )

#         assert meta_output.columns[0]["description"] == "This is the int column", f'column description missmatch, expecting "This is the int column" >> {meta_output.columns[0]}'
        
        


