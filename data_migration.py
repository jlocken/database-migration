import sqlalchemy as db
import pandas as pd
from pandas.io import sql


class data_migration():
    
    def __init__(self,src_conn_string,dest_conn_string):
        '''
        params:
            1.db is an alias of imported sqlalchemy library
            2.sql is a module of pandas.io library
            
        
        '''
        self.src_conn_string=src_conn_string
        self.dest_conn_string=dest_conn_string
        self.db=db
        self.sql=sql
        self.src_schema=src_conn_string.split('/')[-1]
        self.dest_schema=dest_conn_string.split('/')[-1]
        
        
    def create_source_schema_table_proxy(self,src_table):
        '''
        params:
            1.src_table: source table to be migrated
            2.src_schema: source shema whose table is to be migrated
        '''
        try:
            print("Creating DataProxy>>>>>>>>>>>")
            print(f'Creating data proxy from database: {self.src_schema}, table name: {src_table}')
            engine=self.db.create_engine(self.src_conn_string)
            metadata=self.db.MetaData()
            connection=engine.connect()
            table_query_config = self.db.Table(src_table, metadata, autoload=True, autoload_with=engine,schema=self.src_schema)
            query = db.select([table_query_config])
            DataProxy = connection.execute(query)
            print("DataProxy created succesfully!!")
            
            return DataProxy
        
        except Exception as error:
            print("Failed to create data proxy")
            print(error)
    
    def move_data_to_destination_table(self,DataProxy,dest_table, insert_sql,chunksize):
        
        '''
        params:
            1.DataProxy:   proxy object of the queried table
            2.dest_schema: schema were data is to be migrated to
            3.dest_table: destination table for the migrated table
            4.insert_sql: sql query definition that will insert data into the destination table
            5.chunksize: size of the data chunks to be inserted into db
        '''
        try:
            total_records=0
            batch_count=0
            engine=self.db.create_engine(self.dest_conn_string)
            metadata=self.db.MetaData()
            connection=engine.connect()

            flag=True
            while flag:
                partial_results=DataProxy.fetchmany(chunksize)

                if(partial_results)!=[]:
                    batch_count=+1
                    sql.execute(insert_sql,con=engine,params=partial_results)
                    total_records=+len(partial_results)
                    display_partial_results=len(partial_results)
                    print(f'Batch no. {batch_count} of {display_partial_results} records succesfully migrated into the database')

                else:
                    flag=False
            print(f'{total_records} records migrated successfully into {self.dest_schema}')
            
            DataProxy.close()
        except Exception as error:
            print(error)
            
        
        