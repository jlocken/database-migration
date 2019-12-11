#!/bin/bash python3
import os, codecs
from configparser import ConfigParser
from data_migration import data_migration
from utils import Utils

if __name__=='__main__':
    
    parser = ConfigParser()
    CONFIG_FILE=os.path.join(os.path.dirname(__file__),'config','config.ini')

    with codecs.open(CONFIG_FILE, 'r', encoding='utf-8') as file:
        parser.read_file(file)
    
    SRC_CONN_STRING=parser['DATABASE']['src_conn_string']
    DEST_CONN_STRING=parser['DATABASE']['dest_conn_string']
    
    migration_object=data_migration(SRC_CONN_STRING,DEST_CONN_STRING)
    utils=Utils()
    migration_utils=utils.migration_utils
    
    for item in migration_utils:
        src_schema=item['src_schema']
        dest_schema=item['dest_schema']
        print(src_schema)
        print(dest_schema)
        for table in item['tables']:
            src_table=table['src']
            dest_table=table['dest']
            dest_insert_sql=table['dest_insert_sql']
            print(table['src'])
            print(table['dest'])
            print(dest_insert_sql)
            
            DataProxy=migration_object.create_source_schema_table_proxy(src_table)
            
            print(DataProxy)
        
            
            #Insert data in chunks of 1000 records from DataProxy into destination database
            migration_object.move_data_to_destination_table(DataProxy,dest_table,dest_insert_sql,chunksize=1000)
            
   
    


