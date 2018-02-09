'''
Created on Mar 15, 2017

@author: Praveen S
'''
'''
Package Required : 
pip install pymssql
pip install pyCrypto
'''
'''class for extracting data from source_mssqlserver database'''

import logging
import os
import sys


from Crypto.Cipher import AES

from src import GenericProperties
from src.main.com.org.dataIngest.utlity.IOUtility import IOUtility
from src.main.com.org.dataIngest.utlity.Logger import Logger
from src.main.com.org.dataIngest.utlity.SQLServerAPI import SQLServerAPI


reload(sys)
sys.setdefaultencoding('utf-8')


class GenerateExtract_BSS(object):
    
    LOGGER_FILE_NAME = 'log.properties'
    
    def __init__(self):
        # initialize logger properties
        log_properties = GenericProperties.RESOURCES_PATH + os.sep + GenerateExtract_BSS.LOGGER_FILE_NAME
        logger_properties = IOUtility.getProperties(log_properties)
        Logger.initialize_logger(logger_properties)

       
    def decrypt(self, key, enc):
        logging.info('Start executing decrypt method')
        BS = 16
        unpad = lambda s : s[0:-ord(s[-1])]
        key = key.decode("hex")
        enc = enc.decode("hex")
        cipher = AES.new(key, AES.MODE_ECB)
        return unpad(cipher.decrypt(enc))
    
    
    
if __name__ == '__main__':
    
    if len(sys.argv) >= 2:    
        # create source_mssqlserver object
        generate_bss_object = GenerateExtract_BSS()
        
        # read database configuration properties
        database_properties = IOUtility.read_database_configuration(file_name=GenericProperties.RESOURCES_PATH + os.sep + 'ConfigFile_mssqlserver.properties')
        
        
        # get decryted password
        password = generate_bss_object.decrypt(key=database_properties.get('source.system.keyname'), enc=database_properties.get('source.system.encrypted.value'))
        
        # Connect to MS SQL server
        sql_server_api = SQLServerAPI()
        sql_connection = sql_server_api.get_sql_server_connection(connection_properties=database_properties , password=password)
        
        
        # get cursor object from connection
        sql_connection_cursor = sql_server_api.get_connection_cursor(connection=sql_connection)
        
        
        #table_details = IOUtility.read_csv_file(file_name=GenericProperties.RESOURCES_PATH + os.sep + 'BSS.csv')
        table_details = list()
        table_details.append(sys.argv[1])
        
        table_list = table_details[0].split(',')
        
        IOUtility.write_table_into_csv(table_list=table_list , database=database_properties.get('database.name') , cursor=sql_connection_cursor , output_path=database_properties.get('source.system.extract.path'))
        IOUtility.schema_file(table_list=table_list , database=database_properties.get('database.name') , cursor=sql_connection_cursor , output_path=database_properties.get('source.system.extract.path'))
        
        
        
        #IOUtility.write_table_into_csv_masked(table_list=table_list , database=database_properties.get('database.name') , cursor=sql_connection_cursor , output_path=database_properties.get('source.system.extract.path'))
        logging.info("Finished Code Successfully")
        # print "Finsished Code Successfully"
    else:
        #print "Invalid command line argument : filename <table name>"
        logging.error("Invalid command line argument : filename <table name>")

