'''
Created on Mar 20, 2017

@author: Praveen S
'''

import pymssql
import logging


class SQLServerAPI(object):
    '''
    class contains list of SQL Server API 
    '''

    def __init__(self):
        '''
        Constructor
        '''
        pass
    
    def get_sql_server_connection(self,connection_properties = None , password = None):
        logging.info('create Connection to sql server')
        connection = pymssql.connect(connection_properties.get('server.hostname'), connection_properties.get('user.name'), password, connection_properties.get('database.name'))
        return connection
        
       
    def get_connection_cursor(self,connection = None): 
        return connection.cursor()