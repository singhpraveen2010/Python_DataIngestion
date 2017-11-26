'''
author: Praveen S
'''


import logging
import ConfigParser
import csv
import pandas
import pymssql
import string
import pyodbc
import re
import json
import collections
import ast
import pprint

class IOUtility(object):
    '''
    class for reading the properties file containing input in key=value format
    '''

    def __init__(self):
        pass


    @staticmethod
    def getProperties(file_name):
        properties_content = dict()
        try:
            with open(file_name, "r") as input_file:
                for line in input_file:
                    tokens = line.split("=")
                    properties_content[tokens[0]] = tokens[1].replace('\n', '').replace('\r', '')
                return properties_content
        except IOError as e:
            e.message = 'Failed to open file {0}'.format(file_name)
            raise
        else:
            logging.info('Read the file content properly')
            input_file.close()


    @staticmethod
    def read_database_configuration(file_name=None):
        config = ConfigParser.RawConfigParser()
        config.read(file_name)
        database_configuration = dict()
        database_configuration['server.hostname'] = config.get('SourceSystemSelection', 'SourceSystem.server')
        database_configuration['database.name'] = config.get('SourceSystemSelection', 'SourceSystem.database')
        database_configuration['user.name'] = config.get('SourceSystemSelection', 'SourceSystem.username')
        database_configuration['source.system.extract.path'] = config.get('SourceSystemSelection', 'SourceSystem.extract_path')
        database_configuration['source.system.keyname'] = config.get('SourceSystemSelection', 'SourceSystem.keyName')
        database_configuration['source.system.encrypted.value'] = config.get('SourceSystemSelection', 'SourceSystem.EncryptedValue')

        return database_configuration



    @staticmethod
    def read_csv_file(file_name=None):
        with open(file_name) as csvfile:
            lines = csvfile.readlines()

        return lines


    @staticmethod
    def write_table_into_csv(table_list=None , database=None , cursor=None , output_path=None, header=True):
        for table in table_list:
            table_name = table
            database_query = " SELECT * from " + database + ".dbo." + table_name

            cursor.execute(database_query)
            with open(output_path + "/" + table_name.rstrip() + ".csv", "wb") as csv_file:

                    csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                    if header == True:
                        csv_writer.writerow([ i[0] for i in cursor.description ])
                    csv_writer.writerows(cursor)
                    logging.info(table_name + " Transfered Successfully to" + output_path)
                    #print table_name + " Transfered Successfully to" + output_path
                    print output_path + "/" + table_name + ".csv.avro"




    @staticmethod
    def write_table_into_csv_masked(table_list=None , database=None , cursor=None , output_path=None, header=True):
        for table in table_list:
            table_name = table
            database_query = " SELECT * from " + database + ".dbo." + table_name

            cursor.execute(database_query)

            mask_dataframe = None
            names = [ x[0] for x in cursor.description]
            rows = cursor.fetchall()
            mask_dataframe = pandas.DataFrame( rows, columns=names)
            #print mask_dataframe
            mask_dataframe.iloc[:, -2:] = 'SECRET VALUE'
            #print mask_dataframe
            #mask_dataframe.columns = [names]
            mask_dataframe.to_csv(output_path + "/" + table_name.rstrip() + "_mask.csv",  header =True ,index=False )
            logging.info(table_name + " Transfered Successfully to" + output_path)
            #print table_name + " Transfered Successfully to" + output_path
            print output_path + "/" + table_name.rstrip() + "_mask.csv"

    @staticmethod
    def schema_file(table_list=None, database=None, cursor=None, output_path=None):

        #connection = pymssql.connect(hostname,user,passcode,db)
        #cursor = connection.cursor()
        #input_file=open(table_list,'r')

        # Read tableList and create txt file for each entry in the tableList
        for table in table_list:
            table_name = table.strip()
            if table_name:
                text_file= output_path + '/' + '%s.csv.avro' %table_name

                logging.info(text_file + " created successfully" )
                #print text_file

            # sql query to fetch column and its data type
            sqlStmt = "SELECT column_name as name, data_type as type FROM information_schema.columns Where TABLE_NAME = '{}'".format(table_name)
            a=cursor.execute(sqlStmt)
            col =[column[0] for column in cursor.description]
            result=[]
            for row in cursor.fetchall():
                result.append(dict(zip(col,row)))


            # dict to json
            i=(ast.literal_eval(json.dumps(result, indent=4, sort_keys=True, separators=(',', ':'))))
            #print i
            logging.info("Writing output to following path : " + text_file)
            createSchema_file=open(text_file, 'w')
            createSchema_file.write('{ \n "type":"record",\n "name":"tblname", \n "fields":[ \n')
            seprator=''

            for d in i:
                for key,val in d.items():

                        # header formatting
                        if key == 'name':
                            a=re.sub(r'\W+', '_', val)
                            d[key]=a

                        # data type conversion
                        if key=='type' and val=='money':
                            d[key] = 'double'
                        elif key=='type' and val=='int':
                            d[key]=='int'
                        elif key == 'type' and val =='long':
                            d[key] == 'long'
                        elif key=='type' and val =='number':
                            d[key] = 'int'
                        elif key=="type" and val =='varchar':
                            d[key] = 'string'
                        elif key=='type' and val == 'char':
                            d[key] = 'string'
                        elif key=='type' and val == 'tinyint':
                            d[key] = 'int'
                        elif key =='type' and val == 'smallint':
                            d[key] = 'int'
                        elif key=="type" and val =="bit":
                            d[key] = 'boolean'
                        elif key=='type' and val =='float':
                            d[key] = 'float'
                        elif key=='type' and val == 'double':
                            d[key] = 'double'
                        elif key =='type':
                            d[key] = 'string'

                seprator+=(json.dumps(d,indent=4))
                seprator+=(',\n')
                #print seprator
                #seprator="]".join([x.rstrip(',') for x in seprator.split("]")])
                #seprator = re.sub(r"\s+(?=\])","",seprator)
                #print seprator

                #seprator = seprator.rstrip('/')

            seprator = seprator[:-2]
            createSchema_file.write(seprator)
            createSchema_file.write('\n \n] \n\n }\n\n')
            createSchema_file.close()


if __name__ == '__main__':
    io_utility = IOUtility()
    data = io_utility.getProperties('C:\\Users\\resources\\log.properties')
    print data
