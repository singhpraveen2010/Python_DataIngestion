'''
Created on Mar 17, 2017

@author: Praveen S
'''
import logging

from src.main.com.org.dataIngest.utlity.IOUtility import IOUtility


class Logger(object):
    '''
    class for initialize logger and print logging information into specified file 
    '''
    def __init__(self):
        pass
        
    @staticmethod
    def initialize_logger(properties):
        if properties.get('logger.bases.path') != None and properties.get('logger.level') != None:
            logger_level = properties.get('logger.level')
            log_level = None
            if logger_level == 'INFO':
                log_level =  logging.INFO
            elif logger_level == 'DEBUG':
                log_level =  logging.DEBUG
            elif logger_level == 'ERROR':
                log_level =  logging.ERROR
            elif logger_level == 'WARN':
                log_level =  logging.WARN
           
            logging.basicConfig(filename=properties.get('logger.bases.path') ,level=log_level,format='%(asctime)s - %(levelname)s - %(message)s')
            

if __name__ == '__main__':
    properties_reader = IOUtility()
    data = properties_reader.getProperties('C:\\Users\\n0304026\\workspace\\LMB_DataIngest\\resources\\log.properties')
    Logger.initialize_logger(data)
    logging.info("Print inside file")
    logging.error("This is error message")