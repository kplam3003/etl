import sys
sys.path.append('../')

import sqlalchemy
import os
import logging

from google.cloud import bigquery as bq
from google.oauth2 import service_account

import config
from core.logger import init_logger

# create logger
logger = init_logger(config.LOGGER_NAME, config.LOGGER)


def make_postgres_connection(connection_uri: str = None, 
                             config: dict = None, 
                             bigquery_credentials_path: str = None,
                             connect_timeout: int = None):
    """
    Make connection to database postgresql base on dict of configs
    Dict of config must have the following keys:
        - username: username to log into the server
        - pwd: password
        - host: address of host
        - port: port through which connection is made
        - db_name: database name
    """
    try:
        if connection_uri:
            connection_string = connection_uri
        else:
            connection_string = f"postgresql+psycopg2://{config['username']}:{config['pwd']}" \
                    + f"@{config['host']}:{config['port']}" \
                    + f"/{config['db_name']}"
                    
        arguments = {
            'name_or_url': connection_string,
            'isolation_level': 'READ COMMITTED'
        }
        # timeout setting
        if connect_timeout:
            arguments['connect_args'] = {'connect_timeout': connect_timeout}
        # for bigquery authentication connection
        if bigquery_credentials_path:
            arguments['credentials_path'] = bigquery_credentials_path

        # create engine with arguments dict
        engine = sqlalchemy.create_engine(**arguments)
        
        # engine created successfully
        logger.info('Connection engine created successfully')
        return engine
    except Exception as e:
        logger.error("Fail to create connection engine")
        logger.error(f'Error happened: {e}')
        raise e


def check_dir_exists(d: str, create: bool = True) -> bool:
    if os.path.exists(d):
        return True
    else:
        if create:
            os.mkdir(d)
        return False


def create_bigquery_client():
    # create bigquery client
    logger.info('Creating bigquery client...')
    try:
        client = bq.Client()
        logger.info('Bigquery client created successfully')
        return client
    except Exception as e:
        logger.critical(f'Failed to create client. Error: {e.__class__.__name__}')
        logger.critical(str(e))
        raise e


def create_bigquery_client_from_json(key_path):
    # create credentials:
    try:
        credentials = service_account.Credentials.from_service_account_file(key_path)
        client = bq.Client(credentials=credentials, project=credentials.project_id)
        return client
    except Exception as e:
        logger.critical(f'Failed to create client. Error: {e.__class__.__name__}')
        logger.critical(str(e))
        raise e


def extract_bq_table_schema(client, table_id): 
    table = client.get_table(table_id)  # API Request
    # View table properties
    return table.schema


def convert_list_to_string(lst):
    string = ''
    for i in lst:
        string += f'{str(i)}, '
    string = string[:-2]
    return string


def split_batch_name_into_company_source(batch_name):
    if not isinstance(batch_name, str):
        return [0, 0]
    
    splitted = batch_name.split('-')
    if len(splitted) < 3:
        return [0, 0]
    
    try:
        if (len(splitted[0]) > 5) or not splitted[0].isnumeric():
            return [int(i) for i in splitted[-2:]]
        else:
            return [int(i) for i in splitted[:2]]
    except ValueError:
        return [0, 0]
