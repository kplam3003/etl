from psycopg2 import sql, connect
from urllib.parse import urlparse, unquote
import config


def open_etl_database_connection():
    r = urlparse(config.ETL_DATABASE_URI)
    return connect(host=r.hostname, port=r.port, dbname=r.path[1:], user=r.username, password=unquote(r.password))

