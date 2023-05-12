import psycopg2
from psycopg2 import sql

def execute_insert(config, update_query, values):
    connection = False
    try:
        connection = psycopg2.connect(host=config['DB_HOST'],
                                      port=config['DB_PORT'],
                                      dbname=config['DB_NAME'],
                                      user=config['DB_USER'],
                                      password=config['DB_PASSWORD'])
        cursor = connection.cursor()
        cursor.execute(sql.SQL(update_query).format(**values))
        connection.commit()
        return cursor.fetchone()[0]
    except (Exception, psycopg2.Error) as error :
        raise error
        if(connection):
            print("Failed to connect to postgre because of: ", error)
        return -1
    finally:
        if(connection):
            cursor.close()
            connection.close()

def execute_update(config, update_query, values):
    connection = False
    try:
        connection = psycopg2.connect(host=config['DB_HOST'],
                                      port=config['DB_PORT'],
                                      dbname=config['DB_NAME'],
                                      user=config['DB_USER'],
                                      password=config['DB_PASSWORD'])
        cursor = connection.cursor()
        cursor.execute(sql.SQL(update_query).format(**values))
        connection.commit()
        return cursor.rowcount
    except (Exception, psycopg2.Error) as error :
        raise error
        if(connection):
            print("Failed to connect to postgre because of: ", error)
        return 0
    finally:
        if(connection):
            cursor.close()
            connection.close()

def execute_query_all(config, query, params, fields):
    connection = False
    try:
        connection = psycopg2.connect(host=config['DB_HOST'],
                                      port=config['DB_PORT'],
                                      dbname=config['DB_NAME'],
                                      user=config['DB_USER'],
                                      password=config['DB_PASSWORD']
        )
        cursor = connection.cursor()
        cursor.execute(sql.SQL(query).format(**params))
        records = cursor.fetchall()
        result = []
        for record in records:
            result.append(_mapping_tuple(record, fields))
        return result
    except (Exception, psycopg2.Error) as error :
        raise error
        if(connection):
            print("Failed to connect to postgre because of: ", error)
        return []
    finally:
        if(connection):
            cursor.close()
            connection.close()

def _mapping_tuple(record, fields):
    result_record = {}
    i = 0
    for cell in record:
        result_record[fields[i]] = cell
        i = i + 1
    return result_record

def execute_query_one(config, query, params, fields):
    connection = False
    try:
        connection = psycopg2.connect(host=config['DB_HOST'],
                                      port=config['DB_PORT'],
                                      dbname=config['DB_NAME'],
                                      user=config['DB_USER'],
                                      password=config['DB_PASSWORD'])
        cursor = connection.cursor()

        cursor.execute(sql.SQL(query).format(**params))
        record = _mapping_tuple(cursor.fetchone(), fields)
        return record
    except (Exception, psycopg2.Error) as error :
        raise error
        if(connection):
            print("Failed to connect to postgre because of: ", error)
        return []
    finally:
        if(connection):
            cursor.close()
            connection.close() 