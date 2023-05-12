import pymongo
from pymongo import MongoClient, errors
from core.logger import auto_logger


class MongoDBUtil:
    uri = None
    db_name = ""

    @staticmethod
    def init(uri, database_name):
        MongoDBUtil.uri = uri
        MongoDBUtil.db_name = database_name

    @staticmethod
    @auto_logger
    def find(collection, filter, logger=None, *args, **kwargs):
        client = MongoClient(MongoDBUtil.uri)
        db = client.get_database(MongoDBUtil.db_name)

        try:
            return True, db[collection].find(filter, *args, **kwargs)
        except errors.PyMongoError as error:
            logger.exception(f"Error while finding collection {collection} filter {filter} - Error: {error}")
            return False, error
        finally:
            client.close()
            del client

    @staticmethod
    @auto_logger
    def insert_one(collection, data, logger=None, *args, **kwargs):
        client = MongoClient(MongoDBUtil.uri)
        db = client.get_database(MongoDBUtil.db_name)
        try:
            return True, db[collection].insert_one(data)
        except errors.PyMongoError as error:
            logger.exception(f"Error while insert collection {collection} data {data} - Error: {error}")
            return False, error
        finally:
            client.close()
            del client


    @staticmethod
    @auto_logger
    def insert_many(collection, data, logger=None, *args, **kwargs):
        client = MongoClient(MongoDBUtil.uri)
        db = client.get_database(MongoDBUtil.db_name)
        try:
            return True, db[collection].insert_many(data)
        except errors.PyMongoError as error:
            logger.exception(f"Error while insert collection {collection} data {data} - Error: {error}")
            return False, error
        finally:
            client.close()
            del client

    @staticmethod
    @auto_logger
    def count(collection, filter, logger=None, *args, **kwargs):
        client = MongoClient(MongoDBUtil.uri)
        db = client.get_database(MongoDBUtil.db_name)
        try:
            return True, db[collection].count_documents(filter=filter)
        except errors.PyMongoError as error:
            logger.exception(f"Error while execute count {collection} with filter {filter} - Error: {error}")
            return False, error
        finally:
            client.close()
            del client


    @staticmethod
    @auto_logger
    def update_one(collection, filter, update, logger=None, *args, **kwargs):
        client = MongoClient(MongoDBUtil.uri)
        db = client.get_database(MongoDBUtil.db_name)
        try:
            return True, db[collection].update_one(filter=filter, update=update, *args, **kwargs)
        except errors.WriteError as error:
            logger.exception(f"Error while execute update {update} collection {collection} with filter {filter} - Error: {error}")
            return False, error
        finally:
            client.close()
            del client


    @staticmethod
    @auto_logger
    def update_many(collection, filter, update, logger=None, *args, **kwargs):
        client = MongoClient(MongoDBUtil.uri)
        db = client.get_database(MongoDBUtil.db_name)
        try:
            return True, db[collection].update_many(filter=filter, update=update, *args, **kwargs)
        except errors.PyMongoError as error:
            logger.exception(f"Error while execute update {update} collection {collection} with filter {filter} - Error: {error}")
            return False, error
        finally:
            client.close()
            del client
