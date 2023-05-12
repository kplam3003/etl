"""Rebuild mongodb index to change index type

Revision ID: eaa6b3494679
Revises: 567776eda0c6
Create Date: 2021-09-14 15:22:47.248754

"""
from alembic import op
import sqlalchemy as sa
import config
import pymongo
from pymongo import MongoClient


# revision identifiers, used by Alembic.
revision = 'eaa6b3494679'
down_revision = '567776eda0c6'
branch_labels = None
depends_on = None


def upgrade():
    mongo_client = mongo_client = MongoClient(
        config.MONGODB_DATABASE_URI, 
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
        socketTimeoutMS=360000, # extend timeout to 5 minutes since building 
        connectTimeoutMS=360000,
        serverSelectionTimeoutMS=360000
    ) 
    database = mongo_client.get_database(config.MONGODB_DATABASE_NAME)
    
    # VOC collection
    voc_collection = database[config.MONGODB_VOC_REVIEW_COLLECTION]
    try:
        print("Trying to drop index for VOC collection...")
        voc_collection.drop_index(
            [("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.TEXT)]
        )
        print("Index dropped successfully!")
    except:
        print("Index dropped failed! Skipped.")
        pass
    
    print("Re-creating proper index...")
    voc_collection.create_index(
        [("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.ASCENDING)],
        unique=True
    )
    
    # VOE collection
    voe_collection = database[config.MONGODB_VOE_REVIEW_COLLECTION]
    try:
        print("Trying to drop index for VOE collection...")
        voe_collection.drop_index(
            [("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.TEXT)]
        )
        print("Index dropped successfully!")
    except:
        print("Index dropped failed! Skipped.")
        pass
    
    print("Re-creating proper index...")
    voe_collection.create_index(
        [("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.ASCENDING)],
        unique=True
    )

    # VOE job collection
    voe_job_collection = database[config.MONGODB_VOE_JOB_COLLECTION]
    try:
        print("Trying to drop index for VOE JOB collection...")
        voe_job_collection.drop_index(
            [("company_datasource_id", pymongo.ASCENDING), ("job_hash", pymongo.TEXT)]
        )
        print("Index dropped successfully!")
    except:
        print("Index dropped failed! Skipped.")
        pass
    
    print("Re-creating proper index...")
    voe_job_collection.create_index(
        [("company_datasource_id", pymongo.ASCENDING), ("job_hash", pymongo.ASCENDING)],
        unique=True
    )

    # VOE overview Collection
    voe_overview_collection = database[config.MONGODB_VOE_OVERVIEW_COLLECTION]
    try:
        print("Trying to drop index for VOE OVERVIEW collection...")
        voe_overview_collection.drop_index(
            [("company_datasource_id", pymongo.ASCENDING), ("overview_hash", pymongo.TEXT)]
        )
        print("Index dropped successfully!")
    except:
        print("Index dropped failed! Skipped.")
        pass

    print("Re-creating proper index...")
    voe_overview_collection.create_index(
        [("company_datasource_id", pymongo.ASCENDING), ("overview_hash", pymongo.ASCENDING)],
        unique=True
    )
    
    print("Index re-created successfully!")


def downgrade():
    mongo_client = mongo_client = MongoClient(
        config.MONGODB_DATABASE_URI, 
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
        socketTimeoutMS=360000, # extend timeout to 5 minutes since building 
        connectTimeoutMS=360000,
        serverSelectionTimeoutMS=360000
    ) 
    database = mongo_client.get_database(config.MONGODB_DATABASE_NAME)
    
    # VOC collection
    voc_collection = database[config.MONGODB_VOC_REVIEW_COLLECTION]
    try:
        print("Trying to drop index for VOC collection...")
        voc_collection.drop_index(
            [("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.ASCENDING)]
        )
        print("Index dropped successfully!")
    except:
        print("Index dropped failed! Skipped.")
        pass
    
    print("Re-creating previous index...")
    voc_collection.create_index(
        [("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.TEXT)],
        unique=True
    )
    
    # VOE collection
    voe_collection = database[config.MONGODB_VOE_REVIEW_COLLECTION]
    try:
        print("Trying to drop index for VOE collection...")
        voe_collection.drop_index(
            [("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.ASCENDING)]
        )
        print("Index dropped successfully!")
    except:
        print("Index dropped failed! Skipped.")
        pass
    
    print("Re-creating previous index...")
    voe_collection.create_index(
        [("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.TEXT)],
        unique=True
    )

    # VOE job collection
    voe_job_collection = database[config.MONGODB_VOE_JOB_COLLECTION]
    try:
        print("Trying to drop index for VOE JOB collection...")
        voe_job_collection.drop_index(
            [("company_datasource_id", pymongo.ASCENDING), ("job_hash", pymongo.ASCENDING)]
        )
        print("Index dropped successfully!")
    except:
        print("Index dropped failed! Skipped.")
        pass
    
    print("Re-creating previous index...")
    voe_job_collection.create_index(
        [("company_datasource_id", pymongo.ASCENDING), ("job_hash", pymongo.TEXT)],
        unique=True
    )

    # VOE overview Collection
    voe_overview_collection = database[config.MONGODB_VOE_OVERVIEW_COLLECTION]
    try:
        print("Trying to drop index for VOE OVERVIEW collection...")
        voe_overview_collection.drop_index(
            [("company_datasource_id", pymongo.ASCENDING), ("overview_hash", pymongo.ASCENDING)]
        )
        print("Index dropped successfully!")
    except:
        print("Index dropped failed! Skipped.")
        pass

    print("Re-creating previous index...")
    voe_overview_collection.create_index(
        [("company_datasource_id", pymongo.ASCENDING), ("overview_hash", pymongo.TEXT)],
        unique=True
    )
    
    print("Index rollbacked successfully!")
