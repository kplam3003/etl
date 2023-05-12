"""create mongodb collections for voc and voe

Revision ID: 58131feebbec
Revises: 22db1b974903
Create Date: 2021-08-03 14:00:57.394535

"""
from alembic import op
import sqlalchemy as sa
import config
import pymongo
from pymongo import MongoClient


# revision identifiers, used by Alembic.
import config

revision = '58131feebbec'
down_revision = '22db1b974903'
branch_labels = None
depends_on = None


def upgrade():
    mongo_client = MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True
    )
    database = mongo_client.get_database(config.MONGODB_DATABASE_NAME)

    list_collection_names = database.list_collection_names()
    if config.MONGODB_VOC_REVIEW_COLLECTION not in list_collection_names:
        voc_collection = database.create_collection(config.MONGODB_VOC_REVIEW_COLLECTION)
        voc_collection.create_index([("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.TEXT)])

    if config.MONGODB_VOE_REVIEW_COLLECTION not in list_collection_names:
        voe_collection = database.create_collection(config.MONGODB_VOE_REVIEW_COLLECTION)
        voe_collection.create_index([("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.TEXT)])

    if config.MONGODB_VOE_JOB_COLLECTION not in list_collection_names:
        voe_job_collection = database.create_collection(config.MONGODB_VOE_JOB_COLLECTION)
        voe_job_collection.create_index([("company_datasource_id", pymongo.ASCENDING), ("job_hash", pymongo.TEXT)])

    if config.MONGODB_VOE_OVERVIEW_COLLECTION not in list_collection_names:
        voe_overview_collection = database.create_collection(config.MONGODB_VOE_OVERVIEW_COLLECTION)
        voe_overview_collection.create_index([("company_datasource_id", pymongo.ASCENDING), ("overview_hash", pymongo.TEXT)])


def downgrade():
    pass
