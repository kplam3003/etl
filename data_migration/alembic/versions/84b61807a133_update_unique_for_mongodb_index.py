"""update unique for mongodb index

Revision ID: 84b61807a133
Revises: e30db71c6a45
Create Date: 2021-08-09 11:14:41.886431

"""
from alembic import op
import sqlalchemy as sa
import config
import pymongo
from pymongo import MongoClient

# revision identifiers, used by Alembic.
revision = '84b61807a133'
down_revision = 'e30db71c6a45'
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

    # VOC collection
    voc_collection = database[config.MONGODB_VOC_REVIEW_COLLECTION]
    try:
        voc_collection.drop_index([("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.TEXT)])
    except:
        pass

    voc_collection.create_index([("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.TEXT)],
                                unique=True)

    # VOE review collection
    voe_collection = database[config.MONGODB_VOE_REVIEW_COLLECTION]
    try:
        voe_collection.drop_index([("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.TEXT)])
    except:
        pass

    voe_collection.create_index([("company_datasource_id", pymongo.ASCENDING), ("review_hash", pymongo.TEXT)],
                                unique=True)

    # VOE job collection
    voe_job_collection = database[config.MONGODB_VOE_JOB_COLLECTION]
    try:
        voe_job_collection.drop_index([("company_datasource_id", pymongo.ASCENDING), ("job_hash", pymongo.TEXT)])
    except:
        pass

    voe_job_collection.create_index([("company_datasource_id", pymongo.ASCENDING), ("job_hash", pymongo.TEXT)],
                                    unique=True)

    # VOE overview Collection
    voe_overview_collection = database[config.MONGODB_VOE_OVERVIEW_COLLECTION]
    try:
        voe_overview_collection.drop_index(
            [("company_datasource_id", pymongo.ASCENDING), ("overview_hash", pymongo.TEXT)])
    except:
        pass

    voe_overview_collection.create_index(
        [("company_datasource_id", pymongo.ASCENDING), ("overview_hash", pymongo.TEXT)], unique=True)


def downgrade():
    pass
