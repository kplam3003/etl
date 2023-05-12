"""create collections to store crawled review_stats data in mongodb

Revision ID: 9f11ee2c779d
Revises: 16ffeb0ac48f
Create Date: 2022-06-09 09:16:19.152853

"""
from alembic import op
import sqlalchemy as sa
import config
import pymongo
from pymongo import MongoClient


# revision identifiers, used by Alembic.
revision = '9f11ee2c779d'
down_revision = '16ffeb0ac48f'
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
    if config.MONGODB_VOC_REVIEW_STATS not in list_collection_names:
        voc_review_stats = database.create_collection(config.MONGODB_VOC_REVIEW_STATS)
        voc_review_stats.create_index([
            ("company_datasource_id", pymongo.ASCENDING),
            ("data_version", pymongo.ASCENDING)], unique=True)
    if config.MONGODB_VOE_REVIEW_STATS not in list_collection_names:
        voe_review_stats = database.create_collection(config.MONGODB_VOE_REVIEW_STATS)
        voe_review_stats.create_index([
            ("company_datasource_id", pymongo.ASCENDING),
            ("data_version", pymongo.ASCENDING)], unique=True)

def downgrade():
    pass
