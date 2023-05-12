"""create collection for company profile

Revision ID: 8d94e92bfe45
Revises: 9176a7458b4d
Create Date: 2022-08-01 09:26:50.652786

"""
from alembic import op
import sqlalchemy as sa
import config
import pymongo
from pymongo import MongoClient

# revision identifiers, used by Alembic.
revision = '8d94e92bfe45'
down_revision = '9176a7458b4d'
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
    if config.MONGODB_CORESIGNAL_STATS not in list_collection_names:
        coresignal_profile = database.create_collection(config.MONGODB_CORESIGNAL_STATS)
        coresignal_profile.create_index([
            ("company_datasource_id", pymongo.ASCENDING),
            ("data_version", pymongo.ASCENDING)], unique=True)


def downgrade():
    pass
