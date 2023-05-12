"""MongoDB mapping collection for Places API

Revision ID: e7dab98aa8e1
Revises: 2b413d0c6fc6
Create Date: 2022-08-23 10:33:12.982426

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e7dab98aa8e1"
down_revision = "2b413d0c6fc6"
branch_labels = None
depends_on = None


import config
import pymongo
from pymongo import MongoClient


def upgrade():
    mongo_client = MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
    )
    database = mongo_client.get_database(config.MONGODB_DATABASE_NAME)
    list_collection_names = database.list_collection_names()
    collection_name = config.MONGODB_GOOGLE_MAPS_PLACES_CACHE

    if collection_name not in list_collection_names:
        coresignal_employees = database.create_collection(collection_name)
        coresignal_employees.create_index(
            [("place_id", pymongo.ASCENDING)], unique=True
        )


def downgrade():
    mongo_client = MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
    )
    database = mongo_client.get_database(config.MONGODB_DATABASE_NAME)
    list_collection_names = database.list_collection_names()
    collection_name = config.MONGODB_GOOGLE_MAPS_PLACES_CACHE

    if collection_name in list_collection_names:
        database.drop_collection(collection_name)
