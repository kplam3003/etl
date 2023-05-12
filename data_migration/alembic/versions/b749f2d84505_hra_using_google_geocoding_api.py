"""hra - using google geocoding api

Revision ID: b749f2d84505
Revises: fe5f8877a8b8
Create Date: 2022-10-25 11:40:53.046159

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b749f2d84505"
down_revision = "fe5f8877a8b8"
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

    if collection_name in list_collection_names:
        database[collection_name].rename(f"{collection_name}_bk")
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

    if (
        collection_name in list_collection_names
        and f"{collection_name}_bk" in list_collection_names
    ):
        database.drop_collection(collection_name)
        database[f"{collection_name}_bk"].rename(collection_name)
