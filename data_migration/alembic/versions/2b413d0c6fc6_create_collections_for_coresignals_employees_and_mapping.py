"""create collections for coresignals employees and mapping

Revision ID: 2b413d0c6fc6
Revises: 516779ad42cd
Create Date: 2022-08-18 17:51:11.118462

"""
from alembic import op
import sqlalchemy as sa
import config
import pymongo
from pymongo import MongoClient


# revision identifiers, used by Alembic.
revision = "2b413d0c6fc6"
down_revision = "516779ad42cd"
branch_labels = None
depends_on = None


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

    if config.MONGODB_CORESIGNAL_EMPLOYEES not in list_collection_names:
        coresignal_employees = database.create_collection(
            config.MONGODB_CORESIGNAL_EMPLOYEES
        )
        coresignal_employees.create_index(
            [("coresignal_id", pymongo.ASCENDING)],
            unique=True,
        )
    if config.MONGODB_CORESIGNAL_CD_MAPPING not in list_collection_names:
        coresignal_cd_mapping = database.create_collection(
            config.MONGODB_CORESIGNAL_CD_MAPPING
        )
        coresignal_cd_mapping.create_index(
            [
                ("company_datasource_id", pymongo.ASCENDING),
                ("coresignal_company_id", pymongo.ASCENDING)
            ]
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

    if config.MONGODB_CORESIGNAL_EMPLOYEES in list_collection_names:
        database.drop_collection(config.MONGODB_CORESIGNAL_EMPLOYEES)
    if config.MONGODB_CORESIGNAL_CD_MAPPING in list_collection_names:
        database.drop_collection(config.MONGODB_CORESIGNAL_CD_MAPPING)
