"""Add cors to gcs for web upload

Revision ID: 4cabe518212b
Revises: f5241224ddd0
Create Date: 2021-03-26 13:40:18.232127

"""
from alembic import op
import sqlalchemy as sa

import config
from google.cloud import storage


# revision identifiers, used by Alembic.
revision = '4cabe518212b'
down_revision = 'f5241224ddd0'
branch_labels = None
depends_on = None


def upgrade():
    bucket_name = config.BUCKET_NAME
    origin = config.GCP_STORAGE_CORS_WEBURL
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bucket.cors = [
        {
            "origin": [origin],
            "responseHeader": ["*"],
            "method": ['*']
        }
    ]
    bucket.patch()


def downgrade():
    bucket_name = config.BUCKET_NAME
    origin = config.GCP_STORAGE_CORS_WEBURL
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bucket.cors = []
    bucket.patch()
    