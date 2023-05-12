"""add columns for voe nlp output case study  table

Revision ID: f4bf87765e65
Revises: ed12abb0e0ab
Create Date: 2022-06-07 16:51:00.171819

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import gcsfs
import pandas as pd
import ast
import time
import config


# revision identifiers, used by Alembic.
revision = 'f4bf87765e65'
down_revision = 'ed12abb0e0ab'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()

# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
dwh = config.DWH

query_string1 = f"""
        ALTER TABLE `{project}.{dwh}.voe_nlp_output_case_study`
        ADD COLUMN parent_review_id STRING,
        ADD COLUMN technical_type STRING;
        """

query_string2 = f"""
        ALTER TABLE `{project}.{dwh}.voe_nlp_output_case_study`
        DROP COLUMN parent_review_id STRING,
        DROP COLUMN technical_type STRING;
        """

def upgrade():
    ##########  Adding columns in voe_nlp_output_case_study #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\Adding columns in voe_nlp_output_case_study table successfull!")


def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade f4bf87765e65 process is successfull!")

