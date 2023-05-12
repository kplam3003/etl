"""add company_datasource_id and url columns for load url

Revision ID: 3c6882c6616d
Revises: 9f11ee2c779d
Create Date: 2022-06-17 22:10:01.396306

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery
import gcsfs
import config


# revision identifiers, used by Alembic.
revision = '3c6882c6616d'
down_revision = '9f11ee2c779d'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()


# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
staging = config.STAGING
dwh = config.DWH

query_string1 = f"""
        ALTER TABLE `{project}.{dwh}.voe_nlp_output_case_study`
        ADD COLUMN company_datasource_id INT64,
        ADD COLUMN url STRING;
        ALTER TABLE `{project}.{dwh}.nlp_output_case_study`
        ADD COLUMN company_datasource_id INT64,
        ADD COLUMN url STRING;
        ALTER TABLE `{project}.{staging}.voc`
        ADD COLUMN company_datasource_id INT64,
        ADD COLUMN url STRING;
        ALTER TABLE `{project}.{staging}.voe`
        ADD COLUMN company_datasource_id INT64,
        ADD COLUMN url STRING;
        ALTER TABLE `{project}.{staging}.voc_review_stats`
        ADD COLUMN company_datasource_id INT64,
        ADD COLUMN url STRING;
        ALTER TABLE `{project}.{staging}.voe_review_stats`
        ADD COLUMN company_datasource_id INT64,
        ADD COLUMN url STRING;
    """

query_string2 = f"""
        ALTER TABLE `{project}.{dwh}.voe_nlp_output_case_study`
        DROP COLUMN company_datasource_id,
        DROP COLUMN url;
        ALTER TABLE `{project}.{dwh}.nlp_output_case_study`
        DROP COLUMN company_datasource_id,
        DROP COLUMN url;
        ALTER TABLE `{project}.{staging}.voc`
        DROP COLUMN company_datasource_id,
        DROP COLUMN url;
        ALTER TABLE `{project}.{staging}.voe`
        DROP COLUMN company_datasource_id,
        DROP COLUMN url;
        ALTER TABLE `{project}.{staging}.voc_review_stats`
        DROP COLUMN company_datasource_id,
        DROP COLUMN url;
        ALTER TABLE `{project}.{staging}.voe_review_stats`
        DROP COLUMN company_datasource_id,
        DROP COLUMN url;
"""

def upgrade():
    ##########  Adding columns in voe_nlp_output_case_study #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\Adding columns in voe_nlp_output_case_study, nlp_output_case_study, voc, voe, voc_review_stats, voe_review_stats table successfull!")


def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 3c6882c6616d process is successfull!")
