"""add column to voc, voe custom dimension

Revision ID: 547c79447568
Revises: 3c6882c6616d
Create Date: 2022-06-23 16:24:58.694630

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery
import gcsfs
import config


# revision identifiers, used by Alembic.
revision = '547c79447568'
down_revision = '3c6882c6616d'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()


# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
dwh = config.DWH
staging = config.STAGING


query_string1 = f"""
        ALTER TABLE `{project}.{dwh}.nlp_output_case_study`
        ADD COLUMN review_country STRING,
        ADD COLUMN country_code STRING;
        ALTER TABLE `{project}.{dwh}.voe_nlp_output_case_study`
        ADD COLUMN review_country STRING,
        ADD COLUMN country_code STRING;
        ALTER TABLE `{project}.{staging}.voc_custom_dimension`
        ADD COLUMN country_code STRING,
        ADD COLUMN company_datasource_id INT64,
        ADD COLUMN url STRING;
        ALTER TABLE `{project}.{staging}.voe_custom_dimension`
        ADD COLUMN review_country STRING,
        ADD COLUMN country_code STRING,
        ADD COLUMN company_datasource_id INT64,
        ADD COLUMN url STRING;
    """
query_string2 = f"""
        ALTER TABLE `{project}.{dwh}.nlp_output_case_study`
        DROP COLUMN review_country,
        DROP COLUMN country_code;
        ALTER TABLE `{project}.{dwh}.voe_nlp_output_case_study`
        DROP COLUMN review_country,
        DROP COLUMN country_code;
        ALTER TABLE `{project}.{staging}.voc_custom_dimension`
        DROP COLUMN country_code,
        DROP COLUMN company_datasource_id,
        DROP COLUMN url;
        ALTER TABLE `{project}.{staging}.voe_custom_dimension`
        DROP COLUMN review_country,
        DROP COLUMN country_code,
        DROP COLUMN company_datasource_id,
        DROP COLUMN url;
"""

def upgrade():
    #########  Adding columns in voc_custom_dimension, voe_custom_dimension #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\Adding columns in voc_custom_dimension, voe_custom_dimension table successfull!")


def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 547c79447568 process is successfull!")
