"""create_table_review_country_mapping_in_staging

Revision ID: b6ee203050cb
Revises: 60be50b9b762
Create Date: 2021-10-21 14:58:47.384172

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
revision = 'b6ee203050cb'
down_revision = '60be50b9b762'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()

# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART


query_string1 = f"""
        CREATE OR REPLACE TABLE `{project}.{staging}.review_country_mapping`
        ( 
        `created_at` 	    TIMESTAMP ,
        `case_study_id` 	INTEGER ,
        `company_id` 	    INTEGER ,
        `source_id` 	    INTEGER ,
        `request_id` 	    INTEGER ,
        `batch_id` 	        INTEGER ,
        `review_id` 	    STRING ,
        `review_country` 	STRING ,
        `country_code` 	    STRING 
        )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) ;
        """
query_string2 = f"""
        CREATE
        OR REPLACE TABLE `{project}.{staging}.review_country_mapping_bk` as
        select
            *
        from
            `{project}.{staging}.review_country_mapping`;

        DROP TABLE `{project}.{staging}.review_country_mapping`; 
        """

def upgrade():
    query_job = bqclient.query(query_string1)
    query_job.result()
    print("\nCreating review_country_mapping table in staging successfull!")
    time.sleep(5)


def downgrade():
    query_job = bqclient.query(query_string2)
    query_job.result()
    print("\n Dropped review_country_mapping and backup as review_country_mapping_bk in staging!")
    print("\n Downgrade 60be50b9b762 process is successfull!")