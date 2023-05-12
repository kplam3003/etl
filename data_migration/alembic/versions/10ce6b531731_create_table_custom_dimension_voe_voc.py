"""create_table_custom_dimension_voe_voc

Revision ID: 10ce6b531731
Revises: a21e23773801
Create Date: 2021-10-15 11:37:38.954816

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
revision = '10ce6b531731'
down_revision = 'a21e23773801'
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

query_string1_voc = f"""
        CREATE OR REPLACE TABLE `{project}.{staging}.voc_custom_dimension`
        ( 
        `created_at`        TIMESTAMP ,
        `review_id`         STRING ,
        `source_name`       STRING ,
        `company_name`      STRING ,
        `nlp_pack`          STRING ,
        `nlp_type`          STRING ,
        `user_name`         STRING ,
        `language`          STRING ,
        `review`            STRING ,
        `trans_review`      STRING ,
        `trans_status`      STRING ,
        `code`              STRING ,
        `dimension`         STRING ,
        `label`             STRING ,
        `terms`             STRING ,
        `relevance`         FLOAT64 ,
        `rel_relevance`     FLOAT64 ,
        `polarity`          STRING ,
        `rating`            FLOAT64 ,
        `batch_id`          INTEGER, 
        `batch_name`        STRING ,
        `file_name`         STRING ,
        `review_date`       TIMESTAMP, 
        `company_id`        INTEGER ,
        `source_id`         INTEGER ,
        `step_id`           INTEGER ,
        `request_id`        INTEGER ,
        `case_study_id`     INTEGER ,
        `parent_review_id`  STRING ,
        `technical_type`    STRING ,
        `review_country`    STRING,
        `run_id`            STRING
        )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) ;
        """

query_string1_voe = f"""
        CREATE OR REPLACE TABLE `{project}.{staging}.voe_custom_dimension`
        ( 
        `created_at`        TIMESTAMP, 
        `review_id`         STRING ,
        `source_name`       STRING ,
        `company_name`      STRING ,
        `nlp_pack`          STRING ,
        `nlp_type`          STRING ,
        `user_name`         STRING ,
        `language`          STRING ,
        `review`            STRING ,
        `trans_review`      STRING ,
        `trans_status`      STRING ,
        `code`              STRING ,
        `dimension`         STRING ,
        `label`             STRING ,
        `terms`             STRING ,
        `relevance`         FLOAT64 ,
        `rel_relevance`     FLOAT64 ,
        `polarity`          STRING ,
        `rating`            FLOAT64 ,
        `batch_id`          INTEGER, 
        `batch_name`        STRING ,
        `file_name`         STRING ,
        `review_date`       TIMESTAMP, 
        `company_id`        INTEGER ,
        `source_id`         INTEGER ,
        `step_id`           INTEGER ,
        `request_id`        INTEGER ,
        `case_study_id`     INTEGER ,
        `parent_review_id`  STRING ,
        `technical_type`    STRING ,
        `run_id`            STRING
        )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) ;
        """

query_string2_voc = f"""
        CREATE
        OR REPLACE TABLE `{project}.{staging}.voc_custom_dimension_bk` as
        select
            *
        from
            `{project}.{staging}.voc_custom_dimension`;

        DROP TABLE `{project}.{staging}.voc_custom_dimension`; 
        """

query_string2_voe = f"""
        CREATE
        OR REPLACE TABLE `{project}.{staging}.voe_custom_dimension_bk` as
        select
            *
        from
            `{project}.{staging}.voe_custom_dimension`;

        DROP TABLE `{project}.{staging}.voe_custom_dimension`; 
        """

def upgrade():
    ##########  CREATE voc_custom_dimension TABLE IN staging #################

    query_job = bqclient.query(query_string1_voc)
    query_job.result()
    print("\nCreating voc_custom_dimension table in staging successfull!")
    time.sleep(5)
    ##########  CREATE voe_custom_dimension TABLE IN staging #################
    query_job = bqclient.query(query_string1_voe)
    query_job.result()
    print("\nCreating voe_custom_dimension table in staging successfull!")
    time.sleep(5)


def downgrade():
    query_job = bqclient.query(query_string2_voc)
    query_job.result()
    print("\n Dropped voc_custom_dimension and backup as voc_custom_dimension_bk in stagging!")

    query_job = bqclient.query(query_string2_voe)
    query_job.result()
    print("\n Dropped voe_custom_dimension and backup as voe_custom_dimension_bk in staging!")

    print("\n Downgrade a21e23773801 process is successfull!")