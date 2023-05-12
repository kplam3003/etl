"""create review stats tables

Revision ID: 16ffeb0ac48f
Revises: f4bf87765e65
Create Date: 2022-06-08 16:34:13.682258

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
revision = '16ffeb0ac48f'
down_revision = 'f4bf87765e65'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()


# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
staging = config.STAGING

query_string1 = f"""
        CREATE OR REPLACE table `{project}.{staging}.voc_review_stats`
        (
            `created_at`        timestamp,
            `case_study_id`     int64,
            `company_name`      string,
            `company_id`	    int64,
            `source_name`	    string,
            `source_id`	        int64,
            `batch_id`	        int64,
            `total_reviews`	    int64,    
            `total_ratings`	    int64,    
            `average_rating`    float64
        )
        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,4));

        CREATE OR REPLACE table `{project}.{staging}.voe_review_stats`
        (
            `created_at`        timestamp,
            `case_study_id`     int64,
            `company_name`      string,
            `company_id`	    int64,
            `source_name`	    string,
            `source_id`	        int64,
            `batch_id`	        int64,
            `total_reviews`	    int64,    
            `total_ratings`	    int64,    
            `average_rating`    float64
        )
        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,4));
    """

query_string2 = f"""
        DROP TABLE `{project}.{staging}.voc_review_stats` ;
        DROP TABLE `{project}.{staging}.voe_review_stats` ;
"""
def upgrade():
    ##########  CREATE voc_review_stats TABLE and voe_review_stats TABLE IN staging #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voc_review_stats, voe_review_stats tables in Staging successfull!")


def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 16ffeb0ac48f process is successfull!")
