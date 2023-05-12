"""add columns dimension_type, is_used into table dimension_default

Revision ID: 1cad18f71048
Revises: e46cf7abdd58
Create Date: 2021-01-27 04:36:55.670822

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
revision = '1cad18f71048'
down_revision = 'e46cf7abdd58'
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

def upgrade():
    """add two new columns and convert Dimension_default to partition table based on dimension_config_id"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")

query_string1 = f"""
CREATE
OR REPLACE TABLE `{project}.{dwh}.dimension_default_bk` as
select
    *
from
    `{project}.{dwh}.dimension_default`;

DROP TABLE `{project}.{dwh}.dimension_default`;

CREATE
OR REPLACE TABLE `{project}.{dwh}.dimension_default` PARTITION BY RANGE_BUCKET(dimension_config_id, GENERATE_ARRAY(1, 4000, 10)) AS
SELECT
    *,
    CAST(null as string) dimension_type,
    CAST(null as bool) is_used
FROM
    `{project}.{dwh}.dimension_default_bk`;

DROP TABLE `{project}.{dwh}.dimension_default_bk`;
    """

query_string2 = f"""
CREATE
OR REPLACE TABLE `{project}.{dwh}.dimension_default_bk` as
select
    *
from
    `{project}.{dwh}.dimension_default`;

DROP TABLE `{project}.{dwh}.dimension_default`;

CREATE
OR REPLACE TABLE `{project}.{dwh}.dimension_default` PARTITION BY RANGE_BUCKET(dimension_config_id, GENERATE_ARRAY(1, 4000, 10)) AS
SELECT
    *
    except (dimension_type, is_used)
FROM
    `{project}.{dwh}.dimension_default_bk`;
    
DROP TABLE `{project}.{dwh}.dimension_default_bk`;


"""