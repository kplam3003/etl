"""add abs_relevance and rel_relevance to table casestudy_company_source

Revision ID: 5612356a5eeb
Revises: dffcb53e3baa
Create Date: 2021-02-18 03:51:40.780145

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
revision = '5612356a5eeb'
down_revision = 'bad6b2d7b53b'
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
    """add abs_relevance and rel_relevance to table casestudy_company_source"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade 5612356a5eeb process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 5612356a5eeb process is successfull!")
    
query_string1 = f"""
CREATE
OR REPLACE TABLE `{project}.{dwh}.casestudy_company_source_bk` as
SELECT
    *
FROM
    `{project}.{dwh}.casestudy_company_source`;

DROP TABLE `{project}.{dwh}.casestudy_company_source`;

CREATE
OR REPLACE TABLE `{project}.{dwh}.casestudy_company_source` 

PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 5)) AS
SELECT
    *,
    CAST(0 as float64 ) abs_relevance_inf,
    CAST(0 as float64) rel_relevance_inf
FROM
    `{project}.{dwh}.casestudy_company_source_bk`;

DROP TABLE `{project}.{dwh}.casestudy_company_source_bk`;
"""


query_string2 = f"""

CREATE
OR REPLACE TABLE `{project}.{dwh}.casestudy_company_source_bk` as
SELECT
    *
FROM
    `{project}.{dwh}.casestudy_company_source`;

DROP TABLE `{project}.{dwh}.casestudy_company_source`;

CREATE
OR REPLACE TABLE `{project}.{dwh}.casestudy_company_source` 

PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 5)) AS
SELECT
    * except(abs_relevance_inf, rel_relevance_inf)
FROM
    `{project}.{dwh}.casestudy_company_source_bk`;

DROP TABLE `{project}.{dwh}.casestudy_company_source_bk`;

"""
