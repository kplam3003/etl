"""update_view_name_voc_12_2

Revision ID: 567776eda0c6
Revises: abc963bd88c6
Create Date: 2021-08-30 03:30:09.457577

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
revision = '567776eda0c6'
down_revision = 'abc963bd88c6'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()

# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT, config.DATAMART_CS]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS

def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Drop view VOC_12_2_sscasualpolarity successfully!")
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Update view namme VOC_12_2_sscausalpolarity successfully!")
	
def downgrade():
    query_job =bqclient.query(query_string3)
    query_job .result()
    print("\n Drop view VOC_12_2_sscausalpolarity successfully!!")
    query_job =bqclient.query(query_string4)
    query_job .result()
    print("\n Downgrade abc963bd88c6 process is successfully!")

query_string1 = f"""
DROP VIEW  `{project}.{datamart}.VOC_12_2_sscasualpolarity`;
"""
query_string2 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_12_2_sscausalpolarity` AS 
SELECT
        case_study_id,
        MAX(case_study_name) case_study_name,
        MAX(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        MAX(company_name) company_name,
        review_date,
        polarity,
        COUNT( DISTINCT review) as review_count
    FROM
        `{project}.{datamart}.summary_table`
    WHERE
        dimension_config_name is not null 
        AND dimension is not NULL
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        review_date,
        polarity;
"""

query_string3 = f"""
DROP VIEW  `{project}.{datamart}.VOC_12_2_sscausalpolarity`;
"""
query_string4 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_12_2_sscasualpolarity` AS 
SELECT
        case_study_id,
        MAX(case_study_name) case_study_name,
        MAX(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        MAX(company_name) company_name,
        review_date,
        polarity,
        COUNT( DISTINCT review) as review_count
    FROM
        `{project}.{datamart}.summary_table`
    WHERE
        dimension_config_name is not null 
        AND dimension is not NULL
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        review_date,
        polarity;
"""
