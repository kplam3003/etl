"""create_view_VOE_7_1_jobfunctionquant

Revision ID: 5163691cdd7c
Revises: 0c5d144774a0
Create Date: 2021-04-15 04:02:29.265030

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
revision = '5163691cdd7c'
down_revision = '0c5d144774a0'
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
    """create view VOE_7_1_jobfunctionquant"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Create view VOE_7_1_jobfunctionquant in Datamart successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 0c5d144774a0 process is successfull!")
    
    
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_7_1_jobfunctionquant` AS

SELECT 
    case_study_id, 
    case_study_name,
    nlp_pack,
    nlp_type, 
    dimension_config_id, 
    dimension_config_name,
    source_name,
    source_id,
    company_name,
    company_id,
    DATE(posted_date) as posted_date,
    CASE 
    WHEN job_function is NULL or job_function = '' THEN 'undefined'
    ELSE job_function 
    END AS job_function,
    COUNT(DISTINCT job_id) as job_quantity,
    MAX(fte) as fte
FROM `{project}.{datamart}.voe_job_summary_table` a
GROUP BY
    case_study_id, 
    case_study_name,
    nlp_pack,
    nlp_type, 
    dimension_config_id, 
    dimension_config_name,
    source_name,
    source_id,
    company_name,
    company_id,
    DATE(posted_date),
    job_function;
        """

query_string2 = f"""
        DROP VIEW `{project}.{datamart}.VOE_7_1_jobfunctionquant` ;

        """

