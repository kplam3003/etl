"""create voe_7_6_jobfunctionbyday

Revision ID: 5c1169c6fab6
Revises: 885cc95e5e46
Create Date: 2021-04-15 08:07:09.799515

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
revision = '5c1169c6fab6'
down_revision = '3d56cb110446'
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
    """create view voe_7_6_jobfunctionbyday"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Create view voe_7_6_jobfunctionbyday in Datamart successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 5c1169c6fab6 process is successfull!")
    
    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_7_6_jobfunctionbyday` AS

WITH single as (SELECT 
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
    CASE 
    WHEN job_function is NULL or job_function = '' THEN 'undefined'
    ELSE job_function 
    END AS job_function,
    job_id,
    date(posted_date) as posted_date,
    DATE_DIFF(current_date(), date(posted_date), DAY) as listing_time
FROM `{project}.{datamart}.voe_job_summary_table` a
)

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
    job_function,
    posted_date,
    min(listing_time) as newest_days,
    max(listing_time) as oldest_days,
    sum(listing_time) as sum_listing_days,
    count(job_id) as job_quantity
FROM single

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
    job_function,
    posted_date

    ;

"""
query_string2 = f"""

 DROP VIEW `{project}.{datamart}.VOE_7_6_jobfunctionbyday` ;

"""