"""create voe_8_1_roleseniorquant

Revision ID: 8486680f5f37
Revises: 5c1169c6fab6
Create Date: 2021-04-16 03:46:52.957830

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
revision = '8486680f5f37'
down_revision = '5c1169c6fab6'
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
    """create view voe_8_1_roleseniorquant"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Create view voe_8_1_roleseniorquant in Datamart successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 8486680f5f37 process is successfull!")
    
    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_8_1_roleseniorquant` 
AS
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
    date(posted_date) as posted_date,
    CASE 
    WHEN role_seniority is NULL or role_seniority = '' THEN 'undefined'
    ELSE role_seniority 
    END AS role_seniority,    
    COUNT(distinct job_id) as job_quantity,
    max(fte) as fte

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
    posted_date,
    role_seniority

    ;


"""
query_string2 = f"""

DROP VIEW `{project}.{datamart}.VOE_8_1_roleseniorquant`;

"""