"""create view VOE_8_4

Revision ID: 885cc95e5e46
Revises: c1017964eb7e
Create Date: 2021-04-15 06:31:05.261015

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
revision = '885cc95e5e46'
down_revision = 'c1017964eb7e'
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
    """create view VOE_8_4_roleseniorbyjobtype"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n create view VOE_8_4 in Datamart successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 885cc95e5e46 process is successfull!")
    
    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_8_4_roleseniorbyjobtype` AS

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
        WHEN job_type is NULL or job_type = '' THEN 'undefined'
        ELSE job_type 
        END AS job_type,
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
        job_type,
        role_seniority
        ;


"""
query_string2 = f"""

DROP VIEW `{project}.{datamart}.VOE_8_4_roleseniorbyjobtype` ;
"""
