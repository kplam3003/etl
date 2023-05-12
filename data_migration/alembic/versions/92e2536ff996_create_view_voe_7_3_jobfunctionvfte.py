"""create_view_voe_7_3_jobfunctionvFTE

Revision ID: 92e2536ff996
Revises: 6b163c54298f
Create Date: 2021-04-16 07:51:58.844294

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
revision = '92e2536ff996'
down_revision = '6b163c54298f'
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
    """create view VOE_7_3_jobfunctionvFTE"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Create view VOE_7_3_jobfunctionvFTE in Datamart successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 6b163c54298f process is successfull!")
    
    
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_7_3_jobfunctionvFTE` AS
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
        DROP VIEW `{project}.{datamart}.VOE_7_3_jobfunctionvFTE` ;

        """

