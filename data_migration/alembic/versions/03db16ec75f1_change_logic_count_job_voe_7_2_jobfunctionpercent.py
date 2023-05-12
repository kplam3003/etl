"""change_logic_count_job_voe_7_2_jobfunctionpercent

Revision ID: 03db16ec75f1
Revises: 0894f9c2e233
Create Date: 2021-12-01 11:16:23.583928

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
revision = '03db16ec75f1'
down_revision = '0894f9c2e233'
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
# change chart 7_2:
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade logic view VOE_7_2_jobfunctionpercent successfull!")


def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade logic view VOE_7_2_jobfunctionpercent successfull!")

query_string1 = f"""
CREATE
OR REPLACE VIEW `{project}.{datamart}.VOE_7_2_jobfunctionpercent` AS
WITH distinct_job_id AS (
    SELECT
        company_id,
        job_id,
        MIN(DATE(posted_date)) as posted_date
    FROM `{project}.{datamart}.voe_job_summary_table`
    GROUP BY company_id, job_id

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
        a.company_id,
        d.posted_date,
        CASE 
            WHEN job_function is NULL or job_function = '' THEN 'undefined'
            ELSE job_function 
        END AS job_function,
        COUNT(DISTINCT d.job_id) as job_quantity,
        MAX(fte) as fte
    FROM `{project}.{datamart}.voe_job_summary_table` a
    LEFT JOIN distinct_job_id d ON a.company_id = d.company_id AND a.job_id = d.job_id
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
        job_function;
"""

query_string2 = f"""
CREATE
OR REPLACE VIEW `{project}.{datamart}.VOE_7_2_jobfunctionpercent` AS
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