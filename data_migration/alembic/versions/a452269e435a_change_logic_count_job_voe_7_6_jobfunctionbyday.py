"""change_logic_count_job_voe_7_6_jobfunctionbyday

Revision ID: a452269e435a
Revises: e5ca7dc2256a
Create Date: 2021-12-01 14:34:07.622545

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
revision = 'a452269e435a'
down_revision = 'e5ca7dc2256a'
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
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade logic view VOE_7_6_jobfunctionbyday successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade logic view VOE_7_6_jobfunctionbyday successfull!")

query_string1 = f"""
CREATE
OR REPLACE VIEW `{project}.{datamart}.VOE_7_6_jobfunctionbyday` AS
WITH distinct_job_id AS (
    SELECT
        company_id,
        job_id,
        MIN(DATE(posted_date)) as posted_date
    FROM `{project}.{datamart}.voe_job_summary_table`
    GROUP BY company_id, job_id
),
single AS (
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
	    CASE 
		    WHEN job_function is NULL or job_function = '' THEN 'undefined'
		    ELSE job_function 
	    END AS job_function,
	    d.job_id,
	    d.posted_date,
	    DATE_DIFF(current_date(), date(d.posted_date), DAY) as listing_time
	FROM `{project}.{datamart}.voe_job_summary_table` a
	LEFT JOIN distinct_job_id d ON a.company_id = d.company_id AND a.job_id = d.job_id
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
    posted_date;
"""


query_string2 = f"""
CREATE
OR REPLACE VIEW `{project}.{datamart}.VOE_7_6_jobfunctionbyday` AS
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
    posted_date;    
"""
