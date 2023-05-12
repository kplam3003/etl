"""change_sources_of_voc_6_5

Revision ID: 97adf2897954
Revises: a96b164e05f1
Create Date: 2021-02-01 06:50:05.150021

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
revision = '97adf2897954'
down_revision = 'a96b164e05f1'
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
    """change_sources_of_voc_6_5"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")
    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_5_competitor` 
AS
WITH competitor_list AS (SELECT case_study_id,ARRAY_AGG(distinct company_name) as competitor FROM `{project}.{datamart}.summary_table`
GROUP BY  
case_study_id
),
cte_competitor as (SELECT s.*,
c.competitor
FROM `{project}.{datamart}.summary_table` as s
LEFT JOIN competitor_list as c
ON s.case_study_id=c.case_study_id),

cte_review as (SELECT DISTINCT
case_study_id, 
case_study_name,
dimension_config_name,
dimension_config_id, 
company_name, 
source_name,
company_id, 
source_id,
nlp_type,
nlp_pack,
dimension,
review_date, 
review_id, 
review, 
competitor 
FROM cte_competitor, UNNEST(competitor) AS competitor 
),
review_mentioned as (SELECT *,
CASE WHEN lower(review)  LIKE '%'||lower(competitor)||'%' and competitor!=company_name  THEN review_id
ELSE NULL END AS mentioned
FROM cte_review
),
total_mention as ( 
SELECT 
case_study_id, 
company_id,
source_id,
review_date,
count(distinct mentioned) as total_review_mention
FROM review_mentioned
GROUP BY 
case_study_id, 
company_id,
source_id,
review_date
)
SELECT
    case_study_id, 
    max(case_study_name) case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id, 
    max(source_name) source_name,
    max(company_name) company_name,
    company_id, 
    source_id,
    nlp_type,
    nlp_pack,
    review_date as daily_date,
    competitor,
    count(distinct review_id) as records,
    count(distinct case when dimension is not null  then review_id else null end) as processed_review_count,
    (SELECT total_review_mention FROM total_mention 
    WHERE case_study_id=a.case_study_id
    AND company_id=a.company_id
    AND source_id=a.source_id
    AND review_date=a.review_date) as processed_review_mention,
    COUNT(distinct case when dimension is not null then mentioned else null end) as sum_mentioned
FROM review_mentioned as a
WHERE dimension_config_name is not null
GROUP BY
    case_study_id,
    dimension_config_id,
    company_id, 
    source_id,
    nlp_type,
    nlp_pack,
    review_date,
    competitor;

"""
    
query_string2 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_5_competitor` 
AS
WITH competitor_list AS (SELECT case_study_id,ARRAY_AGG(distinct company_name) as competitor FROM `{project}.{dwh}.summary_table`
GROUP BY  
case_study_id
),
cte_competitor as (SELECT s.*,
c.competitor
FROM `{project}.{dwh}.summary_table` as s
LEFT JOIN competitor_list as c
ON s.case_study_id=c.case_study_id),

cte_review as (SELECT DISTINCT
case_study_id, 
case_study_name,
dimension_config_name,
dimension_config_id, 
company_name, 
source_name,
company_id, 
source_id,
nlp_type,
nlp_pack,
dimension,
review_date, 
review_id, 
review, 
competitor 
FROM cte_competitor, UNNEST(competitor) AS competitor 
),
review_mentioned as (SELECT *,
CASE WHEN lower(review)  LIKE '%'||lower(competitor)||'%' and competitor!=company_name  THEN review_id
ELSE NULL END AS mentioned
FROM cte_review
),
total_mention as ( 
SELECT 
case_study_id, 
company_id,
source_id,
review_date,
count(distinct mentioned) as total_review_mention
FROM review_mentioned
GROUP BY 
case_study_id, 
company_id,
source_id,
review_date
)
SELECT
    case_study_id, 
    max(case_study_name) case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id, 
    max(source_name) source_name,
    max(company_name) company_name,
    company_id, 
    source_id,
    nlp_type,
    nlp_pack,
    review_date as daily_date,
    competitor,
    count(distinct review_id) as records,
    count(distinct case when dimension is not null  then review_id else null end) as processed_review_count,
    (SELECT total_review_mention FROM total_mention 
    WHERE case_study_id=a.case_study_id
    AND company_id=a.company_id
    AND source_id=a.source_id
    AND review_date=a.review_date) as processed_review_mention,
    COUNT(distinct case when dimension is not null then mentioned else null end) as sum_mentioned
FROM review_mentioned as a
WHERE dimension_config_name is not null
GROUP BY
    case_study_id,
    dimension_config_id,
    company_id, 
    source_id,
    nlp_type,
    nlp_pack,
    review_date,
    competitor;


"""
