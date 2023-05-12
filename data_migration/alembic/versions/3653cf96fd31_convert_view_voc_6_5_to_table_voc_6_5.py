"""convert view VOC_6_5 to table VOC_6_5

Revision ID: 3653cf96fd31
Revises: e0465db5a413
Create Date: 2021-02-23 02:51:01.142890

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
revision = '3653cf96fd31'
down_revision = 'e0465db5a413'
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
    """convert view VOC_6_5 to table VOC_6_5"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade 3653cf96fd31 process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 3653cf96fd31 process is successfull!")
    
query_string1 = f"""
### CREATE TABLE 6_5 FROM datamart.summary_table:

CREATE OR REPLACE TABLE `{project}.{datamart}.VOC_6_5_competitor_table` 

PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
AS
WITH competitor_list AS (SELECT case_study_id,ARRAY_AGG(distinct company_name) as competitor 
FROM `{project}.{datamart}.summary_table`
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
competitor ,
run_id
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
count(distinct mentioned) as total_review_mention,
run_id
FROM review_mentioned
GROUP BY 
case_study_id, 
company_id,
source_id,
review_date,
run_id
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
    COUNT(distinct case when dimension is not null then mentioned else null end) as sum_mentioned,
    run_id
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
    competitor,
    run_id
    ;


### CREATE VIEW 6_5 BASED ON RUN_ID:

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_5_competitor` AS

WITH run_id_t as (
    SELECT
        *,
        row_number() over (
            partition by case_study_id
            order by
                created_at desc,
                case_study_id desc
        ) as rank
    FROM
        `{project}.{staging}.case_study_run_id`
)

SELECT * except(run_id) FROM `{project}.{datamart}.VOC_6_5_competitor_table`
WHERE
    run_id in (
        SELECT
            run_id
        FROM
            run_id_t
        WHERE
            rank = 1
    )
;



"""
query_string2 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_5_competitor` AS
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

###

DROP TABLE `{project}.{datamart}.VOC_6_5_competitor_table` ;

"""