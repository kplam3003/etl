"""modify voc review_raw_data datamart view to include review_country

Revision ID: 29120ed786f8
Revises: b172ebe767e1
Create Date: 2021-10-21 18:23:58.105265

"""
from alembic import op
import sqlalchemy as sa
from google.cloud import bigquery

import config

# revision identifiers, used by Alembic.
revision = '29120ed786f8'
down_revision = 'b172ebe767e1'
branch_labels = None
depends_on = None


bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS


upgrade_sql = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.review_raw_data` AS

WITH BATCH_LIST as (
    SELECT
        batch_id
    FROM
        `{project}.{staging}.batch_status`
    WHERE
        status = 'Active'       
),
parent_review_mapping as (
    SELECT DISTINCT 
        case_study_id,
        review_id,
        parent_review_id,
        technical_type
    FROM  `{project}.{staging}.parent_review_mapping`
    WHERE
        batch_id IN (
            SELECT
                batch_id FROM BATCH_LIST
        )
),
review_country_mapping AS (
    SELECT DISTINCT
        case_study_id,
        review_id,
        review_country
    FROM `{project}.{staging}.review_country_mapping`
    WHERE
        batch_id IN (
            SELECT batch_id FROM BATCH_LIST
        )
)
SELECT DISTINCT
    a.case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    a.review_id,
    review as orig_review,
    language,
    trans_review,
    rating,
    review_date,
    p.parent_review_id,
    p.technical_type,
    CASE
        WHEN r.review_country IS NULL THEN 'blank'
        WHEN r.review_country = 'Unknown' THEN 'blank'
        ELSE r.review_country
    END AS review_country
FROM `{project}.{datamart}.summary_table` a
LEFT JOIN parent_review_mapping p
    ON a.case_study_id = p.case_study_id
    AND a.review_id = p.review_id
LEFT JOIN review_country_mapping r
    ON a.case_study_id = r.case_study_id
    AND a.review_id = r.review_id
WHERE dimension_config_name is not null
;
"""


def upgrade():
    query_job = bqclient.query(upgrade_sql)
    query_job.result()
  
    print("\n Update view review_raw_data on Datamart to include review_country successfully!")


downgrade_sql = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.review_raw_data` AS

WITH BATCH_LIST as (
    SELECT
        batch_id
    FROM
        `{project}.{staging}.batch_status`
    WHERE
        status = 'Active'
        
) ,
parent_review as (
    SELECT DISTINCT 
        case_study_id,
        review_id,
        parent_review_id,
        technical_type
    FROM `{project}.{staging}.parent_review_mapping`
    WHERE
        batch_id IN (
            SELECT
                batch_id FROM BATCH_LIST
        )
)
SELECT DISTINCT
    a.case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    a.review_id,
    review as orig_review,
    language,
    trans_review,
    rating,
    review_date,
    p.parent_review_id,
    p.technical_type
FROM `{project}.{datamart}.summary_table` a
LEFT JOIN parent_review p
    ON a.case_study_id = p.case_study_id
    AND a.review_id = p.review_id
WHERE dimension_config_name is not null
;
"""


def downgrade():
    query_job = bqclient.query(downgrade_sql)
    query_job.result()
  
    print("\n Downgrade to revision b172ebe767e1 successfully")
