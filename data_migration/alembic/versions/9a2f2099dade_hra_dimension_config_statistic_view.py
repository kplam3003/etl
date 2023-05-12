"""HRA Dimension config statistic view

Revision ID: 9a2f2099dade
Revises: e957fe9a4d01
Create Date: 2022-10-17 11:46:10.827921

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9a2f2099dade'
down_revision = 'e957fe9a4d01'
branch_labels = None
depends_on = None


from google.cloud import bigquery

import config

project = config.GCP_PROJECT_ID
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS

view_id = f"{project}.{datamart}.hra_dimension_config_statistic"
summary_table_wildcard = f"{project}.{datamart_cs}.hra_summary_table_experience_*"


def upgrade():
    query = f"""
CREATE OR REPLACE VIEW `{view_id}` AS
WITH experience_summary_table as (
  SELECT
    created_at,
    case_study_id,
    case_study_name,
    source_id,
    source_name,
    company_id,
    company_name,
    nlp_pack,
    nlp_type,
    dimension_config_id,
    dimension_config_name,
    experience_id,
    job_function_group AS dimension,
    modified_job_function_group AS modified_dimension,
    job_function AS label,
    modified_job_function AS modified_label,
    is_used,
    run_id
FROM
    `{summary_table_wildcard}`
WHERE is_used = True
)
SELECT
    case_study_id,
    case_study_name,
    source_id,
    source_name,
    company_id,
    company_name,
    nlp_pack,
    nlp_type,
    dimension_config_id,
    dimension_config_name,
    dimension,
    modified_dimension,
    label,
    modified_label,
    is_used as used_for_analysis,
    COUNT(
        DISTINCT CASE
            WHEN dimension IS NULL THEN NULL
            ELSE experience_id
        END
    ) AS customer_review_processed,
    (
      SELECT COUNT(experience_id) 
      FROM experience_summary_table 
      WHERE case_study_id = a.case_study_id
        AND dimension_config_id = a.dimension_config_id
    ) AS dim_customer_review_processed,
FROM experience_summary_table a
GROUP BY
    case_study_id,
    case_study_name,
    source_id,
    source_name,
    company_id,
    company_name,
    nlp_pack,
    nlp_type,
    dimension_config_id,
    dimension_config_name,
    dimension,
    modified_dimension,
    label,
    modified_label,
    is_used
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    # voc
    query_job = bqclient.query(query)
    query_job.result()
    print(f"Created hra_dimension_config_statistic view in {datamart} successfully!")

def downgrade():
    query = f"""
    DROP VIEW IF EXISTS `{view_id}`
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    # voc
    query_job = bqclient.query(query)
    query_job.result()
    print(f"Dropped hra_dimension_config_statistic view in {datamart} successfully!")
