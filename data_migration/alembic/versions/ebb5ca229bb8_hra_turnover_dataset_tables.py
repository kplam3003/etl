"""HRA - Turnover Dataset tables

Revision ID: ebb5ca229bb8
Revises: 9175039a32a3
Create Date: 2023-01-31 15:18:35.624868

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "ebb5ca229bb8"
down_revision = "9175039a32a3"
branch_labels = None
depends_on = None

from google.cloud import bigquery
from typing import List
import config

project = config.GCP_PROJECT_ID
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS


def _get_existing_casestudy_ids() -> List[int]:
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_cs_ids = f"""
        SELECT DISTINCT case_study_id 
        FROM `{project}.{datamart_cs}.hra_summary_table_company_*`
    """
    query_results = bqclient.query(query_cs_ids).result()

    case_study_ids: List[int] = []
    for row in query_results:
        case_study_ids.append(row.case_study_id)

    return case_study_ids


def upgrade():
    casestudy_ids = _get_existing_casestudy_ids()
    upgrade_script = f"""
        CREATE OR REPLACE TABLE `{project}.{dwh}.hra_company_employees_bucket`
        (
            case_study_id INT64,
            company_id INT64,
            time_key DATE,
            employees ARRAY<INT64>, -- IDs of employee has experience(s) covered `time_key`
            experiences ARRAY<INT64> -- IDs of experience covered `time_key`
        )
        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 1));
    """
    for cs_id in casestudy_ids:
        upgrade_script += f"""
            CREATE OR REPLACE TABLE `{project}.{datamart_cs}.hra_summary_table_turnover_{cs_id}`
            (
                case_study_id INT64,
                case_study_name STRING,
                company_id INT64,
                company_name STRING,
                time_key DATE,
                category_type STRING,
                category_value STRING,
                turnover_rate FLOAT64,
                num_employees_start INT64,
                num_employees_end INT64,
                num_starters INT64,
                num_leavers INT64
            );
        """

    upgrade_script += f"""
        CREATE OR REPLACE VIEW `{project}.{datamart}.HRA_turnover_dataset` AS
        SELECT case_study_id,
            case_study_name,
            company_id,
            company_name,
            time_key,
            category_type,
            category_value,
            turnover_rate,
            num_employees_start,
            num_employees_end,
            num_starters,
            num_leavers
        FROM `{project}.{datamart_cs}.hra_summary_table_turnover_*`;
    """

    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(upgrade_script)
    query_job.result()
    print(f"<HRA - Turnover Dataset tables> successfully!")


def downgrade():
    casestudy_ids = _get_existing_casestudy_ids()
    downgrade_script = f"""
        DROP TABLE IF EXISTS `{project}.{dwh}.hra_company_employees_bucket`;
        DROP VIEW IF EXISTS `{project}.{datamart}.HRA_turnover_dataset`;
    """
    for cs_id in casestudy_ids:
        downgrade_script += f"""
            DROP TABLE IF EXISTS `{project}.{datamart_cs}.hra_summary_table_turnover_{cs_id}`;
        """

    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(downgrade_script)
    query_job.result()
    print(f"Rollback <HRA - Turnover Dataset tables> successfully!")
