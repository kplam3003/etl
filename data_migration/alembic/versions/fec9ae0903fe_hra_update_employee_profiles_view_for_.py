"""HRA - Update employee profiles view for exporting csv - Bug fix

Revision ID: fec9ae0903fe
Revises: efd125c2eb7f
Create Date: 2023-02-24 11:42:01.145771

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fec9ae0903fe'
down_revision = 'efd125c2eb7f'
branch_labels = None
depends_on = None


from google.cloud import bigquery
from typing import List
import config

project = config.GCP_PROJECT_ID
staging = config.STAGING
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS


def upgrade():
    upgrade_script = f"""
        CREATE OR REPLACE VIEW `{project}.{datamart}.HRA_employee_profiles` AS
        SELECT
            case_study_id,
            case_study_name,
            nlp_pack,
            coresignal_employee_id AS employee_id,
            source_name,
            company_name,
            name,
            title,
            url,
            industry,
            created,
            last_updated,
            google_country,
            google_admin1,
            TO_JSON_STRING(google_address_components) AS google_address_components,
            country,
            connection_count,
            highest_education_level_code,
            highest_education_level
        FROM `{project}.{datamart_cs}.hra_summary_table_employees_*`;
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(upgrade_script)
    query_job.result()
    print(f"<HRA - Update employee profiles view for exporting csv> successfully!")


def downgrade():
    downgrade_script = f"""
        CREATE OR REPLACE VIEW `{project}.{datamart}.HRA_employee_profiles` AS
        SELECT
            case_study_id,
            case_study_name,
            nlp_pack,
            coresignal_employee_id AS employee_id,
            source_name,
            company_name,
            name,
            title,
            url,
            industry,
            created,
            last_updated,
            google_country,
            google_admin1,
            TO_JSON_STRING(google_address_components) AS google_address_components,
            country,
            connection_count
        FROM `{project}.{datamart_cs}.hra_summary_table_employees_*`;
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(downgrade_script)
    query_job.result()
    print(f"Rollback <HRA - Update employee profiles view for exporting csv> successfully!")
