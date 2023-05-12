"""hra - LEON-130 experience filter by leo company id

Revision ID: 4ce967d5b194
Revises: 29fda5af8940
Create Date: 2022-11-29 15:37:49.024695

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "4ce967d5b194"
down_revision = "29fda5af8940"
branch_labels = None
depends_on = None

import config
from typing import List
from google.cloud import bigquery

project = config.GCP_PROJECT_ID
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS

hra_summary_table_monthly_dataset = (
    f"{project}.{datamart_cs}.hra_summary_table_monthly_dataset"
)
hra_view_monthly_dataset = f"{project}.{datamart}.HRA_monthly_dataset"


def list_table_name() -> List[str]:
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    sql = f"""
        select table_name
        from `{project}.{datamart_cs}.INFORMATION_SCHEMA.TABLES`
        where table_name like 'hra_summary_table_experience_%';
    """
    query_job = bqclient.query(sql)
    rows = query_job.result()

    return [row["table_name"] for row in rows]


def upgrade():
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    upgrade_scripts = ""
    table_names = list_table_name()
    for table_name in table_names:
        upgrade_scripts += f"""
            ALTER TABLE `{project}.{datamart_cs}.{table_name}`
            ADD COLUMN IF NOT EXISTS experience_company_id INT64;
        """
        
    upgrade_scripts += f"""
        CREATE OR REPLACE VIEW `{project}.{datamart}.HRA_employee_experiences` AS 
        SELECT
            case_study_id,
            case_study_name,
            nlp_pack,
            date_from,
            date_to,
            experience_id,
            coresignal_employee_id AS employee_id,
            job_function_group,
            job_function,
            most_similar_keyword AS keyword,
            title,
            company_name,
            company_id,
            experience_company_id AS experience_leo_company_id,
            coresignal_company_id,
            coresignal_company_name,
            coresignal_company_url,
            duration,
            google_country,
            google_admin1,
            TO_JSON_STRING(google_address_components) AS google_address_components,
            previous_company_name,
            previous_coresignal_company_id,
            previous_experience_id,
            next_company_name,
            next_coresignal_company_id,
            next_experience_id,
            is_starter,
            is_leaver,
            is_target,
            is_cohort,
            
            -- Tenure 
            workforce_years,
            company_years,
            role_years,
            last_education_years
        FROM `{project}.{datamart_cs}.hra_summary_table_experience_*`;
    """
    query_job = bqclient.query(upgrade_scripts)
    query_job.result()

    print(f"<hra - LEON-130 experience filter by leo company id> successfully!")


def downgrade():
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    downgrade_scripts = ""
    table_names = list_table_name()
    for table_name in table_names:
        downgrade_scripts += f"""
            ALTER TABLE `{project}.{datamart_cs}.{table_name}`
            DROP COLUMN IF EXISTS experience_company_id;
        """
        
    downgrade_scripts += f"""
        CREATE OR REPLACE VIEW `{project}.{datamart}.HRA_employee_experiences` AS 
        SELECT
            case_study_id,
            case_study_name,
            nlp_pack,
            date_from,
            date_to,
            experience_id,
            coresignal_employee_id AS employee_id,
            job_function_group,
            job_function,
            most_similar_keyword AS keyword,
            title,
            company_name,
            company_id,
            coresignal_company_id,
            coresignal_company_name,
            coresignal_company_url,
            duration,
            google_country,
            google_admin1,
            TO_JSON_STRING(google_address_components) AS google_address_components,
            previous_company_name,
            previous_coresignal_company_id,
            previous_experience_id,
            next_company_name,
            next_coresignal_company_id,
            next_experience_id,
            is_starter,
            is_leaver,
            is_target,
            is_cohort,
            
            -- Tenure 
            workforce_years,
            company_years,
            role_years,
            last_education_years
        FROM `{project}.{datamart_cs}.hra_summary_table_experience_*`;
    """
    query_job = bqclient.query(downgrade_scripts)
    query_job.result()

    print(
        f"Rollback <hra - LEON-130 experience filter by leo company id> successfully!"
    )
