"""hra - add is_target, is_cohort, workforce_years, company_years, role_years, and last_education_years fields to experience

Revision ID: cd43d8d89102
Revises: 9edbd7c29fca
Create Date: 2022-11-21 13:21:59.537374

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'cd43d8d89102'
down_revision = '9edbd7c29fca'
branch_labels = None
depends_on = None

import config
from typing import List
from google.cloud import bigquery

project = config.GCP_PROJECT_ID
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS

hra_view_employee_experiences = f"{project}.{datamart}.HRA_employee_experiences"


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
    # STAGING + DWH: Add new columns and tables
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    upgrade_scripts = f"""
        ALTER TABLE `{project}.{staging}.coresignal_employees_experiences`
        ADD COLUMN IF NOT EXISTS is_target BOOLEAN,
        ADD COLUMN IF NOT EXISTS is_cohort BOOLEAN;

        CREATE OR REPLACE table `{project}.{dwh}.hra_employees_tenure` (
            `case_study_id` int64,
            `company_datasource_id` int64,
            `employee_id` int64,
            `experience_id` int64,
            `workforce_years` float64,
            `company_years` float64,
            `role_years` float64,
            `last_education_years` float64
        )
        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 4));

        CREATE OR REPLACE table `{project}.{dwh}.hra_employees_tenure_monthly` (
            `case_study_id` int64,
            `company_datasource_id` int64,
            `employee_id` int64,
            `experience_id` int64,
            `reference_date` date,
            `workforce_years` float64,
            `company_years` float64,
            `role_years` float64,
            `last_education_years` float64
        )
        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 4));

    """
    query_job = bqclient.query(upgrade_scripts)
    query_job.result()

    # DATAMART_CS: Add columns to existing tables
    sql = ""
    table_names = list_table_name()
    for table_name in table_names:
        sql += f"""
            ALTER TABLE `{project}.{datamart_cs}.{table_name}`
            ADD COLUMN IF NOT EXISTS is_target BOOLEAN,
            ADD COLUMN IF NOT EXISTS is_cohort BOOLEAN,
            ADD COLUMN IF NOT EXISTS workforce_years FLOAT64,
            ADD COLUMN IF NOT EXISTS company_years FLOAT64,
            ADD COLUMN IF NOT EXISTS role_years FLOAT64,
            ADD COLUMN IF NOT EXISTS last_education_years FLOAT64;
        """

    sql += f"""
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
    query_job = bqclient.query(sql)
    query_job.result()
    

    print(f"<hra - add is_target field to experience> successfully!")


def downgrade():
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    downgrade_scripts = f"""
        ALTER TABLE `{project}.{staging}.coresignal_employees_experiences`
        DROP COLUMN IF EXISTS is_target,
        DROP COLUMN IF EXISTS is_cohort;

        DROP TABLE IF EXISTS `{project}.{dwh}.hra_employees_tenure`;
        DROP TABLE IF EXISTS `{project}.{dwh}.hra_employees_tenure_monthly`;
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
            is_leaver
        FROM `{project}.{datamart_cs}.hra_summary_table_experience_*`;  
    """
    query_job = bqclient.query(downgrade_scripts)
    query_job.result()

    sql = ""
    table_names = list_table_name()
    for table_name in table_names:
        sql += f"""
            ALTER TABLE `{project}.{datamart_cs}.{table_name}`
            DROP COLUMN IF EXISTS is_target,
            DROP COLUMN IF EXISTS is_cohort,
            DROP COLUMN IF EXISTS workforce_years,
            DROP COLUMN IF EXISTS company_years,
            DROP COLUMN IF EXISTS role_years,
            DROP COLUMN IF EXISTS last_education_years;
        """
    query_job = bqclient.query(sql)
    query_job.result()

    print(f"Rollback <hra - add is_target field to experience> successfully!")
