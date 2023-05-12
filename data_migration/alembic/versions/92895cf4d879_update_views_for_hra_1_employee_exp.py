"""Update views for HRA_1 and Employee Experiences view

Revision ID: 92895cf4d879
Revises: 5c52bc7347c6
Create Date: 2022-10-20 14:48:31.572712

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '92895cf4d879'
down_revision = '5c52bc7347c6'
branch_labels = None
depends_on = None


from google.cloud import bigquery

import config

# Get Bigquery Database name
project = config.GCP_PROJECT_ID
dwh = config.DWH
datamart_cs = config.DATAMART_CS
datamart = config.DATAMART

hra_1_view_id = f"{project}.{datamart}.HRA_1_profilestats"
company_profiles_view_id = f"{project}.{datamart}.HRA_company_profiles"
employee_profiles_view_id = f"{project}.{datamart}.HRA_employee_profiles"
employee_experiences_view_id = f"{project}.{datamart}.HRA_employee_experiences"

hra_summary_table_company_prefix = f"{project}.{datamart_cs}.hra_summary_table_company"
hra_summary_table_employees_prefix = f"{project}.{datamart_cs}.hra_summary_table_employees"
hra_summary_table_experience_prefix = f"{project}.{datamart_cs}.hra_summary_table_experience"
hra_summary_table_education_prefix = f"{project}.{datamart_cs}.hra_summary_table_education"



def upgrade():
    query = f"""
        -- HRA_1_profilestats
        CREATE OR REPLACE VIEW {hra_1_view_id} AS
        WITH employee_summary_table AS (
            SELECT *
            FROM `{hra_summary_table_employees_prefix}_*`
        ),
        experience_summary_table AS (
            SELECT *
            FROM `{hra_summary_table_experience_prefix}_*`
        ),
        education_summary_table AS (
            SELECT *
            FROM `{hra_summary_table_education_prefix}_*`
        )
        SELECT
            empl.case_study_id,
            empl.case_study_name,
            empl.source_name,
            empl.company_name,
            empl.nlp_type,
            empl.nlp_pack,
            empl.dimension_config_name,
            empl.coresignal_employee_id,
            expr.experience_id,
            expr.date_from AS experience_date_from,
            expr.date_to AS experience_date_to,
            edu.education_id,
            edu.date_from AS education_date_from,
            edu.date_to AS education_date_to
        FROM employee_summary_table empl
        LEFT JOIN experience_summary_table expr
        ON empl.company_datasource_id = expr.company_datasource_id
            AND empl.coresignal_employee_id = expr.coresignal_employee_id
        LEFT JOIN education_summary_table edu
        ON empl.company_datasource_id = edu.company_datasource_id
            AND empl.coresignal_employee_id = edu.coresignal_employee_id;
        
        -- Job experience
        CREATE OR REPLACE VIEW {employee_experiences_view_id} AS
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
            google_location,
            previous_company_name,
            previous_coresignal_company_id,
            previous_experience_id,
            next_company_name,
            next_coresignal_company_id,
            next_experience_id,
            is_starter,
            is_leaver
        FROM `{hra_summary_table_experience_prefix}_*`;
    
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(query)
    query_job.result()
    print(f"Modified {hra_1_view_id} and {employee_experiences_view_id}")

def downgrade():
    query = f"""
        -- HRA_1_profilestats
        CREATE OR REPLACE VIEW {hra_1_view_id} AS
        WITH employee_summary_table AS (
            SELECT *
            FROM `{hra_summary_table_employees_prefix}_*`
        ),
        experience_summary_table AS (
            SELECT *
            FROM `{hra_summary_table_experience_prefix}_*`
        )
        SELECT
            empl.case_study_id,
            empl.case_study_name,
            empl.source_name,
            empl.company_name,
            empl.nlp_type,
            empl.nlp_pack,
            empl.dimension_config_name,
            empl.num_education_records AS education_processed,
            empl.coresignal_employee_id,
            expr.experience_id,
            expr.date_from,
            expr.date_to
        FROM employee_summary_table empl
        LEFT JOIN experience_summary_table expr
        ON empl.company_datasource_id = expr.company_datasource_id
            AND empl.coresignal_employee_id = expr.coresignal_employee_id;
        
        -- Job experience
        CREATE OR REPLACE VIEW {employee_experiences_view_id} AS
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
            duration,
            google_location,
            previous_company_name,
            previous_coresignal_company_id,
            previous_experience_id,
            next_company_name,
            next_coresignal_company_id,
            next_experience_id,
            is_starter,
            is_leaver
        FROM `{hra_summary_table_experience_prefix}_*`;
    
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(query)
    query_job.result()
    print(f"Modifies {hra_1_view_id} and {employee_experiences_view_id}")
