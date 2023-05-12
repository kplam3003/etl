"""Add HRA_1, company stats, employees, and experiences sql for exports

Revision ID: 29b131608403
Revises: f22ff3ad337c
Create Date: 2022-10-12 16:56:08.530795

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "29b131608403"
down_revision = "f22ff3ad337c"
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


def upgrade():
    query = f"""
        -- Create a first, empty table for each of the summary tables so that views can be created
        -- company
        CREATE OR REPLACE TABLE `{hra_summary_table_company_prefix}_0`
        (
            -- Common
            created_at TIMESTAMP,
            case_study_id INTEGER,
            case_study_name STRING,
            company_datasource_id INTEGER,
            source_id INTEGER,
            source_name STRING,
            company_id INTEGER,
            company_name STRING,
            run_id STRING,
            
            -- Dimension info
            nlp_pack STRING,
            nlp_type STRING,
            dimension_config_id INTEGER,
            dimension_config_name STRING,
            
            -- company stats result
            coresignal_company_id INTEGER, 
            coresignal_url STRING, 
            name STRING, 
            website STRING, 
            size STRING, 
            industry STRING, 
            followers INTEGER, 
            founded INTEGER, 
            created TIMESTAMP, 
            last_updated TIMESTAMP, 
            type STRING, 
            employees_count INTEGER, 
            headquarters_country_parsed STRING, 
            company_shorthand_name STRING, 
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));
        
        -- employee
        CREATE OR REPLACE TABLE `{hra_summary_table_employees_prefix}_0`
        (
            -- Common
            created_at TIMESTAMP,
            case_study_id INTEGER,
            case_study_name STRING,
            company_datasource_id INTEGER,
            source_id INTEGER,
            source_name STRING,
            company_id INTEGER,
            company_name STRING,
            url STRING,
            run_id STRING,
            
            -- Dimension info
            nlp_pack STRING,
            nlp_type STRING,
            dimension_config_id INTEGER,
            dimension_config_name STRING,
            
            -- employee info result
            coresignal_employee_id INTEGER,
            name STRING,
            title STRING,
            industry STRING,
            created TIMESTAMP,
            last_updated TIMESTAMP,
            location STRING,
            country STRING,
            connection_count INTEGER,
            num_experience_records INTEGER,
            num_education_records INTEGER
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));
        
        -- experience
        CREATE OR REPLACE TABLE `{hra_summary_table_experience_prefix}_0`
        (
            -- Common
            created_at TIMESTAMP,
            case_study_id INTEGER,
            case_study_name STRING,
            company_datasource_id INTEGER,
            source_id INTEGER,
            source_name STRING,
            company_id INTEGER,
            company_name STRING,
            run_id STRING,
            
            -- Dimension info
            nlp_pack STRING,
            nlp_type STRING,
            dimension_config_id INTEGER,
            dimension_config_name STRING,
            
            -- Job classification result
            experience_id INTEGER,
            title STRING,
            job_function_group STRING,
            modified_job_function_group STRING,
            job_function STRING,
            modified_job_function STRING,
            is_used BOOLEAN,
            most_similar_keyword STRING,
            
            -- Experience information
            coresignal_employee_id INTEGER,
            coresignal_company_id INTEGER,
            date_from DATE,
            date_to DATE,
            duration STRING,
            previous_coresignal_company_id INTEGER,
            previous_company_name STRING,
            previous_experience_id INTEGER,
            next_coresignal_company_id INTEGER,
            next_company_name STRING,
            next_experience_id INTEGER,
            google_location STRING,
            is_starter BOOLEAN,
            is_leaver BOOLEAN,
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));
        
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
            
        -- company_profiles_view_id
        CREATE OR REPLACE VIEW {company_profiles_view_id} AS
        SELECT
            case_study_id,
            case_study_name,
            nlp_pack,
            coresignal_company_id AS id,
            coresignal_url AS url,
            name,
            website,
            size,
            industry,
            followers,
            founded,
            created,
            last_updated,
            type,
            employees_count,
            headquarters_country_parsed AS headquarter,
            company_shorthand_name,
        FROM `{hra_summary_table_company_prefix}_*`;
        
        -- employee_profiles_view_id
        CREATE OR REPLACE VIEW {employee_profiles_view_id} AS
        SELECT
            case_study_id,
            case_study_name,
            nlp_pack,
            coresignal_employee_id AS employee_id,
            source_name,
            company_name,
            name,
            title
            url,
            industry,
            created,
            last_updated,
            location AS google_location,
            country,
            connection_count,
        FROM `{hra_summary_table_employees_prefix}_*`;
        
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
    print(
        f"Created views {hra_1_view_id}, {company_profiles_view_id}, {employee_profiles_view_id}, {employee_experiences_view_id}"
    )


def downgrade():
    query = f"""
        -- HRA_1_profilestats
        DROP VIEW IF EXISTS {hra_1_view_id};
            
        -- company_profiles_view_id
        DROP VIEW IF EXISTS {company_profiles_view_id};
        
        -- employee_profiles_view_id
        DROP VIEW IF EXISTS {employee_profiles_view_id};
        
        -- Job experience
        DROP VIEW IF EXISTS {employee_experiences_view_id};
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(query)
    query_job.result()
    print(
        f"Dropped views {hra_1_view_id}, {company_profiles_view_id}, {employee_profiles_view_id}, {employee_experiences_view_id}"
    )
