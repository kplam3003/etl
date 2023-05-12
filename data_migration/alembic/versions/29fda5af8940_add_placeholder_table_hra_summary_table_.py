"""Add placeholder table hra_summary_table_monthly_dataset_0

Revision ID: 29fda5af8940
Revises: cd43d8d89102
Create Date: 2022-11-29 14:27:23.271378

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '29fda5af8940'
down_revision = 'cd43d8d89102'
branch_labels = None
depends_on = None


from google.cloud import bigquery

import config

# Get Bigquery Database name
project = config.GCP_PROJECT_ID
dwh = config.DWH
datamart_cs = config.DATAMART_CS
datamart = config.DATAMART

hra_summary_table_monthly_dataset = (
    f"{project}.{datamart_cs}.hra_summary_table_monthly_dataset"
)
hra_view_monthly_dataset = f"{project}.{datamart}.HRA_monthly_dataset"


def upgrade():
    query = f"""
        -- Recreate
        CREATE OR REPLACE TABLE `{hra_summary_table_monthly_dataset}_0`
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
            experience_company_id INTEGER,
            coresignal_employee_id INTEGER,
            coresignal_company_id INTEGER,
            coresignal_company_name STRING,
            coresignal_company_url STRING,
            date_from DATE,
            date_to DATE,
            duration STRING,
            previous_coresignal_company_id INTEGER,
            previous_company_name STRING,
            previous_experience_id INTEGER,
            next_coresignal_company_id INTEGER,
            next_company_name STRING,
            next_experience_id INTEGER,
            google_country STRING,
            google_admin1 STRING,
            google_address_components ARRAY<
                STRUCT<
                    long_name STRING,
                    short_name STRING,
                    types ARRAY<STRING>
                >
            >,
            is_starter BOOLEAN,
            is_leaver BOOLEAN,

            is_target BOOLEAN,
            is_cohort BOOLEAN,
            
            -- Monthly dataset
            reference_date DATE,
            workforce_years FLOAT64,
            company_years FLOAT64,
            role_years FLOAT64,
            last_education_years FLOAT64
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));
        
        -- Create view for export
        CREATE OR REPLACE VIEW `{hra_view_monthly_dataset}` AS
        SELECT
            case_study_id,
            case_study_name,
            nlp_pack,
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
            date_from,
            date_to,
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
            
            -- Monthly dataset
            reference_date,
            workforce_years,
            company_years,
            role_years,
            last_education_years
        FROM `{hra_summary_table_monthly_dataset}_*`;
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(query)
    query_job.result()
    print(f"Recreated placeholder table {hra_summary_table_monthly_dataset}_0")


def downgrade():
    query = f"""
        -- Drop created views
        DROP TABLE IF EXISTS `{hra_summary_table_monthly_dataset}_0`;
        DROP VIEW IF EXISTS `{hra_view_monthly_dataset}`;
        
        -- Revert view changes
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(query)
    query_job.result()
    print(f"Drop placeholder table {hra_summary_table_monthly_dataset}_0")
