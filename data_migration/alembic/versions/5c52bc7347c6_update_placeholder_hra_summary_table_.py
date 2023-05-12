"""Update placeholder hra_summary_table_experience_0

Revision ID: 5c52bc7347c6
Revises: 56c2fe825eb6
Create Date: 2022-10-20 14:38:22.865917

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "5c52bc7347c6"
down_revision = "56c2fe825eb6"
branch_labels = None
depends_on = None


from google.cloud import bigquery

import config

# Get Bigquery Database name
project = config.GCP_PROJECT_ID
dwh = config.DWH
datamart_cs = config.DATAMART_CS
datamart = config.DATAMART

hra_summary_table_experience_prefix = (
    f"{project}.{datamart_cs}.hra_summary_table_experience"
)


def upgrade():
    query = f"""
        -- Recreate
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
            coresignal_company_name STRING, -- NEW COLUMN
            coresignal_company_url STRING, -- NEW COLUMN
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
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(query)
    query_job.result()
    print(f"Recreated placeholder table {hra_summary_table_experience_prefix}_0")


def downgrade():
    query = f"""
        -- Recreate
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
            -- coresignal_company_name STRING, -- REMOVED COLUMN
            -- coresignal_company_url STRING, -- REMOVED COLUMN
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
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(query)
    query_job.result()
    print(f"Reverse placeholder table {hra_summary_table_experience_prefix}_0")
