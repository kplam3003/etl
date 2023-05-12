"""Add placeholder hra_summary_table_education_0 table for wildcard query

Revision ID: 56c2fe825eb6
Revises: 8e945b16d0fa
Create Date: 2022-10-20 14:21:21.055742

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "56c2fe825eb6"
down_revision = "8e945b16d0fa"
branch_labels = None
depends_on = None

from google.cloud import bigquery

import config

# Get Bigquery Database name
project = config.GCP_PROJECT_ID
dwh = config.DWH
datamart_cs = config.DATAMART_CS
datamart = config.DATAMART

hra_summary_table_education_prefix = (
    f"{project}.{datamart_cs}.hra_summary_table_education"
)


def upgrade():
    query = f"""
        -- Create a first, empty table for each of the summary tables so that views can be created
        -- company
        CREATE OR REPLACE TABLE `{hra_summary_table_education_prefix}_0`
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
            
            -- Education information
            education_id INTEGER,
            coresignal_employee_id INTEGER,
            title STRING,
            subtitle STRING,
            description STRING,
            date_from DATE,
            date_to DATE,
        )
        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(query)
    query_job.result()
    print(f"Created placeholder table {hra_summary_table_education_prefix}_0")


def downgrade():
    query = f"""
        DROP TABLE IF EXISTS `{hra_summary_table_education_prefix}_0`;
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(query)
    query_job.result()
    print(f"Dropped placeholder table {hra_summary_table_education_prefix}_0")
