"""HRA - Add education degree table

Revision ID: 63e665b4e0e4
Revises: 4ce967d5b194
Create Date: 2023-01-03 10:42:10.443274

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "63e665b4e0e4"
down_revision = "4ce967d5b194"
branch_labels = None
depends_on = None

from google.cloud import bigquery
from typing import List
import config

project = config.GCP_PROJECT_ID
staging = config.STAGING
dwh = config.DWH
datamart_cs = config.DATAMART_CS


def _get_existing_casestudy_ids() -> List[int]:
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_cs_ids = f"""
        SELECT DISTINCT case_study_id 
        FROM `{project}.{datamart_cs}.hra_summary_table_experience_*`   
    """
    query_results = bqclient.query(query_cs_ids).result()

    case_study_ids: List[int] = []
    for row in query_results:
        case_study_ids.append(row.case_study_id)

    return case_study_ids


def upgrade():
    casestudy_ids = _get_existing_casestudy_ids()
    upgrade_script = f"""
        -- STAG - Add `school_url` to table `coresignal_employees_education`
        ALTER TABLE `{project}.{staging}.coresignal_employees_education`
        ADD COLUMN IF NOT EXISTS school_url STRING;

        -- DWH - New table
        CREATE OR REPLACE table `{project}.{dwh}.hra_education_degree` (
            `case_study_id` int64,
            `company_datasource_id` int64,
            `dimension_config_id` int64,
            `dimension_config_version` int64,
            `education_id` int64,
            `subtitle` string,
            `level` string,
            `level_code` int64,
            `confidence` float64,
            `run_id` string
        )
        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 4));

        -- DATAMART_CS - New summary tables & New columns for existing casestudy
    """
    for cs_id in casestudy_ids:
        upgrade_script += f"""
            CREATE OR REPLACE TABLE `{project}.{datamart_cs}.hra_summary_table_education_degree_{cs_id}`
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

                -- Education info
                education_id INTEGER,
                coresignal_employee_id INTEGER,
                title STRING,
                subtitle STRING,
                description STRING,
                date_from DATE,
                date_to DATE,
                school_url STRING,

                -- Education degree
                degree_level STRING,
                degree_level_code INTEGER,
                degree_confidence FLOAT64
            )
            PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1));

            ALTER TABLE `{project}.{datamart_cs}.hra_summary_table_employees_{cs_id}`
            ADD COLUMN IF NOT EXISTS highest_education_level_code INTEGER,
            ADD COLUMN IF NOT EXISTS highest_education_level STRING;
        """

    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(upgrade_script)
    query_job.result()
    print(f"<HRA - Add education degree table> successfully!")


def downgrade():
    casestudy_ids = _get_existing_casestudy_ids()
    downgrade_script = f"""
        ALTER TABLE `{project}.{staging}.coresignal_employees_education`
        DROP COLUMN IF EXISTS school_url;

        DROP TABLE IF EXISTS `{project}.{dwh}.hra_education_degree`;
    """
    for cs_id in casestudy_ids:
        downgrade_script += f"""
            DROP TABLE IF EXISTS `{project}.{datamart_cs}.hra_summary_table_education_degree_{cs_id}`;

            ALTER TABLE `{project}.{datamart_cs}.hra_summary_table_employees_{cs_id}`
            DROP COLUMN IF EXISTS highest_education_level_code,
            DROP COLUMN IF EXISTS highest_education_level;
        """

    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(downgrade_script)
    query_job.result()
    print(f"Rollback <HRA - Add education degree table> successfully!")
