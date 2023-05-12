"""create tables for HRA case study

Revision ID: f22ff3ad337c
Revises: f20edf3ba1ad
Create Date: 2022-09-29 02:19:25.002539

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f22ff3ad337c'
down_revision = 'f20edf3ba1ad'
branch_labels = None
depends_on = None


from google.cloud import bigquery
import config

project = config.GCP_PROJECT_ID
staging = config.STAGING
dwh = config.DWH

def upgrade():
    create_tables_query = f"""
        CREATE OR REPLACE table `{project}.{staging}.coresignal_stats` (
            `case_study_id` int64,
            `case_study_name` string,
            `company_datasource_id` int64,
            `nlp_pack` string,
            `coresignal_company_id` int64,
            `url` string,
            `name` string,
            `website` string,
            `size` string,
            `industry` string,
            `followers` int64,
            `founded` int64,
            `created` timestamp,
            `last_updated` timestamp,
            `type` string,
            `employees_count` int64,
            `headquarters_country_parsed` string,
            `company_shorthand_name` string
        );

        CREATE OR REPLACE table `{project}.{staging}.coresignal_company_datasource` (
            `case_study_id` int64,
            `company_datasource_id` int64,
            `company_id` int64,
            `company_name` string,
            `source_id` int64,
            `source_name` string,
            `coresignal_company_id` int64,
            `coresignal_member_ids` array<int64>,
            `created_at` timestamp
        );

        CREATE OR REPLACE table `{project}.{staging}.coresignal_employees` (
            -- common info
            `coresignal_employee_id` int64,
            `case_study_id` int64,
            `case_study_name` string,
            `company_datasource_id` int64,
            -- employee info
            `name` string,
            `title` string,
            `url` string,
            `industry` string,
            `created` timestamp,
            `last_updated` timestamp,
            `location` string,
            `country` string,
            `connection_count` int64,
            -- collections
            `member_experience_collection` array<
                struct<
                    id int64,
                    title string,
                    coresignal_company_id int64,
                    company_name string,
                    date_from date,
                    date_to date,
                    duration string,
                    previous_coresignal_company_id int64,
                    previous_company_name string,
                    previous_experience_id int64,
                    next_coresignal_company_id int64,
                    next_company_name string,
                    next_experience_id int64,
                    google_location string,
                    is_starter bool,
                    is_leaver bool
                >
            >,
            `member_education_collection` array<
                struct<
                    id int64,
                    title string,
                    subtitle string,
                    description string,
                    date_from date,
                    date_to date
                >
            >,
            `created_at` timestamp
        )
        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 4));

        CREATE OR REPLACE table `{project}.{dwh}.hra_casestudy_dimension_config` (
            `case_study_id` int64,
            `dimension_config_id` int64,
            `dimension_config_name` string,
            `dimensions` array<
                struct<
                    dimension_type string,
                    is_used boolean,
                    is_user_defined boolean,
                    job_function_group string,
                    modified_job_function_group string,
                    job_function string,
                    modified_job_function string,
                    keywords array<string>
                >
            >,
            `nlp_pack` string,
            `version` int64,
            `created_at` timestamp,
            `run_id` string
        );

        CREATE OR REPLACE table `{project}.{dwh}.hra_experience_function` (
            `case_study_id` int64,
            `experience_id` int64,
            `company_datasource_id` int64,
            `dimension_config_id` int64,
            `dimension_config_version` int64,
            `job_function` string,
            `job_function_group` string,
            `job_function_similarity` float64,
            `most_similar_keyword` string,
            `run_id` string
        )
        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 4));
        
        CREATE OR REPLACE table `{project}.{staging}.hra_case_study_run_id` (
            `created_at` timestamp,
            `case_study_id` integer,
            `run_id` string
        )
        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 4));
        
        CREATE OR REPLACE table `{project}.{dwh}.hra_company_aliases_list` (
            created_at TIMESTAMP, 
            case_study_id INTEGER, 
            company_id INTEGER, 
            company_name STRING, 
            aliases STRING, 
            run_id STRING, 
            original_aliases STRING
        )
        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000, 4));
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(create_tables_query)
    query_job.result()
    print(f"<create tables for HRA case study> successfully!")


def downgrade():
    rollback_create_tables_query = f"""
        DROP TABLE IF EXISTS `{project}.{staging}.coresignal_stats`;
        DROP TABLE IF EXISTS `{project}.{staging}.coresignal_company_datasource`;
        DROP TABLE IF EXISTS `{project}.{staging}.coresignal_employees`;
        DROP TABLE IF EXISTS `{project}.{staging}.hra_case_study_run_id`;
        DROP TABLE IF EXISTS `{project}.{dwh}.hra_casestudy_dimension_config`;
        DROP TABLE IF EXISTS `{project}.{dwh}.hra_experience_function`;
        DROP TABLE IF EXISTS `{project}.{dwh}.hra_company_aliases_list`;
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(rollback_create_tables_query)
    query_job.result()
    print(f"Rollback <create tables for HRA case study> successfully!")
