"""HRA - Export education degree dataset

Revision ID: 9175039a32a3
Revises: 63e665b4e0e4
Create Date: 2023-02-01 13:54:09.138757

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9175039a32a3"
down_revision = "63e665b4e0e4"
branch_labels = None
depends_on = None

import config
from google.cloud import bigquery

project = config.GCP_PROJECT_ID
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS


def upgrade():
    upgrade_scripts = f"""
        CREATE OR REPLACE VIEW `{project}.{datamart}.HRA_education_dataset` AS
        SELECT
            created_at,
            case_study_id,
            case_study_name,
            company_datasource_id,
            source_id,
            source_name,
            company_id,
            company_name,
            run_id,
            nlp_pack,
            nlp_type,
            dimension_config_id,
            dimension_config_name,
            education_id,
            coresignal_employee_id,
            title,
            subtitle,
            description,
            date_from,
            date_to,
            school_url,
            degree_level,
            degree_level_code,
            degree_confidence
        FROM `{project}.{datamart_cs}.hra_summary_table_education_degree_*`;
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(upgrade_scripts)
    query_job.result()

    print(f"<HRA - Export education degree dataset> successfully!")


def downgrade():
    downgrade_scripts = f"""
        DROP VIEW IF EXISTS `{project}.{datamart}.HRA_education_dataset;
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(downgrade_scripts)
    query_job.result()

    print(f"Rollback <HRA - Export education degree dataset> successfully!")
