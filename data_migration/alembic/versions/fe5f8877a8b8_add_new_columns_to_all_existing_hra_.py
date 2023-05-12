"""Add new columns to all existing hra_summary_table_experience_* tables

Revision ID: fe5f8877a8b8
Revises: 92895cf4d879
Create Date: 2022-10-21 09:52:34.678033

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "fe5f8877a8b8"
down_revision = "92895cf4d879"
branch_labels = None
depends_on = None

from google.cloud import bigquery

import config

# Get Bigquery Database name
project = config.GCP_PROJECT_ID
dwh = config.DWH
datamart_cs = config.DATAMART_CS
datamart = config.DATAMART

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)


def upgrade():
    # get all current case_study_id
    query_cs_ids = f"""
        SELECT DISTINCT case_study_id 
        FROM `{project}.{datamart_cs}.hra_summary_table_experience_*`   
    """
    query_results = bqclient.query(query_cs_ids).result()
    case_study_list = set()
    for row in query_results:
        case_study_list.add(row.case_study_id)

    # add
    # NOTE: this only alter tables, adding new columns at the end, but still no data
    # CS needs to be re-run before having data.
    query_alter_table = """
        ALTER TABLE `{project}.{datamart_cs}.hra_summary_table_experience_{case_study_id}`
        ADD COLUMN IF NOT EXISTS coresignal_company_id INTEGER,
        ADD COLUMN IF NOT EXISTS coresignal_company_name STRING,
        ADD COLUMN IF NOT EXISTS coresignal_company_url STRING;
    """
    for case_study_id in case_study_list:
        _ = bqclient.query(
            query_alter_table.format(
                project=project,
                datamart_cs=datamart_cs,
                case_study_id=case_study_id,
            )
        ).result()

    print("Modified hra_summary_table_experience to include 3 new columns")


def downgrade():
    # get all current case_study_id
    query_cs_ids = f"""
        SELECT DISTINCT case_study_id 
        FROM `{project}.{datamart_cs}.hra_summary_table_experience_*`   
    """
    query_results = bqclient.query(query_cs_ids).result()
    case_study_list = set()
    for row in query_results:
        case_study_list.add(row.case_study_id)

    # remove
    query_alter_table = """
        ALTER TABLE `{project}.{datamart_cs}.hra_summary_table_experience_{case_study_id}`
        ADD COLUMN IF EXISTS coresignal_company_id INTEGER,
        ADD COLUMN IF EXISTS coresignal_company_name STRING,
        ADD COLUMN IF EXISTS coresignal_company_url STRING;
    """
    for case_study_id in case_study_list:
        _ = bqclient.query(
            query_alter_table.format(
                project=project,
                datamart_cs=datamart_cs,
                case_study_id=case_study_id,
            )
        ).result()

    print("Modified hra_summary_table_experience removing 3 columns")
