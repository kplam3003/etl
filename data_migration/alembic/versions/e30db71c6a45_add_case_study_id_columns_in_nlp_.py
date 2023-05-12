"""add case_study_id columns in nlp_statistics

Revision ID: e30db71c6a45
Revises: 33e70686d17c
Create Date: 2021-08-07 23:54:23.453401

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery
import config

# revision identifiers, used by Alembic.
revision = 'e30db71c6a45'
down_revision = '33e70686d17c'
branch_labels = None
depends_on = None

project = config.GCP_PROJECT_ID
dwh = config.DWH

voc_nlp_statistics_table_id = f"{project}.{dwh}.voc_nlp_statistics"
voe_nlp_statistics_table_id = f"{project}.{dwh}.voe_nlp_statistics"


def upgrade():
    client = bigquery.Client()
    
    for table_id in [voc_nlp_statistics_table_id, voe_nlp_statistics_table_id]:
        table = client.get_table(table_id)
        original_schema = table.schema
        new_schema = original_schema[:]  # Creates a copy of the schema.
        new_schema.append(bigquery.SchemaField("case_study_id", "INTEGER")) # add new column
        table.schema = new_schema
        table = client.update_table(table, ["schema"])  # Make an API request.
        print(f"New column `case_study_id` added into table {table_id} successfully")
        

def downgrade():
    client = bigquery.Client()
    
    for table_id in [voc_nlp_statistics_table_id, voe_nlp_statistics_table_id]:
        query = f"""
        ALTER TABLE `{table_id}`
        DROP COLUMN IF EXISTS case_study_id;
        """
        query_job = client.query(query)
        query_job.result()
        print(f"Column `case_study_id` deleted from table {table_id} successfully")
