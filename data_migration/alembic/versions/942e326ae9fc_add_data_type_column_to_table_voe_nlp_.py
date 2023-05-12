"""add data_type column to table voe_nlp_statistics BQ

Revision ID: 942e326ae9fc
Revises: 84b61807a133
Create Date: 2021-08-10 15:05:38.926055

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery
import config

# revision identifiers, used by Alembic.
revision = '942e326ae9fc'
down_revision = '84b61807a133'
branch_labels = None
depends_on = None

project = config.GCP_PROJECT_ID
dwh = config.DWH

voe_crawl_statistics_table_id = f"{project}.{dwh}.voe_crawl_statistics"


def upgrade():
    client = bigquery.Client()
    table = client.get_table(voe_crawl_statistics_table_id)
    original_schema = table.schema
    new_schema = original_schema[:]  # Creates a copy of the schema.
    new_schema.append(bigquery.SchemaField("data_type", "STRING")) # add new column
    table.schema = new_schema
    table = client.update_table(table, ["schema"])  # Make an API request.
    print(f"New column `data_type` added into table {voe_crawl_statistics_table_id} successfully")
        

def downgrade():
    client = bigquery.Client()
    query = f"""
    ALTER TABLE `{voe_crawl_statistics_table_id}`
    DROP COLUMN IF EXISTS data_type;
    """
    query_job = client.query(query)
    query_job.result()
    print(f"Column `data_type` deleted from table {voe_crawl_statistics_table_id} successfully")
