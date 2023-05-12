"""add data_type to voc_crawl_statistics

Revision ID: 5b03ddc6a3c2
Revises: 942e326ae9fc
Create Date: 2021-08-10 16:08:13.206192

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery
import config

# revision identifiers, used by Alembic.
revision = '5b03ddc6a3c2'
down_revision = '942e326ae9fc'
branch_labels = None
depends_on = None

project = config.GCP_PROJECT_ID
dwh = config.DWH

voc_crawl_statistics_table_id = f"{project}.{dwh}.voc_crawl_statistics"


def upgrade():
    client = bigquery.Client()
    table = client.get_table(voc_crawl_statistics_table_id)
    original_schema = table.schema
    new_schema = original_schema[:]  # Creates a copy of the schema.
    new_schema.append(bigquery.SchemaField("data_type", "STRING")) # add new column
    table.schema = new_schema
    table = client.update_table(table, ["schema"])  # Make an API request.
    print(f"New column `data_type` added into table {voc_crawl_statistics_table_id} successfully")
        

def downgrade():
    client = bigquery.Client()
    query = f"""
    ALTER TABLE `{voc_crawl_statistics_table_id}`
    DROP COLUMN IF EXISTS data_type;
    """
    query_job = client.query(query)
    query_job.result()
    print(f"Column `data_type` deleted from table {voc_crawl_statistics_table_id} successfully")
