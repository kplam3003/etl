"""add column success_rate into nlp statistics table

Revision ID: d2e06f196f01
Revises: 07a5e5a8b23d
Create Date: 2021-08-07 23:02:26.916292

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery

import config

# revision identifiers, used by Alembic.
revision = 'd2e06f196f01'
down_revision = '07a5e5a8b23d'
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
        new_schema.append(bigquery.SchemaField("num_total_reviews", "INTEGER")) # add new column
        table.schema = new_schema
        table = client.update_table(table, ["schema"])  # Make an API request.
        print(f"New column `num_total_reviews` added into table {table_id} successfully")
        

def downgrade():
    client = bigquery.Client()
    
    for table_id in [voc_nlp_statistics_table_id, voe_nlp_statistics_table_id]:
        query = f"""
        ALTER TABLE `{table_id}`
        DROP COLUMN IF EXISTS num_total_reviews;
        """
        query_job = client.query(query)
        query_job.result()
        print(f"Column `num_total_reviews` deleted from table {table_id} successfully")
        
