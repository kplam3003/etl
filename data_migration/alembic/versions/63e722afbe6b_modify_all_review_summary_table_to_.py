"""Modify all review summary_table to include new tables

Revision ID: 63e722afbe6b
Revises: b074be58dcbe
Create Date: 2022-06-27 13:59:59.756109

"""
from alembic import op
import sqlalchemy as sa
from google.cloud import bigquery
from google.api_core.exceptions import PreconditionFailed, BadRequest

import config


# revision identifiers, used by Alembic.
revision = '63e722afbe6b'
down_revision = 'b074be58dcbe'
branch_labels = None
depends_on = None


bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)

# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
datamart_cs = config.DATAMART_CS

datamart_cs_tables = [t.full_table_id for t in bqclient.list_tables(datamart_cs)]
summary_table_ids = [
    t.replace(":", ".") 
    for t in datamart_cs_tables
    if "job" not in t and "summary_table_" in t
]

def upgrade():
    """Alter all summary_table_* and voe_summary_table_*"""
    #extract list of case_study_id in summary_table_origin:
    # cs_list=bqclient.query(query_string1).to_dataframe()
    # case_study_list = cs_list['case_study_id'].unique()
    
    # voc first
    i = 1
    num_tables = len(summary_table_ids)
    for table_id in summary_table_ids:
        print(f"\tAltering schema for table {table_id}: {i}/{num_tables}")
        table = bqclient.get_table(table_id)  # Make an API request.
        try:
            original_schema = table.schema
            new_schema = original_schema[:]  # Creates a copy of the schema.
            # add new columns
            new_schema.append(bigquery.SchemaField("parent_review_id", "STRING"))
            new_schema.append(bigquery.SchemaField("technical_type", "STRING"))
            new_schema.append(bigquery.SchemaField("company_datasource_id", "INTEGER"))
            new_schema.append(bigquery.SchemaField("url", "STRING"))
            new_schema.append(bigquery.SchemaField("review_country", "STRING"))
            new_schema.append(bigquery.SchemaField("country_code", "STRING"))
            new_schema.append(bigquery.SchemaField("total_reviews", "INTEGER"))
            new_schema.append(bigquery.SchemaField("total_ratings", "INTEGER"))
            new_schema.append(bigquery.SchemaField("average_rating", "FLOAT"))
            table.schema = new_schema
            table = bqclient.update_table(table, ["schema"])  # Make an API request.
        except (PreconditionFailed, BadRequest):
            print(f"\t\tWARNING: cannot add new columns to table. Maybe columns already exist?")
            
        i += 1
    
    print(f"\nUpgrade {revision} process is successfull!")
    
def downgrade():
    #extract list of case_study_id in summary_table_origin:
    i = 1
    num_tables = len(summary_table_ids)
    for table_id in summary_table_ids:
        print(f"\tAltering schema for table {table_id}: {i}/{num_tables}")
        query = f"""
        ALTER TABLE {table_id}
            DROP COLUMN parent_review_id,
            DROP COLUMN technical_type,
            DROP COLUMN company_datasource_id,
            DROP COLUMN url,
            DROP COLUMN review_country,
            DROP COLUMN country_code,
            DROP COLUMN total_reviews,
            DROP COLUMN total_ratings,
            DROP COLUMN average_rating
        """
        query_job = bqclient.query(query)
        err = query_job.result()
        
        if err:
            print(f"\t\tWARNING: error happens: {err}. Maybe columns do not exist?")
    
    print("\nDowngrade {revision} process is successfull!")


