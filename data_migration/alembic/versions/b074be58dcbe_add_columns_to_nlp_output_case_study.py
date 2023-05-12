"""add columns to nlp_output_case_study

Revision ID: b074be58dcbe
Revises: 547c79447568
Create Date: 2022-06-27 11:27:57.569950

"""
from alembic import op
import sqlalchemy as sa


from google.cloud import bigquery
import gcsfs
import config

# revision identifiers, used by Alembic.
revision = 'b074be58dcbe'
down_revision = '547c79447568'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()


# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
dwh = config.DWH

query_string1 = f"""
        ALTER TABLE `{project}.{dwh}.nlp_output_case_study`
        ADD COLUMN total_reviews INT64,
        ADD COLUMN total_ratings INT64,
        ADD COLUMN average_rating FLOAT64;
        ALTER TABLE `{project}.{dwh}.voe_nlp_output_case_study`
        ADD COLUMN total_reviews INT64,
        ADD COLUMN total_ratings INT64,
        ADD COLUMN average_rating FLOAT64;
    """
query_string2 = f"""
        ALTER TABLE `{project}.{dwh}.nlp_output_case_study`
        DROP COLUMN total_reviews,
        DROP COLUMN total_ratings,
        DROP COLUMN average_rating;
        ALTER TABLE `{project}.{dwh}.voe_nlp_output_case_study`
        DROP COLUMN total_reviews,
        DROP COLUMN total_ratings,
        DROP COLUMN average_rating;
    """
def upgrade():
    #########  Adding columns in voe_nlp_output_case_study, nlp_output_case_study #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\Adding columns in nlp_output_case_study, voe_nlp_output_case_study table successfull!")


def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade b074be58dcbe process is successfull!")
