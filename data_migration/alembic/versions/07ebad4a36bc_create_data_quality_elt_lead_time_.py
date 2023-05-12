"""create_data_quality_elt_lead_time_monitor_dedup

Revision ID: 07ebad4a36bc
Revises: d7def269c23f
Create Date: 2021-03-10 08:16:45.932744

"""
from alembic import op
import sqlalchemy as sa


from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import gcsfs
import pandas as pd
import ast
import time
import config


# revision identifiers, used by Alembic.
revision = '07ebad4a36bc'
down_revision = 'd7def269c23f'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem() 

# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.DATA_QUALITY]
data_quality=config.DATA_QUALITY  

def upgrade():
    
    query_string1 = f"""
        CREATE OR REPLACE VIEW `{project}.{data_quality}.elt_lead_time_monitor_dedup` 
        AS
        WITH new_table AS (
            SELECT 
                *, 
                ROW_NUMBER() OVER(
                    PARTITION BY 
                        request_id,
                        case_study_id,
                        batch_id,
                        step_id, 
                        step_detail_id 
                    ORDER BY inserted_at DESC
                ) rn
                FROM `{project}.{data_quality}.etl_lead_time_monitor`
        )
        
        SELECT 
            *
            EXCEPT(rn)
        FROM new_table
        WHERE rn = 1
        ;


    """
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating view elt_lead_time_monitor_dedup in Data Quality successfull!")


def downgrade():
    
    query_string2 = f"""
        DROP  VIEW  `{project}.{data_quality}.elt_lead_time_monitor_dedup`  ;
        """
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 07ebad4a36bc process is successfull!")
