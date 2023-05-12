"""create new dataset datamart_cs to store summary_tables

Revision ID: be144eb1d8fa
Revises: 67a3c1eda433
Create Date: 2021-04-13 09:43:56.675128

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
revision = 'be144eb1d8fa'
down_revision = '67a3c1eda433'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()


# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT, config.DATAMART_CS]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS

def upgrade():
    ###################################
    ##CREATE DATASET DATAMART_CS #################

    
    dataset_id = "{}.{}".format(project,datamart_cs)    

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
   
    dataset = bqclient.create_dataset(dataset)  # Make an API request.
    print("Created dataset {}.{}".format(project, datamart_cs))


def downgrade():
    pass
