"""create_data_quality_dataset

Revision ID: 069d86599e52
Revises: c794828e7a01

Create Date: 2021-03-04 07:33:35.554610

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
revision = '069d86599e52'
down_revision = 'c794828e7a01'
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

    ###################################
    ########## Step 1: CREATE DATASET #################

    for database in database_list:
        dataset_id = "{}.{}".format(project,database)    

        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)

        # Specify the geographic location where the dataset should reside.
        dataset.location = "US"

        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
   
        dataset = bqclient.create_dataset(dataset)  # Make an API request.
        print("Created dataset {}.{}".format(project, dataset.dataset_id))


    

def downgrade():

    for database in database_list:
        dataset_id = "{}.{}".format(project,database)
         
        bqclient.delete_dataset(
            dataset_id, delete_contents=True, not_found_ok=True
        )  # Make an API request.
        print("Deleted  dataset {}.{}".format(project, database))

