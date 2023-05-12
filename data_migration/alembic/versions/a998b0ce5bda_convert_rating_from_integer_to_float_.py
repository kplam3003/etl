"""convert rating from integer to float - nlp_output_case_study

Revision ID: a998b0ce5bda
Revises: 9336586cc4c3
Create Date: 2021-03-03 09:55:21.839962

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
revision = 'a998b0ce5bda'
down_revision = '9336586cc4c3'
branch_labels = None
depends_on = None


bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()


# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART

def upgrade():
    """convert rating from integer to float - table nlp_output_case_study"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    time.sleep(5)

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Upgrade a998b0ce5bda process is successfull!")

    
    
def downgrade():
    pass
    print("\n Downgrade a998b0ce5bda process is successfull!")
    
    
query_string1 = f"""
### Create table backup with new type of rating column:
CREATE OR REPLACE TABLE `{project}.{dwh}.nlp_output_case_study_bk`

(
created_at	    timestamp	,
review_id	    string	,
source_name	    string	,
company_name	string	,
nlp_pack	    string	,
nlp_type	    string	,
user_name	    string	,
language	    string	,
review	        string	,
trans_review	string	,
trans_status	string	,
code	        string	,
dimension	    string	,
label	        string	,
terms	        string	,
relevance	    float64	,
rel_relevance	float64	,
polarity	    string	,
rating	        float64	,
batch_id	    int64	,
batch_name	    string	,
file_name	    string	,
review_date	    timestamp	,
company_id	    int64	,
source_id	    int64	,
step_id	        int64	,
request_id	    int64	,
is_target	    bool	,
case_study_id	int64	,
case_study_name	string	,
dimension_config_id	int64,
run_id          string	
)

PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
;

### Insert data into table:
INSERT INTO `{project}.{dwh}.nlp_output_case_study_bk` 
SELECT * FROM `{project}.{dwh}.nlp_output_case_study`; 


### Drop table nlp_output_case_study and create the new one:

DROP TABLE `{project}.{dwh}.nlp_output_case_study` ;

  
"""
query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{dwh}.nlp_output_case_study` 
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) 
AS
SELECT * FROM `{project}.{dwh}.nlp_output_case_study_bk` ;

### Drop table nlp_output_case_study_bk:

DROP TABLE `{project}.{dwh}.nlp_output_case_study_bk` ;

"""
