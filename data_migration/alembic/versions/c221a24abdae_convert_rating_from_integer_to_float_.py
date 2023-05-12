"""convert rating from integer to float - summary_table_origin

Revision ID: c221a24abdae
Revises: a998b0ce5bda
Create Date: 2021-03-03 10:11:26.032067

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
revision = 'c221a24abdae'
down_revision = 'a998b0ce5bda'
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
    """convert rating from integer to float - table summary_table_origin"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    time.sleep(5)

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Upgrade c221a24abdae process is successfull!")

    
    
def downgrade():
    pass
    print("\n Downgrade c221a24abdae process is successfull!")
    
    
query_string1 = f"""
### Create table backup with new type of rating column:
CREATE OR REPLACE TABLE `{project}.{dwh}.summary_table_origin_bk`

(
created_at	                timestamp	,
case_study_id	            int64	,
case_study_name	            string	,
source_id	                int64	,
source_name	                string	,
company_id	                int64	,
company_name	            string	,
nlp_pack	                string	,
nlp_type	                string	,
user_name	                string	,
dimension_config_id	        int64	,
dimension_config_name       string	,
review_id	                string	,
review	                    string	,
trans_review	            string	,
trans_status	            string	,
review_date	                date	,
rating	                    float64	,
language_code	            string	,
language	                string	,
code	                    string	,
dimension_type	            string	,
dimension	                string	,
modified_dimension	        string	,
label	                    string	,
modified_label	            string	,
is_used	                    bool	,
terms	                    string	,
relevance	                float64	,
rel_relevance	            float64	,
polarity	                string	,
modified_polarity	        float64	,
batch_id	                int64	,
batch_name	                string	,
run_id                      string


)
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
;

### Insert data into table:
INSERT INTO `{project}.{dwh}.summary_table_origin_bk` 
SELECT * FROM `{project}.{dwh}.summary_table_origin`; 


### Drop table summary_table_origin and create the new one:

DROP TABLE `{project}.{dwh}.summary_table_origin` ;

  
"""
query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{dwh}.summary_table_origin` 
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) 
AS
SELECT * FROM `{project}.{dwh}.summary_table_origin_bk` ;

### Drop table summary_table_origin_bk:

DROP TABLE `{project}.{dwh}.summary_table_origin_bk` ;

"""
