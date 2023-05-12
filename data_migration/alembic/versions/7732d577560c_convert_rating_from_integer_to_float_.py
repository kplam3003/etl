"""convert rating from integer to float - table voc_3

Revision ID: 7732d577560c
Revises: c221a24abdae
Create Date: 2021-03-04 04:11:31.153510

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
revision = '7732d577560c'
down_revision = 'c221a24abdae'
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
    """convert rating from integer to float - table voc"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    time.sleep(5)

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Upgrade 7732d577560c process is successfull!")

    
    
def downgrade():
    pass
    print("\n Downgrade 7732d577560c process is successfull!")
    
    
query_string1 = f"""
### Create table backup with new type of rating column:
CREATE OR REPLACE TABLE `{project}.{datamart}.VOC_3_ratingtime_table_bk` 
  (
case_study_id	int64	,
case_study_name	string	,
dimension_config_name	string	,
dimension_config_id	int64	,
nlp_type	string	,
nlp_pack	string	,
source_id	int64	,
source_name	string	,
company_name	string	,
company_id	int64	,
daily_date	date	,
records	int64	,
records_daily	int64	,
rating_daily	float64	,
run_id	string	,
RATING_MA7	float64	,
RECORDS_MA7	int64	,
RATING_MA14	float64	,
RECORDS_MA14	int64	,
RATING_MA30	float64	,
RECORDS_MA30	int64	,
RATING_MA60	float64	,
RECORDS_MA60	int64	,
RATING_MA90	float64	,
RECORDS_MA90	int64	,
RATING_MA120	float64	,
RECORDS_MA120	int64	,
RATING_MA150	float64	,
RECORDS_MA150	int64	,
RATING_MA180	float64	,
RECORDS_MA180	int64	

  )


PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) 

;

### Insert data into table:
INSERT INTO `{project}.{datamart}.VOC_3_ratingtime_table_bk` 
SELECT * FROM `{project}.{datamart}.VOC_3_ratingtime_table`; 


### Drop table voc and create the new one:

DROP TABLE `{project}.{datamart}.VOC_3_ratingtime_table` ;

  
"""
query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{datamart}.VOC_3_ratingtime_table` 
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) 
AS
SELECT * FROM `{project}.{datamart}.VOC_3_ratingtime_table_bk` ;

### Drop table voc_bk:

DROP TABLE `{project}.{datamart}.VOC_3_ratingtime_table_bk` ;

"""
