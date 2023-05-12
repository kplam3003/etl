"""add run_id to polarity_trans, casestudy_dimension_config, casestudy_company_source

Revision ID: 9cf121b0b3c0
Revises: 402ee94ab14a
Create Date: 2021-01-29 06:33:16.721234

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
revision = '9cf121b0b3c0'
down_revision = '402ee94ab14a'
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
    """add run_id to polarity_trans, casestudy_dimension_config, casestudy_company_source, casestudy_batchid"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")
    
query_string1 = f"""

##### polarity_trans:
CREATE OR REPLACE TABLE `{project}.{dwh}.polarity_trans_bk` 
AS
SELECT *, CAST(NULL AS string) run_id FROM `{project}.{dwh}.polarity_trans`;

DROP TABLE `{project}.{dwh}.polarity_trans`;

CREATE OR REPLACE TABLE `{project}.{dwh}.polarity_trans` 
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
AS
SELECT * FROM `{project}.{dwh}.polarity_trans_bk`;

DROP TABLE `{project}.{dwh}.polarity_trans_bk`;
	
##### casestudy_company_source:

CREATE OR REPLACE TABLE `{project}.{dwh}.casestudy_company_source_bk` 
AS
SELECT * , CAST(NULL AS string) run_id FROM `{project}.{dwh}.casestudy_company_source`;

DROP TABLE `{project}.{dwh}.casestudy_company_source`;

CREATE OR REPLACE TABLE `{project}.{dwh}.casestudy_company_source`
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
AS
SELECT * FROM `{project}.{dwh}.casestudy_company_source_bk`;

DROP TABLE `{project}.{dwh}.casestudy_company_source_bk`;	


##### casestudy_dimension_config:
	
CREATE OR REPLACE TABLE `{project}.{dwh}.casestudy_dimension_config_bk` 
AS
SELECT *, CAST(NULL AS string) run_id FROM `{project}.{dwh}.casestudy_dimension_config`;

DROP TABLE `{project}.{dwh}.casestudy_dimension_config`;

CREATE OR REPLACE TABLE `{project}.{dwh}.casestudy_dimension_config` 
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
AS
SELECT * FROM `{project}.{dwh}.casestudy_dimension_config_bk`;

DROP TABLE `{project}.{dwh}.casestudy_dimension_config_bk`;


###### casestudy_batchid:

CREATE OR REPLACE TABLE `{project}.{staging}.casestudy_batchid_bk` 
AS
SELECT *, CAST(NULL AS string) run_id FROM `{project}.{staging}.casestudy_batchid`;

DROP TABLE `{project}.{staging}.casestudy_batchid`;

CREATE OR REPLACE TABLE `{project}.{staging}.casestudy_batchid` 
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
AS
SELECT * FROM `{project}.{staging}.casestudy_batchid_bk`;

DROP TABLE `{project}.{staging}.casestudy_batchid_bk`;

"""

query_string2 = f"""

###### drop polarity_trans:

CREATE OR REPLACE TABLE `{project}.{dwh}.polarity_trans_bk` 
AS
SELECT * excep(run_id) FROM `{project}.{dwh}.polarity_trans`;

DROP TABLE `{project}.{dwh}.polarity_trans`;

CREATE OR REPLACE TABLE `{project}.{dwh}.polarity_trans` 
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
AS
SELECT * FROM `{project}.{dwh}.polarity_trans_bk`;

DROP TABLE `{project}.{dwh}.polarity_trans_bk`;

##### drop casestudy_company_source:

CREATE OR REPLACE TABLE `{project}.{dwh}.casestudy_company_source_bk` 
AS
SELECT * except(run_id) FROM `{project}.{dwh}.casestudy_company_source`;

DROP TABLE `{project}.{dwh}.casestudy_company_source`;

CREATE OR REPLACE TABLE `{project}.{dwh}.casestudy_company_source`
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
AS
SELECT * FROM `{project}.{dwh}.casestudy_company_source_bk`;

DROP TABLE `{project}.{dwh}.casestudy_company_source_bk`;	

##### drop casestudy_dimension_config:
	
CREATE OR REPLACE TABLE `{project}.{dwh}.casestudy_dimension_config_bk` 
AS
SELECT * except(run_id) FROM `{project}.{dwh}.casestudy_dimension_config`;

DROP TABLE `{project}.{dwh}.casestudy_dimension_config`;

CREATE OR REPLACE TABLE `{project}.{dwh}.casestudy_dimension_config` 
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
AS
SELECT * FROM `{project}.{dwh}.casestudy_dimension_config_bk`;

DROP TABLE `{project}.{dwh}.casestudy_dimension_config_bk`;

###### drop casestudy_batchid:

CREATE OR REPLACE TABLE `{project}.{staging}.casestudy_batchid_bk` 
AS
SELECT * except(run_id) FROM `{project}.{staging}.casestudy_batchid`;

DROP TABLE `{project}.{staging}.casestudy_batchid`;

CREATE OR REPLACE TABLE `{project}.{staging}.casestudy_batchid` 
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
AS
SELECT * FROM `{project}.{staging}.casestudy_batchid_bk`;

DROP TABLE `{project}.{staging}.casestudy_batchid_bk`;


"""