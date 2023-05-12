"""remove username from nlp_output and review_raw_data

Revision ID: 72e4ee1d66c5
Revises: bdcd29b62ccf
Create Date: 2021-01-28 10:17:08.514331

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
revision = '72e4ee1d66c5'
down_revision = 'bdcd29b62ccf'
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
    """remove username from nlp_output and review_raw_data"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")
    
query_string1 = f"""

###### nlp_output:

CREATE OR REPLACE VIEW `{project}.{datamart}.nlp_output` AS

SELECT 

    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    language,
    nlp_pack,
    nlp_type,
    review_id,
    review as orig_review,
    trans_review,

    CASE
    WHEN LENGTH(trans_review)=0 THEN 0
    ELSE LENGTH(trans_review)-LENGTH(REPLACE(trans_review," ",""))+1
    END AS words_count,

    CHAR_LENGTH(trans_review) AS characters_count,
    rating,
    review_date,
    modified_dimension as dimension,
    modified_label as label,
    terms,
    relevance,
    rel_relevance,
    polarity,
    modified_polarity as sentiment_score

FROM 
`{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
;

######## review_raw_data:

CREATE OR REPLACE VIEW `{project}.{datamart}.review_raw_data` AS

SELECT 
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    review_id,
    review as orig_review,
    language,
    trans_review,
    rating,
    review_date

FROM 
`{project}.{dwh}.summary_table`

WHERE dimension_config_name is not null
GROUP BY 
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    review_id,
    language,
    review,
    trans_review,
    rating,
    review_date;
"""

query_string2 = f"""

###### nlp_output:

CREATE OR REPLACE VIEW `{project}.{datamart}.nlp_output` AS
SELECT 
    user_name,
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    language,
    nlp_pack,
    nlp_type,
    review_id,
    review as orig_review,
    trans_review,
    CASE
    WHEN LENGTH(trans_review)=0 THEN 0
    ELSE LENGTH(trans_review)-LENGTH(REPLACE(trans_review," ",""))+1
    END AS words_count,
    CHAR_LENGTH(trans_review) AS characters_count,
    rating,
    review_date,
    modified_dimension as dimension,
    modified_label as label,
    terms,
    relevance,
    rel_relevance,
    polarity,
    modified_polarity as sentiment_score

FROM 
`{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
;



######## review_raw_data:
CREATE OR REPLACE VIEW `{project}.{datamart}.review_raw_data` AS

SELECT 
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    review_id,
    user_name,
    review as orig_review,
    language,
    trans_review,
    rating,
    review_date

FROM 
`{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
GROUP BY 
    case_study_id,
    case_study_name,
    company_name,
    source_name,
    company_id, 
    source_id,
    review_id,
    user_name,
    language,
    review,
    trans_review,
    rating,
    review_date;


"""

