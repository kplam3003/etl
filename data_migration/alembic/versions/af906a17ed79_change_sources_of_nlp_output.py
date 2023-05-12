"""change_sources_of_nlp_output

Revision ID: af906a17ed79
Revises: bb0b529980ad
Create Date: 2021-02-01 06:59:00.312402

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
revision = 'af906a17ed79'
down_revision = 'bb0b529980ad'
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
    """change_sources_of_nlp_output"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")
    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.nlp_output` 
AS

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
`{project}.{datamart}.summary_table`
WHERE dimension_config_name is not null;


"""

query_string2 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.nlp_output` 
AS

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
WHERE dimension_config_name is not null;

"""