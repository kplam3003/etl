"""create voe_dimension_config_statistic

Revision ID: 0c5d144774a0
Revises: 980a225d2e2f
Create Date: 2021-04-14 10:17:37.080914

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
revision = '0c5d144774a0'
down_revision = '980a225d2e2f'
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
    """create voe_dimension_config_statistic"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Create voe_dimension_config_statistic in Datamart successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 980a225d2e2f process is successfull!")
    
    
query_string1 = f"""
        CREATE OR REPLACE VIEW `{project}.{datamart}.voe_dimension_config_statistic` AS
        WITH summary as( SELECT
            created_at,
            case_study_id,
            case_study_name,
            source_id,
            source_name,
            company_id,
            company_name,
            nlp_pack,
            nlp_type,
            user_name,
            dimension_config_id,
            dimension_config_name,
            review_id,
            review,
            trans_review,
            trans_status,
            review_date,
            rating,
            language_code,
            language,
            code,
            dimension_type,
            dimension,
            modified_dimension,
            label,
            modified_label,
            is_used,
            terms,
            relevance,
            rel_relevance,
            polarity,
            modified_polarity,
            batch_id,
            batch_name,
            run_id
        FROM
            `{project}.{datamart}.voe_summary_table`
        WHERE
            is_used = True
        
        )


        SELECT
            case_study_id,
            case_study_name,
            dimension_config_id,
            dimension_config_name,
            nlp_type,
            nlp_pack,
            dimension,
            modified_dimension,
            label,
            modified_label,
            count(
                distinct CASE
                    WHEN dimension is null THEN null
                    ELSE review_id
                END
            ) as customer_review_processed,

            (select count (distinct review_id) from summary 
            where case_study_id = a.case_study_id and dimension = a.dimension
            and dimension_config_id = a.dimension_config_id
            and nlp_type = a.nlp_type
            and nlp_pack = a.nlp_pack   
            
            ) as dim_customer_review_processed,

            is_used as used_for_analysis,
            dimension_type
        FROM
        summary a
        
        GROUP BY
            case_study_id,
            case_study_name,
            dimension_config_id,
            dimension_config_name,
            nlp_type,
            nlp_pack,
            dimension,
            modified_dimension,
            label,
            modified_label,
            is_used,
            dimension_type
        ;

        """

query_string2 = f"""
        DROP VIEW `{project}.{datamart}.voe_dimension_config_statistic` ;

        """

