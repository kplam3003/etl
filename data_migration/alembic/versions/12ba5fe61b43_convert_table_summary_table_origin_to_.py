"""convert table summary_table_origin to individual cs tables

Revision ID: 12ba5fe61b43
Revises: be144eb1d8fa
Create Date: 2021-04-13 09:47:21.174542

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
# import pyarrrow
# revision identifiers, used by Alembic.
revision = '12ba5fe61b43'
down_revision = '1d0ef651ad6f'
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
    """convert table summary_table_origin to individual cs tables"""
    #extract list of case_study_id in summary_table_origin:
    # cs_list=bqclient.query(query_string1).to_dataframe()
    # case_study_list = cs_list['case_study_id'].unique()

    query_results = bqclient.query(query_string1).result()
    case_study_list = set()
    for row in query_results:
        case_study_list.add(row.case_study_id)

    case_study_list = list(case_study_list)
    if len(case_study_list) == 0:
        query_string2 = f"""
        CREATE OR REPLACE TABLE `{project}.{datamart_cs}.summary_table_tmp` 

        ( 
        created_at	timestamp	,
        case_study_id	int64	,
        case_study_name	string	,
        source_id	int64	,
        source_name	string	,
        company_id	int64	,
        company_name	string	,
        nlp_pack	string	,
        nlp_type	string	,
        user_name	string	,
        dimension_config_id	int64	,
        dimension_config_name	string	,
        review_id	string	,
        review	string	,
        trans_review	string	,
        trans_status	string	,
        review_date	date	,
        rating	float64	

        )

        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1))

        """
        query_job =bqclient.query(query_string2)
        query_job.result()
        print(f"summary_table_tmp is created successfully!")
    else:

        #create small summary_table:
        for cs in case_study_list:
            query_string2 = f"""
            CREATE OR REPLACE TABLE `{project}.{datamart_cs}.summary_table_{cs}` 
            PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1))
            AS
            WITH run_id_t as (
                SELECT
                    *,
                    row_number() over (
                        partition by case_study_id
                        order by
                            created_at desc,
                            case_study_id desc
                    ) as rank
                FROM
                    `{project}.{staging}.case_study_run_id` 
            )

            SELECT
                created_at,
                a.case_study_id,
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
                CASE 
                    WHEN language_code is null or language_code = '' THEN 'blank'
                    ELSE language_code
                END as language_code,
                CASE 
                    WHEN language is null AND (language_code is null or language_code = '') THEN 'blank'
                    WHEN language is null AND language_code is not null THEN language_code
                    ELSE language
                END as language,
                code,
                dimension_type,
                dimension,
                modified_dimension,
                label,
                modified_label,
                is_used,
                terms,
                a.relevance,
                a.rel_relevance,
                polarity,
                modified_polarity,
                batch_id,
                batch_name,
                a.run_id,
                abs_relevance_inf,
                rel_relevance_inf
            

            FROM
                `{project}.{dwh}.summary_table_origin` a
            LEFT JOIN (

                select distinct case_study_id,run_id, abs_relevance_inf, rel_relevance_inf from `{project}.{dwh}.casestudy_company_source` 

                
            ) b
            ON a.case_study_id = b.case_study_id
            AND a.run_id = b.run_id
            WHERE
                a.run_id in (
                    SELECT
                        run_id
                    FROM
                        run_id_t
                    WHERE
                        rank = 1
                )


            AND (CASE WHEN abs_relevance_inf > 0 THEN a.relevance >= abs_relevance_inf ELSE 1=1 END)

            AND (CASE WHEN rel_relevance_inf > 0 THEN a.rel_relevance >= rel_relevance_inf ELSE 1=1 END)
            AND a.case_study_id = {cs}

            ;  
            """
            query_job =bqclient.query(query_string2)
            query_job.result()
            print(f"summary_table_{cs} is created successfully!")
    print("\n Upgrade 12ba5fe61b43 process is successfull!")
    
def downgrade():
    #extract list of case_study_id in summary_table_origin:
    
    query_results = bqclient.query(query_string1).result()
    case_study_list = set()
    for row in query_results:
        case_study_list.add(row.case_study_id)

    case_study_list = list(case_study_list)

    if len(case_study_list) == 0:
        pass
    else:

        #create small summary_table:
        for cs in case_study_list:
            query_string3 = f"""
            DROP TABLE `{project}.{datamart_cs}.summary_table_{cs}` ;
            """
            query_job =bqclient.query(query_string3)
            query_job.result()
            print(f"summary_table_{cs} is dropped successfully!")
    
    print("\n Downgrade 12ba5fe61b43 process is successfull!")

query_string1 = f"""
    SELECT distinct case_study_id FROM `{project}.{dwh}.summary_table_origin`;   
"""



