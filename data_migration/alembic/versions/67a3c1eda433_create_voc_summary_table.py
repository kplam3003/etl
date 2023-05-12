"""create_voc_summary_table

Revision ID: 67a3c1eda433
Revises: ad5eb77e70de
Create Date: 2021-04-13 04:19:51.789522

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
revision = '67a3c1eda433'
down_revision = 'ad5eb77e70de'
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

query_string1 = f"""
         DROP  VIEW `{project}.{datamart}.summary_table`;
        """
query_string2 = f"""
            CREATE OR REPLACE TABLE `{project}.{datamart}.summary_table` 
            PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,1))  AS 
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
                SELECT distinct 
                case_study_id,
                run_id, 
                abs_relevance_inf, 
                rel_relevance_inf 
                FROM  `{project}.{dwh}.casestudy_company_source`
                
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

            AND (CASE WHEN rel_relevance_inf > 0 THEN a.rel_relevance >= rel_relevance_inf ELSE 1=1 END);
        """

def upgrade():
    ##########  CREATE summary_table TABLE IN Datamart#################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nDrop view summary_table in Datamart successfull!")
    time.sleep(15)
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\nCreating table summary_table in Datamart successfull!")

query_string3 = f"""
        DROP  TABLE `{project}.{datamart}.summary_table`;
        """ 
query_string4 = f"""
        CREATE OR REPLACE VIEW `{project}.{datamart}.summary_table` AS
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
            SELECT distinct 
            case_study_id,
            run_id, 
            abs_relevance_inf, 
            rel_relevance_inf 
            FROM `{project}.{dwh}.casestudy_company_source`
            
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
        ;
        """ 

def downgrade():
    query_job =bqclient.query(query_string3)
    query_job .result()
    print("\nDrop table summary_table in Datamart successfull!")
    time.sleep(15)

    query_job =bqclient.query(query_string4)
    query_job .result()
    print("\n Downgrade ad5eb77e70de process is successfull!")

