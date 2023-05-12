"""convert view VOC_5 to table VOC_5

Revision ID: e0465db5a413
Revises: b5ffcc35fea7
Create Date: 2021-02-23 02:50:29.070585

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
revision = 'e0465db5a413'
down_revision = 'b5ffcc35fea7'
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
    """convert view VOC_6_5 to table VOC_6_5 and replace tab by \t"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade 3653cf96fd31 process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 3653cf96fd31 process is successfull!")
    
query_string1 = f"""

##### CREATE TABLE 5 FROM datamart.summary_table:


CREATE OR REPLACE TABLE `{project}.{datamart}.VOC_5_heatmapdim_table` 
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
AS
WITH tmp AS (
    SELECT
        case_study_id,
        case_study_name,
        dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        source_name,
        company_name,
        company_id,
        source_id,
        review_id,
        review_date,
        modified_dimension,
        modified_label,
        split(terms, '\t') AS single_terms,
        polarity,
        modified_polarity,
        run_id
    FROM
        `{project}.{datamart}.summary_table`
    WHERE
        dimension is not null
        AND is_used = true
),
single AS (
    SELECT
        *
    EXCEPT
(single_terms),
        single_terms
    FROM
        tmp,
        UNNEST(single_terms) AS single_terms
),
count_terms AS (
    SELECT
        case_study_id,
        max(case_study_name) case_study_name,
        max(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        max(source_name) source_name,
        max(company_name) company_name,
        company_id,
        source_id,
        review_date as daily_date,
        modified_dimension as dimension,
        modified_label as label,
        single_terms,
        polarity,
        count(distinct review_id) as collected_review_count,
        a.run_id
    FROM
        single a
    WHERE
        a.dimension_config_id is not null
    GROUP BY
        a.case_study_id,
        a.dimension_config_id,
        a.nlp_type,
        a.nlp_pack,
        a.company_id,
        a.source_id,
        a.modified_dimension,
        a.modified_label,
        single_terms,
        polarity,
        daily_date,
        a.run_id
),
count_topic AS (
    SELECT
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        source_id,
        modified_dimension as dimension,
        modified_label as label,
        polarity,
        count(distinct review_id) topic_review_counts,
        review_date as daily_date,
        run_id
    FROM
        `{project}.{datamart}.summary_table`
    WHERE
        dimension is not null 
        AND is_used = true
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        source_id,
        modified_dimension,
        modified_label,
        polarity,
        daily_date,
        run_id
),
count_dimension AS (
    SELECT
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        source_id,
        modified_dimension as dimension,
        sum(
            CASE
                WHEN modified_polarity is null THEN null
                ELSE modified_polarity
            END
        ) as sum_ss,
        count(
            CASE
                WHEN (polarity != 'NONE')
                AND (polarity is not null) THEN review_id
                ELSE null
            END
        ) as sum_review_counts,
        review_date as daily_date,
        run_id
    FROM
        `{project}.{datamart}.summary_table`
    WHERE
        dimension is not null 
        AND is_used = true
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        source_id,
        modified_dimension,
        daily_date,
        run_id
),
count_records AS (
    SELECT
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        source_id,
        count(distinct review_id) as records,
        review_date as daily_date,
        run_id
    FROM
        `{project}.{datamart}.summary_table`
    WHERE
        dimension is not null   
        AND is_used = true
    
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        source_id,
        daily_date,
        run_id
)
select
    a.*,
    topic_review_counts,
    sum_ss,
    sum_review_counts,
    records
FROM
    count_terms a
    LEFT JOIN count_topic b on a.case_study_id = b.case_study_id
        and a.dimension_config_id = b.dimension_config_id
        and a.nlp_type = b.nlp_type
        and a.nlp_pack = b.nlp_pack
        and a.company_id = b.company_id
        and a.source_id = b.source_id
        and a.dimension = b.dimension
        and a.label = b.label
        and a.polarity = b.polarity
        and date(a.daily_date) = date(b.daily_date)
        and a.run_id = b.run_id
    LEFT JOIN count_dimension c on a.case_study_id = c.case_study_id
        and a.dimension_config_id = c.dimension_config_id
        and a.nlp_type = c.nlp_type
        and a.nlp_pack = c.nlp_pack
        and a.company_id = c.company_id
        and a.source_id = c.source_id
        and a.dimension = c.dimension
        and date(a.daily_date) = date(c.daily_date)
        and a.run_id = c.run_id
    LEFT JOIN count_records d on a.case_study_id = d.case_study_id
        and a.dimension_config_id = d.dimension_config_id
        and a.nlp_type = d.nlp_type
        and a.nlp_pack = d.nlp_pack
        and a.company_id = d.company_id
        and a.source_id = d.source_id
        and date(a.daily_date) = date(d.daily_date)
        and a.run_id = d.run_id

    ;
### CREATE VIEW 5 BASED ON run_id:


CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_5_heatmapdim` AS

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

SELECT * except(run_id) FROM `{project}.{datamart}.VOC_5_heatmapdim_table`
WHERE
    run_id in (
        SELECT
            run_id
        FROM
            run_id_t
        WHERE
            rank = 1
    )
;

"""
query_string2 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_5_heatmapdim` AS
WITH tmp AS (
    SELECT
        case_study_id,
        case_study_name,
        dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        source_name,
        company_name,
        company_id,
        source_id,
        review_id,
        review_date,
        modified_dimension,
        modified_label,
        split(terms, '	') AS single_terms,
        polarity,
        modified_polarity
    FROM
        `{project}.{datamart}.summary_table`
    WHERE
        dimension is not null
        AND is_used = true
),
single AS (
    SELECT
        *
    EXCEPT
(single_terms),
        single_terms
    FROM
        tmp,
        UNNEST(single_terms) AS single_terms
),
count_terms AS (
    SELECT
        case_study_id,
        max(case_study_name) case_study_name,
        max(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        max(source_name) source_name,
        max(company_name) company_name,
        company_id,
        source_id,
        review_date as daily_date,
        modified_dimension as dimension,
        modified_label as label,
        single_terms,
        polarity,
        count(distinct review_id) as collected_review_count
    FROM
        single a
    WHERE
        a.dimension_config_id is not null
    GROUP BY
        a.case_study_id,
        a.dimension_config_id,
        a.nlp_type,
        a.nlp_pack,
        a.company_id,
        a.source_id,
        a.modified_dimension,
        a.modified_label,
        single_terms,
        polarity,
        daily_date
),
count_topic AS (
    SELECT
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        source_id,
        modified_dimension as dimension,
        modified_label as label,
        polarity,
        count(distinct review_id) topic_review_counts,
        review_date as daily_date
    FROM
        `{project}.{datamart}.summary_table`
    WHERE
        dimension is not null 
        AND is_used = true
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        source_id,
        modified_dimension,
        modified_label,
        polarity,
        daily_date
),
count_dimension AS (
    SELECT
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        source_id,
        modified_dimension as dimension,
        sum(
            CASE
                WHEN modified_polarity is null THEN null
                ELSE modified_polarity
            END
        ) as sum_ss,
        count(
            CASE
                WHEN (polarity != 'NONE')
                AND (polarity is not null) THEN review_id
                ELSE null
            END
        ) as sum_review_counts,
        review_date as daily_date
    FROM
        `{project}.{datamart}.summary_table`
    WHERE
        dimension is not null 
        AND is_used = true
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        source_id,
        modified_dimension,
        daily_date
),
count_records AS (
    SELECT
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        source_id,
        count(distinct review_id) as records,
        review_date as daily_date
    FROM
        `{project}.{datamart}.summary_table`
    WHERE
        dimension is not null   
        AND is_used = true     
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        source_id,
        daily_date
)
select
    a.*,
    topic_review_counts,
    sum_ss,
    sum_review_counts,
    records
FROM
    count_terms a
    LEFT JOIN count_topic b on a.case_study_id = b.case_study_id
    and a.dimension_config_id = b.dimension_config_id
    and a.nlp_type = b.nlp_type
    and a.nlp_pack = b.nlp_pack
    and a.company_id = b.company_id
    and a.source_id = b.source_id
    and a.dimension = b.dimension
    and a.label = b.label
    and a.polarity = b.polarity
    and date(a.daily_date) = date(b.daily_date)
    LEFT JOIN count_dimension c on a.case_study_id = c.case_study_id
    and a.dimension_config_id = c.dimension_config_id
    and a.nlp_type = c.nlp_type
    and a.nlp_pack = c.nlp_pack
    and a.company_id = c.company_id
    and a.source_id = c.source_id
    and a.dimension = c.dimension
    and date(a.daily_date) = date(c.daily_date)
    LEFT JOIN count_records d on a.case_study_id = d.case_study_id
    and a.dimension_config_id = d.dimension_config_id
    and a.nlp_type = d.nlp_type
    and a.nlp_pack = d.nlp_pack
    and a.company_id = d.company_id
    and a.source_id = d.source_id
    and date(a.daily_date) = date(d.daily_date)

    ;

############

DROP TABLE `{project}.{datamart}.VOC_5_heatmapdim_table` ;

"""