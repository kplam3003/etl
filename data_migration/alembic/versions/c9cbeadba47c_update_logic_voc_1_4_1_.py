"""update_logic_VOC_1_4_1_cusrevtimecountry_v

Revision ID: c9cbeadba47c
Revises: 08d467fd4c96
Create Date: 2021-10-26 14:17:40.912458

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
revision = 'c9cbeadba47c'
down_revision = '08d467fd4c96'
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
    
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n update logic view VOC_1_4_1_cusrevtimecountry_v successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade VOC_1_4_1_cusrevtimecountry_v process is successfull!")

query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_4_1_cusrevtimecountry_v` AS
WITH 
BATCH_LIST AS (
    SELECT
        batch_id
    FROM
        `{project}.{staging}.batch_status`
    WHERE
        status = 'Active'
),
country_code AS (
    SELECT
        DISTINCT case_study_id,
        review_id,
        CASE
            WHEN review_country is null
            OR review_country = 'Unknown'
            THEN 'blank'
            ELSE review_country
        END AS country_name,
        CASE
            WHEN country_code is null
            OR country_code = '' then 'blank'
            ELSE country_code
        END AS country_code
    FROM
        `{project}.{staging}.review_country_mapping`
    WHERE
        batch_id in (
            SELECT
                batch_id
            FROM
                BATCH_LIST
        )
),
date_case_study AS (
    SELECT
        case_study_id,
        max(case_study_name) case_study_name,
        dimension_config_id,
        max(dimension_config_name) dimension_config_name,
        nlp_type,
        nlp_pack,
        max(review_date) as max_date,
        min(review_date) as min_date,
        GENERATE_DATE_ARRAY(min(review_date), max(review_date)) as day
    FROM
        `{project}.{datamart}.summary_table`
    WHERE
        dimension_config_name is not null
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack
),
date_company as (
    SELECT
        a.case_study_id,
        max(case_study_name) case_study_name,
        max(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        ct.country_name,
        ct.country_code,
        max(company_name) company_name,
        (
            SELECT
                max(max_date)
            FROM
                date_case_study
            WHERE
                case_study_id = a.case_study_id
                AND dimension_config_id = a.dimension_config_id
                AND nlp_pack = a.nlp_pack
                AND nlp_type = a.nlp_type
        ) as max_date,
        (
            SELECT
                min(min_date)
            FROM
                date_case_study
            WHERE
                case_study_id = a.case_study_id
                AND dimension_config_id = a.dimension_config_id
                AND nlp_pack = a.nlp_pack
                AND nlp_type = a.nlp_type
        ) as min_date
    FROM
        `{project}.{datamart}.summary_table` a
        LEFT JOIN country_code ct
        ON a.case_study_id = ct.case_study_id
        AND a.review_id = ct.review_id
    WHERE
        dimension_config_name is not null
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        country_name,
        country_code
),
date_info as (
    SELECT
        case_study_id,
        case_study_name,
        dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        company_name,
        country_name,
        country_code,     
        min_date,
        max_date,
        GENERATE_DATE_ARRAY(min_date, max_date) as day
    FROM
        date_company
),
date_range AS (
    SELECT
        *
    EXCEPT
        (day),
        day
    FROM
        date_info,
        UNNEST(day) AS day
),
parent_review AS (
    SELECT
        DISTINCT case_study_id,
        review_id,
        parent_review_id,
        technical_type
    FROM
        `{project}.{staging}.parent_review_mapping`
    WHERE
        batch_id IN (
            SELECT
                batch_id
            FROM
                BATCH_LIST
        )
),
data AS(
    SELECT
        a.case_study_id,
        max(case_study_name) case_study_name,
        max(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        max(company_name) company_name,
        company_id,
        max(ct.country_name) country_name,
        max(ct.country_code) country_code,
        review_date,
        count(distinct parent_review_id) AS records,
        count(distinct parent_review_id) as collected_review_count
    FROM
        `{project}.{datamart}.summary_table` a
        LEFT JOIN parent_review p ON a.case_study_id = p.case_study_id
        AND a.review_id = p.review_id
        LEFT JOIN country_code ct ON a.case_study_id = ct.case_study_id
        AND a.review_id = ct.review_id
    WHERE
        dimension_config_name is not null
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        review_date
)
,
final AS (
    SELECT
        d.case_study_id,
        d.case_study_name,
        d.company_name,
        d.company_id,
        d.dimension_config_name,
        d.dimension_config_id,
        d.nlp_type,
        d.nlp_pack,
        d.day as daily_date,
        dt.records,
        dt.collected_review_count,
        d.country_name,
        d.country_code
    FROM
        date_range as d
        LEFT JOIN data dt ON d.case_study_id = dt.case_study_id
        AND d.dimension_config_id = dt.dimension_config_id
        AND d.nlp_pack = dt.nlp_pack
        AND d.nlp_type = dt.nlp_type
        AND d.company_id = dt.company_id
        AND d.day = dt.review_date
        and d.country_code = dt.country_code
)
SELECT
    case_study_id,
    case_study_name,
    dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_name,
    company_id,
    daily_date,
    records,
    collected_review_count AS records_daily,
    country_name,
    country_code,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
        ) < 7 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA7,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 13 PRECEDING
                AND CURRENT ROW
        ) < 14 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 13 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA14,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 29 PRECEDING
                AND CURRENT ROW
        ) < 30 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 29 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA30,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 59 PRECEDING
                AND CURRENT ROW
        ) < 60 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 59 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA60,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 89 PRECEDING
                AND CURRENT ROW
        ) < 90 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 89 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA90,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 119 PRECEDING
                AND CURRENT ROW
        ) < 120 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 119 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA120,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 149 PRECEDING
                AND CURRENT ROW
        ) < 150 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 149 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA150,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 179 PRECEDING
                AND CURRENT ROW
        ) < 180 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 179 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA180
FROM
    final;
"""
        
query_string2 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_4_1_cusrevtimecountry_v` AS 
WITH date_case_study as (
    SELECT
        case_study_id,
        max(case_study_name) case_study_name,
        dimension_config_id,
        max(dimension_config_name) dimension_config_name,
        nlp_type,
        nlp_pack,
        max(review_date) as max_date,
        min(review_date) as min_date,
        GENERATE_DATE_ARRAY(min(review_date), max(review_date)) as day
    FROM
        `{project}.{datamart}.summary_table`
    WHERE
        dimension_config_name is not null
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack
),
date_company as (
    SELECT
        case_study_id,
        max(case_study_name) case_study_name,
        max(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        max(company_name) company_name,
        (
            SELECT
                max(max_date)
            FROM
                date_case_study
            WHERE
                case_study_id = a.case_study_id
                AND dimension_config_id = a.dimension_config_id
                AND nlp_pack = a.nlp_pack
                AND nlp_type = a.nlp_type
        ) as max_date,
        (
            SELECT
                min(min_date)
            FROM
                date_case_study
            WHERE
                case_study_id = a.case_study_id
                AND dimension_config_id = a.dimension_config_id
                AND nlp_pack = a.nlp_pack
                AND nlp_type = a.nlp_type
        ) as min_date
    FROM
        `{project}.{datamart}.summary_table` a
    WHERE
        dimension_config_name is not null
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id
),
date_info as (
    SELECT
        case_study_id,
        case_study_name,
        dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        company_name,
        min_date,
        max_date,
        GENERATE_DATE_ARRAY(min_date, max_date) as day
    FROM
        date_company
),
date_range as (
    SELECT
        *
    EXCEPT
        (day),
        day
    FROM
        date_info,
        UNNEST(day) AS day
),
BATCH_LIST as (
    SELECT
        batch_id
    FROM
        `{project}.{staging}.batch_status`
    WHERE
        status = 'Active'
),
parent_review as (
    SELECT
        DISTINCT case_study_id,
        review_id,
        parent_review_id,
        technical_type
    FROM
        `{project}.{staging}.parent_review_mapping`
    WHERE
        batch_id IN (
            SELECT
                batch_id
            FROM
                BATCH_LIST
        )
),
country_code as (
    SELECT
        distinct case_study_id,
        review_id,
        case
            WHEN review_country is null
            or review_country = 'Unknown' THEN 'blank'
            else review_country
        end as country_name,
        case
            when country_code is null
            or country_code = 'Unknown' then 'blank'
            else country_code
        end as country_code
    FROM
        `{project}.{staging}.review_country_mapping`
    WHERE
        batch_id in (
            SELECT
                batch_id
            FROM
                BATCH_LIST
        )
),
data as(
    SELECT
        a.case_study_id,
        max(case_study_name) case_study_name,
        max(dimension_config_name) dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        max(company_name) company_name,
        company_id,
        max(ct.country_name) country_name,
        max(ct.country_code) country_code,
        review_date,
        count(distinct parent_review_id) AS records,
        count(distinct parent_review_id) as collected_review_count
    FROM
        `{project}.{datamart}.summary_table` a
    LEFT JOIN parent_review p
        ON a.case_study_id = p.case_study_id
        AND a.review_id = p.review_id
    LEFT JOIN country_code ct
        ON a.case_study_id = ct.case_study_id
        AND a.review_id = ct.review_id
    WHERE
        dimension_config_name is not null
    GROUP BY
        case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        review_date
),
final as (
    SELECT
        d.case_study_id,
        d.case_study_name,
        d.company_name,
        d.company_id,
        d.dimension_config_name,
        d.dimension_config_id,
        d.nlp_type,
        d.nlp_pack,
        d.day as daily_date,
        dt.records,
        dt.collected_review_count,
        dt.country_name,
        dt.country_code
    FROM
        date_range as d
        LEFT JOIN data dt ON d.case_study_id = dt.case_study_id
        AND d.dimension_config_id = dt.dimension_config_id
        AND d.nlp_pack = dt.nlp_pack
        AND d.nlp_type = dt.nlp_type
        AND d.company_id = dt.company_id
        AND d.day = dt.review_date
)
SELECT
    case_study_id,
    case_study_name,
    dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_name,
    company_id,
    daily_date,
    records,
    collected_review_count AS records_daily,
    country_name,
    country_code,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
        ) < 7 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA7,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 13 PRECEDING
                AND CURRENT ROW
        ) < 14 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 13 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA14,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 29 PRECEDING
                AND CURRENT ROW
        ) < 30 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 29 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA30,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 59 PRECEDING
                AND CURRENT ROW
        ) < 60 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 59 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA60,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 89 PRECEDING
                AND CURRENT ROW
        ) < 90 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 89 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA90,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 119 PRECEDING
                AND CURRENT ROW
        ) < 120 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 119 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA120,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 149 PRECEDING
                AND CURRENT ROW
        ) < 150 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 149 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA150,
    CASE
        WHEN COUNT(daily_date) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 179 PRECEDING
                AND CURRENT ROW
        ) < 180 THEN NULL
        ELSE SUM(collected_review_count) OVER (
            PARTITION BY case_study_id,
            dimension_config_id,
            nlp_type,
            nlp_pack,
            company_id,
            country_name,
            country_code
            ORDER BY
                daily_date ROWS BETWEEN 179 PRECEDING
                AND CURRENT ROW
        )
    END AS CR_MA180
FROM
    final;
"""
