"""update_polarity_voe_6_1

Revision ID: 8c7631c2d403
Revises: f6c0288a2208
Create Date: 2021-08-02 10:07:18.324009

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
revision = '8c7631c2d403'
down_revision = 'f6c0288a2208'
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
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Update view VOE_6_1_sstimedimcompany successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade f6c0288a2208 process is successfull!")



query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_6_1_sstimedimcompany` AS 
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
    `{project}.{datamart}.voe_summary_table`
WHERE
    dimension_config_name is not null
    and dimension is not null 
    AND is_used = true
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
    modified_dimension as dimension,
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
    `{project}.{datamart}.voe_summary_table` a
WHERE
    dimension_config_name is not null
    AND dimension is not null 
    AND is_used = true
GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id,
    modified_dimension
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
    dimension,
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
data as(
SELECT
    case_study_id,
    max(case_study_name) case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id,
    max(company_name) company_name,
    modified_dimension as dimension,
    review_date,
    count(distinct review_id) AS records,
    count(
        CASE
            WHEN polarity  IN ('N', 'N+', 'NEU', 'P', 'P+') THEN review_id
            ELSE null
        END
    ) as collected_review_count,
    sum(
        CASE
			WHEN polarity  IN ('N', 'N+', 'NEU', 'P', 'P+') THEN modified_polarity
            ELSE null
        END
    ) as sum_modified_polarity
FROM
    `{project}.{datamart}.voe_summary_table` a
WHERE
    dimension_config_name is not null
    and dimension is not null 
    AND is_used = true
GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id,
    modified_dimension,
    review_date
),
final as (
SELECT
    d.case_study_id,
    d.case_study_name,
    d.dimension_config_name,
    d.dimension_config_id,
    d.nlp_type,
    d.nlp_pack,
    d.company_id,
    d.company_name,
    d.dimension,
    d.day as daily_date,
    dt.records,
    dt.collected_review_count,
    dt.sum_modified_polarity
FROM
    date_range as d
    LEFT JOIN data dt ON d.case_study_id = dt.case_study_id
    AND d.dimension_config_id = dt.dimension_config_id
    AND d.nlp_pack = dt.nlp_pack
    AND d.nlp_type = dt.nlp_type
    AND d.company_id = dt.company_id
    AND d.day = dt.review_date
    AND d.dimension = dt.dimension
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
dimension,
daily_date,
records,
collected_review_count AS records_daily,
sum_modified_polarity as ss_daily,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 6 PRECEDING
            AND CURRENT ROW
    ) < 7 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 6 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA7,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 6 PRECEDING
            AND CURRENT ROW
    ) < 7 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 6 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA7,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 13 PRECEDING
            AND CURRENT ROW
    ) < 14 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 13 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA14,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 13 PRECEDING
            AND CURRENT ROW
    ) < 14 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 13 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA14,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 29 PRECEDING
            AND CURRENT ROW
    ) < 30 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 29 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA30,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 29 PRECEDING
            AND CURRENT ROW
    ) < 30 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 29 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA30,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 59 PRECEDING
            AND CURRENT ROW
    ) < 60 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 59 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA60,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 59 PRECEDING
            AND CURRENT ROW
    ) < 60 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 59 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA60,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 89 PRECEDING
            AND CURRENT ROW
    ) < 90 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 89 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA90,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 89 PRECEDING
            AND CURRENT ROW
    ) < 90 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 89 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA90,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 119 PRECEDING
            AND CURRENT ROW
    ) < 120 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 119 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA120,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 119 PRECEDING
            AND CURRENT ROW
    ) < 120 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 119 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA120,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 149 PRECEDING
            AND CURRENT ROW
    ) < 150 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 149 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA150,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 149 PRECEDING
            AND CURRENT ROW
    ) < 150 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 149 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA150,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 179 PRECEDING
            AND CURRENT ROW
    ) < 180 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 179 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA180,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 179 PRECEDING
            AND CURRENT ROW
    ) < 180 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 179 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA180
FROM
final
;
"""


		
query_string2 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_6_1_sstimedimcompany` AS 
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
    `{project}.{datamart}.voe_summary_table`
WHERE
    dimension_config_name is not null
    and dimension is not null 
    AND is_used = true
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
    modified_dimension as dimension,
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
    `{project}.{datamart}.voe_summary_table` a
WHERE
    dimension_config_name is not null
    AND dimension is not null 
    AND is_used = true
GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id,
    modified_dimension
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
    dimension,
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
data as(
SELECT
    case_study_id,
    max(case_study_name) case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id,
    max(company_name) company_name,
    modified_dimension as dimension,
    review_date,
    count(distinct review_id) AS records,
    count(
        CASE
            WHEN polarity not in ('N', 'N+', 'NEU', 'P', 'P+') THEN null
            ELSE review_id
        END
    ) as collected_review_count,
    sum(
        CASE
            WHEN polarity not in ('N', 'N+', 'NEU', 'P', 'P+') THEN null
            ELSE modified_polarity
        END
    ) as sum_modified_polarity
FROM
    `{project}.{datamart}.voe_summary_table` a
WHERE
    dimension_config_name is not null
    and dimension is not null 
    AND is_used = true
GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id,
    modified_dimension,
    review_date
),
final as (
SELECT
    d.case_study_id,
    d.case_study_name,
    d.dimension_config_name,
    d.dimension_config_id,
    d.nlp_type,
    d.nlp_pack,
    d.company_id,
    d.company_name,
    d.dimension,
    d.day as daily_date,
    dt.records,
    dt.collected_review_count,
    dt.sum_modified_polarity
FROM
    date_range as d
    LEFT JOIN data dt ON d.case_study_id = dt.case_study_id
    AND d.dimension_config_id = dt.dimension_config_id
    AND d.nlp_pack = dt.nlp_pack
    AND d.nlp_type = dt.nlp_type
    AND d.company_id = dt.company_id
    AND d.day = dt.review_date
    AND d.dimension = dt.dimension
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
dimension,
daily_date,
records,
collected_review_count AS records_daily,
sum_modified_polarity as ss_daily,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 6 PRECEDING
            AND CURRENT ROW
    ) < 7 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 6 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA7,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 6 PRECEDING
            AND CURRENT ROW
    ) < 7 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 6 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA7,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 13 PRECEDING
            AND CURRENT ROW
    ) < 14 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 13 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA14,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 13 PRECEDING
            AND CURRENT ROW
    ) < 14 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 13 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA14,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 29 PRECEDING
            AND CURRENT ROW
    ) < 30 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 29 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA30,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 29 PRECEDING
            AND CURRENT ROW
    ) < 30 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 29 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA30,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 59 PRECEDING
            AND CURRENT ROW
    ) < 60 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 59 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA60,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 59 PRECEDING
            AND CURRENT ROW
    ) < 60 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 59 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA60,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 89 PRECEDING
            AND CURRENT ROW
    ) < 90 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 89 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA90,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 89 PRECEDING
            AND CURRENT ROW
    ) < 90 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 89 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA90,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 119 PRECEDING
            AND CURRENT ROW
    ) < 120 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 119 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA120,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 119 PRECEDING
            AND CURRENT ROW
    ) < 120 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 119 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA120,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 149 PRECEDING
            AND CURRENT ROW
    ) < 150 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 149 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA150,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 149 PRECEDING
            AND CURRENT ROW
    ) < 150 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 149 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA150,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 179 PRECEDING
            AND CURRENT ROW
    ) < 180 THEN NULL
    ELSE sum(sum_modified_polarity) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 179 PRECEDING
            AND CURRENT ROW
    )
END AS SS_MA180,
CASE
    WHEN COUNT(daily_date) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 179 PRECEDING
            AND CURRENT ROW
    ) < 180 THEN NULL
    ELSE sum(collected_review_count) OVER (
        PARTITION BY case_study_id,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        company_id,
        dimension
        ORDER BY
            daily_date ROWS BETWEEN 179 PRECEDING
            AND CURRENT ROW
    )
END AS RECORDS_MA180
FROM
final
;
"""
