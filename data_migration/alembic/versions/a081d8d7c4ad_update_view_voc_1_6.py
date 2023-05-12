"""update_view_voc_1_6

Revision ID: a081d8d7c4ad
Revises: 1cbe2d820048
Create Date: 2021-07-15 12:24:08.075415

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
revision = 'a081d8d7c4ad'
down_revision = '1cbe2d820048'
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
    print("\n Update view VOC_1_6_cusrevtimesource_v successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 1cbe2d820048 process is successfull!")
	
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_6_cusrevtimesource_v` AS 
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
	source_id,
	max(source_name) source_name,
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
	source_id,
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
	source_id,
	source_name,
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
        
) ,
parent_review as (
SELECT 
DISTINCT 
case_study_id,
review_id,
parent_review_id,
technical_type
FROM  `{project}.{staging}.parent_review_mapping`
WHERE
        batch_id IN (
            SELECT
                batch_id FROM BATCH_LIST
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
	source_id,
	max(source_name) source_name,
	company_id,
	max(company_name) company_name,
	review_date,
	count(distinct parent_review_id) AS records,
	count(distinct parent_review_id) as collected_review_count
FROM
	`{project}.{datamart}.summary_table` a
LEFT JOIN parent_review p
    ON a.case_study_id = p.case_study_id
    AND a.review_id = p.review_id
WHERE
	dimension_config_name is not null 
GROUP BY
	case_study_id,
	dimension_config_id,
	nlp_type,
	nlp_pack,
	source_id,
	company_id,
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
	d.source_id,
	d.source_name,
	d.company_id,
	d.company_name,
	d.day as daily_date,
	dt.records,
	dt.collected_review_count
FROM
	date_range as d
	LEFT JOIN data dt ON d.case_study_id = dt.case_study_id
	AND d.dimension_config_id = dt.dimension_config_id
	AND d.nlp_pack = dt.nlp_pack
	AND d.nlp_type = dt.nlp_type
	AND d.company_id = dt.company_id
	AND d.source_id = dt.source_id
	AND d.day = dt.review_date
)
SELECT
case_study_id,
case_study_name,
dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
source_id,
source_name,
company_name,
company_id,
daily_date,
records,
collected_review_count AS records_daily,
CASE
	WHEN COUNT(daily_date) OVER (
		PARTITION BY case_study_id,
		dimension_config_id,
		nlp_type,
		nlp_pack,
		company_id,
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
		ORDER BY
			daily_date ROWS BETWEEN 179 PRECEDING
			AND CURRENT ROW
	)
END AS CR_MA180
FROM
final;
"""



	
query_string2 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_6_cusrevtimesource_v` AS 
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
	source_id,
	max(source_name) source_name,
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
	source_id,
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
	source_id,
	source_name,
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
data as(
SELECT
	case_study_id,
	max(case_study_name) case_study_name,
	max(dimension_config_name) dimension_config_name,
	dimension_config_id,
	nlp_type,
	nlp_pack,
	source_id,
	max(source_name) source_name,
	company_id,
	max(company_name) company_name,
	review_date,
	count(distinct review_id) AS records,
	count(distinct review_id) as collected_review_count
FROM
	`{project}.{datamart}.summary_table` a
WHERE
	dimension_config_name is not null 
GROUP BY
	case_study_id,
	dimension_config_id,
	nlp_type,
	nlp_pack,
	source_id,
	company_id,
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
	d.source_id,
	d.source_name,
	d.company_id,
	d.company_name,
	d.day as daily_date,
	dt.records,
	dt.collected_review_count
FROM
	date_range as d
	LEFT JOIN data dt ON d.case_study_id = dt.case_study_id
	AND d.dimension_config_id = dt.dimension_config_id
	AND d.nlp_pack = dt.nlp_pack
	AND d.nlp_type = dt.nlp_type
	AND d.company_id = dt.company_id
	AND d.source_id = dt.source_id
	AND d.day = dt.review_date
)
SELECT
case_study_id,
case_study_name,
dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
source_id,
source_name,
company_name,
company_id,
daily_date,
records,
collected_review_count AS records_daily,
CASE
	WHEN COUNT(daily_date) OVER (
		PARTITION BY case_study_id,
		dimension_config_id,
		nlp_type,
		nlp_pack,
		company_id,
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
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
		source_id
		ORDER BY
			daily_date ROWS BETWEEN 179 PRECEDING
			AND CURRENT ROW
	)
END AS CR_MA180
FROM
final;
"""
		

