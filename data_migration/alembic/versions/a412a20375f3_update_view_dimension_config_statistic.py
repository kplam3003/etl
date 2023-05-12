"""update_view_dimension_config_statistic


Revision ID: a412a20375f3
Revises: 1ba6de256065
Create Date: 2021-05-31 13:59:01.500300

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
revision = 'a412a20375f3'
down_revision = '1ba6de256065'
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
  
    print("\n Update dimension_config_statistic in Datamart successfull!")

def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 1ba6de256065  process is successfull!")
    
    
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.dimension_config_statistic` AS
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
	`{project}.{datamart}.summary_table`
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
FROM summary a
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
CREATE OR REPLACE VIEW `{project}.{datamart}.dimension_config_statistic` AS
WITH run_id_t as (
    SELECT
        *,
        row_number() over (
            partition by case_study_id
            order by
                created_at desc,
                case_study_id desc
        ) as rank
    FROM  `{project}.{staging}.case_study_run_id`
)
,summary as( SELECT
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
    `{project}.{dwh}.summary_table_origin`
WHERE
    run_id in (
        SELECT
            run_id
        FROM
            run_id_t
        WHERE
            rank = 1
    )
    
    AND is_used = True
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
    dimension_type;

"""


