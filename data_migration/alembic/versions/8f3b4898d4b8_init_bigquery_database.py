"""init_bigquery_database

Revision ID: 8f3b4898d4b8
Revises: 039d59cb195a
Create Date: 2021-01-08 18:07:14.139333

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
revision = '8f3b4898d4b8'
down_revision = '41ac3d38ccc0'
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

    ###################################
    ########## Step 1: CREATE DATASET #################

    for database in database_list:
        dataset_id = "{}.{}".format(project,database)    

        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)

        # Specify the geographic location where the dataset should reside.
        dataset.location = "US"

        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
   
        dataset = bqclient.create_dataset(dataset)  # Make an API request.
        print("Created dataset {}.{}".format(project, dataset.dataset_id))

    time.sleep(15)
    
    ###################################
    ########## Step 2: CREATE TABLE IN STAGING AND DWH #################
    
    query_string2 = f"""
    CREATE OR REPLACE table `{project}.{staging}.voc`
    (
    `id` int64,    
    `created_at` timestamp,
    `review_id` string,
    `source_name` string,
    `company_name`  string,
    `nlp_pack` string,
    `nlp_type` string,
    `user_name` string,    
    `language` string,    
    `review` string,
    `trans_review` string,
    `trans_status` string,    
    `code`  string,
    `dimension`  string,
    `label`  string,
    `terms`  string,
    `relevance`  float64,
    `rel_relevance`  float64,   
    `polarity` string,
    `rating` int64,
    `batch_id` int64,
    `batch_name` string,
	`file_name` string,
	`review_date` timestamp,
	`company_id` int64,
	`source_id` int64,
	`step_id` int64,
    `request_id` int64,
    `case_study_id` int64
     
  )

PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) 

;

CREATE OR REPLACE table `{project}.{staging}.batch_status` 

(     
    `created_at` timestamp,
    `batch_id` int64,
     `batch_name` string,
     `source_id` int64,
     `source_name` string,
     `company_id`  int64,
     `company_name`  string,
     `status` string,
     `version` int64
 
     
  )
PARTITION BY RANGE_BUCKET(batch_id, GENERATE_ARRAY(1, 4000,10))

;

CREATE OR REPLACE TABLE `{project}.{dwh}.casestudy_company_source`
(   `created_at` timestamp,
    `case_study_id` int64,
    `case_study_name` string,
    `dimension_config_id` int64,
	 `company_id` int64,
    `company_name` string,
    `source_name`  string,
    `source_id` int64,
    `nlp_pack` string,
    `nlp_type` string,
    `is_target` bool,
    `run_at` timestamp
    )
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
 ;


 CREATE OR REPLACE table `{project}.{dwh}.polarity_trans` 

(   `created_at` timestamp,
    `case_study_id` int64,
    `case_study_name` string,   
    `nlp_polarity` string,
    `modified_polarity` float64

    )
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))

;

CREATE OR REPLACE table `{project}.{dwh}.casestudy_dimension_config` 

(   
    `created_at` timestamp,
    `case_study_id` int64,
    `case_study_name` string,   
    `nlp_pack` string,
    `nlp_type` string,
    `dimension_config_id` int64,
    `dimension_config_name` string,
    `nlp_dimension` string,
    `modified_dimension` string,
    `nlp_label` string,
    `modified_label` string,
    `is_used` bool,
    `dimension_type` string

    )
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))

;


# CREATE OR REPLACE TABLE `{project}.{dwh}.nlp_output`

# (id	            int64	,
# created_at	    timestamp	,
# review_id	    string	,
# source_name	    string	,
# company_name    string	,
# nlp_pack	    string	,
# nlp_type	    string	,
# user_name	    string	,
# language	    string	,
# review	        string	,
# trans_review	string	,
# trans_status	string	,
# code	        string	,
# dimension	    string	,
# label	        string	,
# terms	        string	,
# relevance	    float64	,
# rel_relevance	float64	,
# polarity	    string	,
# rating	        int64	,
# batch_id	    int64	,
# batch_name	    string	,
# file_name	    string	,
# review_date	    timestamp	,
# company_id	    int64	,
# source_id	    int64	,
# step_id	        int64	,
# request_id	    int64
# )

# PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,10))
# ;

CREATE OR REPLACE TABLE `{project}.{dwh}.nlp_output_case_study`

(
created_at	    timestamp	,
review_id	    string	,
source_name	    string	,
company_name	string	,
nlp_pack	    string	,
nlp_type	    string	,
user_name	    string	,
language	    string	,
review	        string	,
trans_review	string	,
trans_status	string	,
code	        string	,
dimension	    string	,
label	        string	,
terms	        string	,
relevance	    float64	,
rel_relevance	float64	,
polarity	    string	,
rating	        int64	,
batch_id	    int64	,
batch_name	    string	,
file_name	    string	,
review_date	    timestamp	,
company_id	    int64	,
source_id	    int64	,
step_id	        int64	,
request_id	    int64	,
is_target	    bool	,
case_study_id	int64	,
case_study_name	string	,
dimension_config_id	int64	
)

PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
;

CREATE OR REPLACE TABLE `{project}.{dwh}.language_trans`

(
language string,
language_code string,
iso_code string
)

;

CREATE OR REPLACE TABLE `{project}.{dwh}.dimension_default`

(
dimension_config_id	    int64	,
nlp_pack	            string	,
nlp_type	            string	,
dimension_config_name	string	,
nlp_dimension	        string	,
modified_dimension	    string	,
nlp_label	            string	,
modified_label	        string	

)

PARTITION BY RANGE_BUCKET( dimension_config_id , GENERATE_ARRAY(1, 4000,10))

;


CREATE OR REPLACE TABLE `{project}.{dwh}.summary_table`

(
created_at	                timestamp	,
case_study_id	            int64	,
case_study_name	            string	,
source_id	                int64	,
source_name	                string	,
company_id	                int64	,
company_name	            string	,
nlp_pack	                string	,
nlp_type	                string	,
user_name	                string	,
dimension_config_id	        int64	,
dimension_config_name       string	,
review_id	                string	,
review	                    string	,
trans_review	            string	,
trans_status	            string	,
review_date	                date	,
rating	                    int64	,
language_code	            string	,
language	                string	,
code	                    string	,
dimension_type	            string	,
dimension	                string	,
modified_dimension	        string	,
label	                    string	,
modified_label	            string	,
is_used	                    bool	,
terms	                    string	,
relevance	                float64	,
rel_relevance	            float64	,
polarity	                string	,
modified_polarity	        float64	,
batch_id	                int64	,
batch_name	                string	


)

PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,5))

;

"""

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\nCreating tables in Staging and DWH is successfull!")

################
################ Step 3: CREATE TABLES IN DATAMART ####################
    query_string3 = f"""

# VIEW nlp_output:


CREATE OR REPLACE VIEW `{project}.{datamart}.nlp_output` 
AS

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
ORDER BY 
case_study_id,
case_study_name,
company_name,
source_name,
company_id, 
source_id,
nlp_pack,
nlp_type,
user_name
;

# VIEW review_raw_data

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
review_date

ORDER BY 
case_study_id,
case_study_name,
company_name,
source_name,
review_date,
review_id,
user_name
;

# VIEW VOC_1_1_cusrevtimecompany_v
CREATE OR REPLACE view `{project}.{datamart}.VOC_1_1_cusrevtimecompany_v` 

AS

WITH table1 AS
(SELECT 
case_study_id,
max(case_study_name) case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
-- max(source_name) source_name,
max(company_name) company_name,
company_id, 
-- source_id,
review_date as daily_date,
count(distinct review_id) as records,
count(distinct review_id) as collected_review_count

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
GROUP BY
case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
-- source_id,
daily_date)

SELECT

case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
-- source_name,
company_name,
company_id, 
-- source_id,
daily_date,
records,
collected_review_count as records_daily,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
 < 7 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
END AS CR_MA7
,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
 < 14 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
END AS CR_MA14,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
 < 30 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
END AS CR_MA30,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
 < 60 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
END AS CR_MA60,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
 < 90 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
END AS CR_MA90,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
 < 120 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
END AS CR_MA120,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
 < 150 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
END AS CR_MA150,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
 < 180 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
END AS CR_MA180

from table1;

# VIEW VOC_1_2_cusrevstat_v

CREATE OR REPLACE view `{project}.{datamart}.VOC_1_2_cusrevstat_v` AS
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
count(distinct review_id) as records,
count(distinct review_id) as collected_review_count,
count(distinct case when dimension is not null  then review_id else null end) as processed_review_count,
count(distinct case when dimension is null  then review_id else null end) as unprocessed_review_count

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
GROUP BY
case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id,
daily_date;

# VIEW VOC_1_3_1_cusrevcompany_p_v

CREATE OR REPLACE view `{project}.{datamart}.VOC_1_3_1_cusrevcompany_p_v` AS
SELECT
case_study_id,
max(case_study_name) case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
-- max(source_name) source_name,
max(company_name) company_name,
company_id, 
-- source_id,
review_date as daily_date,
count(distinct review_id) as records,
count(distinct review_id) as collected_review_count

from `{project}.{dwh}.summary_table`
where dimension_config_name is not null
group by 
case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
-- source_id,
daily_date;


# VIEW VOC_1_3_2_cusrevcompany_b_v
CREATE OR REPLACE view `{project}.{datamart}.VOC_1_3_2_cusrevcompany_b_v`  AS
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
count(distinct review_id) as records,
count(distinct review_id) as collected_review_count

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
GROUP BY
case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id,
daily_date;



# VIEW VOC_1_4_cusrevtimelangue_v
CREATE OR REPLACE view `{project}.{datamart}.VOC_1_4_cusrevtimelangue_v` AS
WITH table1 AS (
SELECT 
case_study_id,
case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
-- max(source_name)source_name,
max(company_name) company_name,
company_id, 
-- source_id,

review_date as daily_date,
language as language_name,
language_code,
count(distinct review_id) as records,
count(distinct review_id) as collected_review_count

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
GROUP BY 
case_study_id,
case_study_name,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
-- source_id,
language_name,
language_code,
daily_date)

SELECT
case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
-- source_name,
company_name,
company_id, 
-- source_id,
language_name,
language_code,
daily_date,
records,
collected_review_count as records_daily,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
 < 7 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
END AS CR_MA7
,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
 < 14 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
END AS CR_MA14,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code

ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
 < 30 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
END AS CR_MA30,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
 < 60 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
END AS CR_MA60,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
 < 90 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
END AS CR_MA90,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
 < 120 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
END AS CR_MA120,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
 < 150 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
END AS CR_MA150,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
 < 180 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
language_name,
language_code
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
END AS CR_MA180

from table1;


# VIEW VOC_1_5_cusrevlangue_p_v

CREATE OR REPLACE view `{project}.{datamart}.VOC_1_5_cusrevlangue_p_v` AS
SELECT 

case_study_id,
case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
-- max(source_name)source_name,
max(company_name) company_name,
company_id, 
-- source_id,
review_date as daily_date,
language as language_name,
language_code,
count(distinct review_id) as records,
count(distinct review_id) as collected_review_count

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
GROUP BY 
case_study_id,
case_study_name,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
-- source_id,
language_name,
language_code,
daily_date;



#VIEW VOC_1_6_cusrevtimesource_v:

CREATE OR REPLACE view `{project}.{datamart}.VOC_1_6_cusrevtimesource_v` AS
WITH table1 AS (
SELECT 
case_study_id,
case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
max(source_name)source_name,
max(company_name) company_name,
company_id, 
source_id,
review_date as daily_date,
count(distinct review_id) as records,
count(distinct review_id) as collected_review_count

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
GROUP BY 
case_study_id,
case_study_name,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id,
daily_date)

select 
case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
source_name,
company_name,
company_id, 
source_id,
daily_date,
records,
collected_review_count,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
 < 7 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
END AS CR_MA7
,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
 < 14 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
END AS CR_MA14,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
 < 30 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
END AS CR_MA30,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
 < 60 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
END AS CR_MA60,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
 < 90 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
END AS CR_MA90,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
 < 120 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
END AS CR_MA120,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
 < 150 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
END AS CR_MA150,

CASE WHEN COUNT(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
 < 180 THEN NULL
ELSE
AVG(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
END AS CR_MA180

from table1
;


#VIEW VOC_1_7_cusrevprocessed_v
CREATE OR REPLACE view `{project}.{datamart}.VOC_1_7_cusrevprocessed_v` AS
SELECT 

case_study_id,
case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
-- max(source_name)source_name,
max(company_name) company_name,
company_id, 
-- source_id,
review_date as daily_date,
count(distinct review_id) as records,
count(distinct review_id) as collected_review_count,
count(distinct case when dimension is not null  then review_id else null end) as processed_review_count

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
GROUP BY
case_study_id,
case_study_name,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
-- source_id,
daily_date;


#VIEW VOC_2_polaritydistr_v

CREATE OR REPLACE view `{project}.{datamart}.VOC_2_polaritydistr_v` AS
SELECT 

case_study_id,
case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
-- max(source_name)source_name,
max(company_name) company_name,
company_id, 
-- source_id,
review_date as daily_date,
polarity,
count(distinct review_id) as records,
count(distinct review_id) as collected_review_count

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
GROUP BY
case_study_id,
case_study_name,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
-- source_id,
daily_date,
polarity;


#VIEW VOC_3_ratingtime:

CREATE OR REPLACE view `{project}.{datamart}.VOC_3_ratingtime` 
as
with table1 as (
select 
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

count(distinct review_id) as records,
count(review_id) as collected_review_count,
sum( rating ) as sum_rating

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_id is not null
GROUP BY
case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id,
daily_date)

SELECT 
case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
source_name,
company_name,
company_id, 
source_id,
daily_date,
records as records,
collected_review_count as records_daily,
sum_rating as rating_daily,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
 < 7 THEN NULL
ELSE
sum(sum_rating) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
END AS RATING_MA7,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
 < 7 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA7,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
 < 14 THEN NULL
ELSE
sum(sum_rating) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
END AS RATING_MA14,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
 < 14 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA14,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
 < 30 THEN NULL
ELSE
sum(sum_rating) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
END AS RATING_MA30,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
 < 30 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA30,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
 < 60 THEN NULL
ELSE
sum(sum_rating) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
END AS RATING_MA60,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
 < 60 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA60,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
 < 90 THEN NULL
ELSE
sum(sum_rating) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
END AS RATING_MA90,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
 < 90 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA90,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
 < 120 THEN NULL
ELSE
sum(sum_rating) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
END AS RATING_MA120,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
 < 120 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA120,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
 < 150 THEN NULL
ELSE
sum(sum_rating) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
END AS RATING_MA150,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
 < 150 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA150,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
 < 180 THEN NULL
ELSE
sum(sum_rating) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
END AS RATING_MA180,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id

ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
 < 180 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
source_id
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA180

from table1

;


#View VOC_4_1_sstimecompany:

CREATE OR REPLACE view `{project}.{datamart}.VOC_4_1_sstimecompany` 
AS
WITH table1 AS (
SELECT 
case_study_id,
max(case_study_name) case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
-- max(source_name) source_name,
max(company_name) company_name,
company_id, 
-- source_id,
review_date as daily_date,

count(distinct review_id) as records,
count(review_id) as collected_review_count,
sum( modified_polarity ) as sum_modified_polarity

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_id is not null
GROUP BY
case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
-- source_id,
daily_date)

select 
case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
-- source_name,
company_name,
company_id, 
-- source_id,
daily_date,
records as records,
collected_review_count as records_daily,
sum_modified_polarity as ss_daily,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
 < 7 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
END AS SS_MA7,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id


ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
 < 7 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA7,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
 < 14 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
END AS SS_MA14,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
 < 14 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA14,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
 < 30 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
END AS SS_MA30,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
 < 30 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA30,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
 < 60 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
END AS SS_MA60,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
 < 60 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA60,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
 < 90 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
END AS SS_MA90,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
 < 90 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA90,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
 < 120 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
END AS SS_MA120,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
 < 120 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA120,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
 < 150 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
END AS SS_MA150,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
 < 150 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA150,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
 < 180 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
END AS SS_MA180,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
 < 180 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA180

from table1
;





#View VOC_4_2_cusrevsstime:


CREATE OR REPLACE view `{project}.{datamart}.VOC_4_2_cusrevsstime` AS

WITH table1 AS (
SELECT 
case_study_id,
max(case_study_name) case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
-- max(source_name) source_name,
max(company_name) company_name,
company_id, 
-- source_id,
review_date as daily_date,

count(distinct review_id) as records,
count(review_id) as collected_review_count,
sum( modified_polarity ) as sum_modified_polarity

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_id is not null
GROUP BY
case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
-- source_id,
daily_date)

SELECT

case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
-- source_name,
company_name,
company_id, 
-- source_id,
daily_date,
records as records,
collected_review_count as records_daily,
sum_modified_polarity as ss_daily,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
 < 7 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
END AS SS_MA7,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
 < 7 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA7,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
 < 7 THEN NULL
ELSE
AVG(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
END AS CR_MA7,


CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
 < 14 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
END AS SS_MA14,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
 < 14 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA14,


CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
 < 14 THEN NULL
ELSE
AVG(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
END AS CR_MA14,


CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
 < 30 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
END AS SS_MA30,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
 < 30 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA30,


CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
 < 30 THEN NULL
ELSE
AVG(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
END AS CR_MA30,


CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
 < 60 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
END AS SS_MA60,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
 < 60 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA60,


CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
 < 60 THEN NULL
ELSE
AVG(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
END AS CR_MA60,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
 < 90 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
END AS SS_MA90,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
 < 90 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA90,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
 < 90 THEN NULL
ELSE
AVG(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
END AS CR_MA90,


CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
 < 120 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
END AS SS_MA120,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
 < 120 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA120,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
 < 120 THEN NULL
ELSE
AVG(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
END AS CR_MA120,


CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
 < 150 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
END AS SS_MA150,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
 < 150 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA150,


CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
 < 150 THEN NULL
ELSE
AVG(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
END AS CR_MA150,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
 < 180 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
END AS SS_MA180,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id

ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
 < 180 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA180,


CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
 < 180 THEN NULL
ELSE
AVG(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
END AS CR_MA180

from table1
;


# VIEW VOC_6_1_sstimedimcompany

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_1_sstimedimcompany` 

AS

WITH table1 AS (
SELECT 
case_study_id,
max(case_study_name) case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
-- max(source_name) source_name,
max(company_name) company_name,
company_id, 
-- source_id,
modified_dimension as dimension,
review_date as daily_date,

count(distinct review_id) as records,
count(review_id) as collected_review_count,
sum( modified_polarity ) as sum_modified_polarity

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_id is not null
GROUP BY
case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
-- source_id,
modified_dimension,
daily_date)

select 
case_study_id,
case_study_name,
dimension_config_name,
nlp_type,
nlp_pack,
-- source_name,
company_name,
company_id, 
-- source_id,
dimension,
daily_date,
records as records,
collected_review_count as records_daily,
sum_modified_polarity as ss_daily,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
 < 7 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
END AS SS_MA7,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension


ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
 < 7 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA7,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
 < 14 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
END AS SS_MA14,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) 
 < 14 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA14,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
 < 30 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
END AS SS_MA30,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) 
 < 30 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA30,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
 < 60 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
END AS SS_MA60,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) 
 < 60 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA60,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
 < 90 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
END AS SS_MA90,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) 
 < 90 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA90,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
 < 120 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
END AS SS_MA120,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) 
 < 120 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA120,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
 < 150 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
END AS SS_MA150,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) 
 < 150 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA150,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
 < 180 THEN NULL
ELSE
sum(sum_modified_polarity) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
END AS SS_MA180,

CASE WHEN COUNT(records) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension

ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) 
 < 180 THEN NULL
ELSE
sum(collected_review_count) OVER (PARTITION BY case_study_id,
dimension_config_id,
nlp_type,
nlp_pack,
company_id,
dimension
ORDER BY daily_date ROWS BETWEEN 179 PRECEDING AND CURRENT ROW)
END AS RECORDS_MA180

from table1;


"""
    query_job =bqclient.query(query_string3)
    query_job .result()
    print("\nCreating tables in Datamart is successfull!")



###############
############### Step 4: INSERT INITIAL DATA INTO language_trans AND dimension_default ####################

# Insert table language_trans in dwh:
    language_file='language_trans.csv'
    file = 'data/'+ language_file

    time.sleep(5)

    # with open(file,"r", encode='utf-8') as f:
    df0 = pd.read_csv(file,sep=',',engine='python')

    rows_to_insert = df0.to_json(orient='records')
    rows_to_insert = ast.literal_eval(rows_to_insert)

    bqclient.insert_rows_json(
"{}.{}.language_trans".format(project, dwh), rows_to_insert)
    print("\nInitial load of language_trans table is successful!")
        
        
# Insert table dimension_default in dwh:
    dimension_file='dimension_default.csv'
    file = 'data/'+ dimension_file

   
    time.sleep(5)
    # with open(file,"r",encode='utf-8') as f:
    df0 = pd.read_csv(file,sep=',',engine='python')

    rows_to_insert = df0.to_json(orient='records')
    rows_to_insert = ast.literal_eval(rows_to_insert)

    bqclient.insert_rows_json(
"{}.{}.dimension_default".format(project, dwh), rows_to_insert)
    print("\nInitial load of dimension_default table is successful!")

def downgrade():

    for database in database_list:
        dataset_id = "{}.{}".format(project,database)

         
        bqclient.delete_dataset(
            dataset_id, delete_contents=True, not_found_ok=True
        )  # Make an API request.
        print("Deleted  dataset {}.{}".format(project, database))

    
