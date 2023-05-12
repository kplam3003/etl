# Changelog
<a name="v5.30.201"></a>

# v5.30.201 Features and Bug Fixing (02/05/2023)

## Bug Fixing
- Bypass SLL verify on google translate

<a name="v5.30.2"></a>

# v5.30.2 Features and Bug Fixing (28/04/2023)

## Features
- [TPPLEO-2844](https://jira.tpptechnology.com/browse/TPPLEO-2844): [HRA] Enter a company website at the endpoint for Coresignal

<a name="v5.30.002"></a>

# v5.30.002 Features and Bug Fixing (24/04/2023)

## Bug Fixing
- HRA coresignal time parser bug fixing

<a name="v5.30.001"></a>

# v5.30.001 Features and Bug Fixing (19/04/2023)

## Bug Fixing
- HRA case study bug fixing


<a name="v5.30.1"></a>

# v5.30.1 Features and Bug Fixing (14/04/2023)

## Bug Fixing
- [TPPLEO-2820](https://jira.tpptechnology.com/browse/TPPLEO-2820) [PROD][Data][Company Overview] - Total crawled Jobs of Glassdoor DS is calculated incorrectly
- [TPPLEO-2834](https://jira.tpptechnology.com/browse/TPPLEO-2834) [DATA] Update data sources from Web Unblocker to Datacenter

<a name="v5.29.3"></a>

# v5.29.3 Features and Bug Fixing (10/04/2023)

## Bug Fixing
- [TPPLEO-2674](https://jira.tpptechnology.com/browse/TPPLEO-2674) [STAG][HRA][Employee Profiles] - Missing value of "Country" column on web UI while exist in exported file
- [TPPLEO-2746](https://jira.tpptechnology.com/browse/TPPLEO-2746) [STAG][NLP Output] - "No data to show" is displayed when clicking "Search"

<a name="v5.29.2"></a>

# v5.29.2 Features and Bug Fixing (06/04/2023)

## Bug Fixing
- [TPPLEO-2813](https://jira.tpptechnology.com/browse/TPPLEO-2813) [STAG]/[PROD][Data][Company Overview] - Jobs of a company cannot be crawled successfully with Ambitionbox DS
- [TPPLEO-2797](https://jira.tpptechnology.com/browse/TPPLEO-2797) [STAG][Data][Company Overview] - Progress crawling reviews of G2/Gartner stop at 25%


<a name="v5.29.1"></a>

# v5.29.1 Features and Bug Fixing (05/04/2023)

## Bug Fixing
- [TPPLEO-2703](https://jira.tpptechnology.com/browse/TPPLEO-2703) [STAG][Data][Company Overview] - Crawling Stat Profile is not crawled when running CS with selected Re-Scrape/Re-NLP
- [TPPLEO-2691](https://jira.tpptechnology.com/browse/TPPLEO-2691) [STAG][HRA][Company Statistics]/[Job Experience]/[Monthly Dataset] - Default value of "End date" filter increases as time passed in day(s) and CS is run with new selected dimension config
- [TPPLEO-2753](https://jira.tpptechnology.com/browse/TPPLEO-2753) [STAG][Analytics][Average number of days a functional job has been listed] - Filter does not work correctly when items in "Job function" are sensitive case of each other


<a name="v5.28.3"></a>

# v5.28.3 Features and Bug Fixing (29/03/2023)
## Features
- [TPPLEO-2793](https://jira.tpptechnology.com/browse/TPPLEO-2793): [DATA] Rework Glassdoor crawler
## Bug Fixing
- [TPPLEO-2703](https://jira.tpptechnology.com/browse/TPPLEO-2703) [STAG][Data][Company Overview] - Crawling Stat Profile is not crawled when running CS with selected Re-Scrape/Re-NLP

<a name="v5.28.2"></a>

# v5.28.2 Features and Bug Fixing (28/03/2023)
## Features
- [TPPLEO-2791](https://jira.tpptechnology.com/browse/TPPLEO-2791): [DATA] Rework Capterra crawler
- [TPPLEO-2790](https://jira.tpptechnology.com/browse/TPPLEO-2790): [DATA] Improve Appstore crawler
- [TPPLEO-2792](https://jira.tpptechnology.com/browse/TPPLEO-2792): [DATA] Rework G2 crawler


## Bug Fixing
- [TPPLEO-2785](https://jira.tpptechnology.com/browse/TPPLEO-2785): [STAG]/[PROD][Data][Company Overview] - Jobs of Glassdoor DS cannot be crawled successfully
- [TPPLEO-2787](https://jira.tpptechnology.com/browse/TPPLEO-2787): [PROD][Data][Company Overview] - Missing reviews while crawling data of Capterra DS



<a name="v5.28.1"></a>

# v5.28.1 Features and Bug Fixing (18/03/2023)
## Features
- [TPPLEO-2776](https://jira.tpptechnology.com/browse/TPPLEO-2776): [PROD][DATA] Fix data sources that failed to crawl
    - [TPPLEO-2779](https://jira.tpptechnology.com/browse/TPPLEO-2779): [Crawler] Trustpilot
    - [TPPLEO-2780](https://jira.tpptechnology.com/browse/TPPLEO-2780): [Crawler] Apple Store
    - [TPPLEO-2781](https://jira.tpptechnology.com/browse/TPPLEO-2781): [Crawler] Google Play
    - [TPPLEO-2782](https://jira.tpptechnology.com/browse/TPPLEO-2782): [Crawler] Mouthshut
    - [TPPLEO-2783](https://jira.tpptechnology.com/browse/TPPLEO-2783): [Crawler] Bypass SSL certificate verify

<a name="v5.27.1"></a>
# v5.27.1 Changelogs (10/03/2023)
## Bugfix
- HRA - CS Data Loader - Fix loading empty company
- [TPPLEO-2724](https://jira.tpptechnology.com/browse/TPPLEO-2724): Show edu config data in export file
- [TPPLEO-2578](https://jira.tpptechnology.com/browse/TPPLEO-2578): Update review/job ID for VoE CSV source
- [TPPLEO-2705](https://jira.tpptechnology.com/browse/TPPLEO-2705): [HRA] Turnover Dataset - Fix missing data for last year
- [TPPLEO-2664](https://jira.tpptechnology.com/browse/TPPLEO-2664): Employee Profiles - Missing URL column in exported file
- Crawler - Fix Capterra
- [TPPLEO-2771](https://jira.tpptechnology.com/browse/TPPLEO-2771): Fix Trustpilot crawler
- [TPPLEO-2751](https://jira.tpptechnology.com/browse/TPPLEO-2751): Fix wrong data_type on statistic table in Load worker

<a name="v5.25.2"></a>
# v5.25.2 Changelogs (15/02/2023)
## Bugfix
- [TPPLEO-2735](https://jira.tpptechnology.com/browse/TPPLEO-2735) [DP] Case Study update_cs_data cannot progress when review_stats is not finished 
- Add Architectural documentation
## Maintenance
- Update Ambitionbox and Gartner crawlers

<a name="v5.25.1"></a>
# v5.25.1 Changelogs (09/02/2023)
## Features
- [TPPLEO-2705](https://jira.tpptechnology.com/browse/TPPLEO-2705) [HRA] Turnover Dataset

<a name="v5.23.2"></a>
# v5.23.2 Changelogs (11/01/2023)
## Features
- [TPPLEO-2694](https://jira.tpptechnology.com/browse/TPPLEO-2694) [HRA] [DP] [DATA] [Case study - Upload file] Education Classification
- [TPPLEO-2696](https://jira.tpptechnology.com/browse/TPPLEO-2696) [HRA] [DP] Education Dataset tab
- [TPPLEO-2697](https://jira.tpptechnology.com/browse/TPPLEO-2697) [HRA] [BA] Add new Education columns on UI and DB for Employee Profiles tab 
## Optimization
- Translation API now accept batch of texts
- Translation Worker now post batches of texts for translation for better efficiency
## Bugfix
- [TPPLEO-2723](https://jira.tpptechnology.com/browse/TPPLEO-2723) [STAG][HRA][Education Dataset] - Duplicated records for Education Dataset
- Mouthshut crawler step_detail missing `data_type` key in `meta_data` field

<a name="v5.23.1"></a>
# v5.23.1 Changelogs (03/01/2023)
## Bugfix
- [TPPLEO-2699](https://jira.tpptechnology.com/browse/TPPLEO-2699) [STAG][Data][Company Overview] - Reviews of a company cannot be crawled successfully with Reddit DS
- Handle case where Apple Store links contain uppercase in country code 
## Maintenance
- Increase number of Translation API from 1 to 2 so that it can handler more translation requests better.


<a name="v5.22.1"></a>
# v5.22.1 Changelogs (22/12/2022)
## Bugfix
- [TPPLEO-2672](https://jira.tpptechnology.com/browse/TPPLEO-2672) [STAG][HRA][Company Profiles] - Duplicated records for each company
- [TPPLEO-2675](https://jira.tpptechnology.com/browse/TPPLEO-2675) [STAG][HRA][Job Experience] - Duplicated records for each job experience
- [TPPLEO-2693](https://jira.tpptechnology.com/browse/TPPLEO-2693) [STAG][Analyticst][Status progress] - Status progress stops at 0% when "Re-scrape" is used in CS
## Maintenance
- Update Glassdoor job scraper to avoid being blocked.
## Optimization
- Improve performance of Tenure calculation for Monthly Dataset

<a name="v5.21.1"></a>
# v5.21.1 Changelogs (16/12/2022)
## Features
- [TPPLEO-2682](https://jira.tpptechnology.com/browse/TPPLEO-2682) [STAG] [Healthcheck] Readable successful rate

<a name="v5.20.1"></a>
# v5.20.1 Changelogs (30/11/2022)
## Features
- [TPPLEO-2678](https://jira.tpptechnology.com/browse/TPPLEO-2678) [STAG] [DATA] Mark target companies
- [TPPLEO-2679](https://jira.tpptechnology.com/browse/TPPLEO-2679) [STAG] [DATA] Monthly Dataset
- [TPPLEO-2680](https://jira.tpptechnology.com/browse/TPPLEO-2680) [STAG] [DATA] Tenure fields
- [TPPLEO-2684](https://jira.tpptechnology.com/browse/TPPLEO-2684) Do not bring employees with only deleted experiences
- Add support for multiple preprocessors
## Bugfix
- [TPPLEO-2677](https://jira.tpptechnology.com/browse/TPPLEO-2677) [STAG] Change logic for "Company" filter at Job Experience

<a name="v5.19.1"></a>
# v5.19.1 Changelogs (14/11/2022)
## Maintenance
- Crawlers: Using Web Unblocker proxy with retry method to crawl data on ambitionbox, applestore, glassdoor, indeed, gartner.

<a name="v5.18.1"></a>
# v5.18.1 Changelogs (01/11/2022)
## Features
- [TPPLEO-2652](https://jira.tpptechnology.com/browse/TPPLEO-2652) [BA][STAG][HRA] Change to use Geocoding API + add Google Country + Google State columns

<a name="v5.17.4"></a>
# v5.17.4 Changelogs (27/10/2022)
## Maintenance
- Bump version of `gcsfs` to `2022.10.0` to resolve import errors when calling deprecated enpoints.

<a name="v5.17.3"></a>
# v5.17.3 Changelogs (20/10/2022)
## Features
- [TPPLEO-2651](https://jira.tpptechnology.com/browse/TPPLEO-2651) [BA][STAG] [HRA] Update columns for Job Experience records

<a name="v5.17.2"></a>
# v5.17.2 Changelogs (18/10/2022)
## Maintenance
- Google Play: Bump version of `google-play-scraper` package to `1.2.2` to resolve changes in Google Play structures
- Ambitionbox: add User Agents to requests to overcome 403 status code
- Linkedin overview: add timeout to Webhook crawler requests to avoid scraper being freezed
## Bugfix
- `web_sync` does not refuse invalid `data_type` from payload leading to dangling request records in database and incorrect processing steps. Will now refuse `data_type`'s value not being one of `("job", "review", "review_stats", "coresignal_stats", "coresignal_employees")`
- `webhook_crawler`: add error handling for webhook accept job `\luminati` endpoint

<a name="v5.17.1"></a>
# v5.17.1 Changelogs (14/10/2022)
## Features
- [TPPLEO-2613](https://jira.tpptechnology.com/browse/TPPLEO-2613)[HR Analysis] [UI][DP] Case study creation for HRA
- [TPPLEO-2617](https://jira.tpptechnology.com/browse/TPPLEO-2617)[HR Analysis] Company Statistics chart for HRA
- [TPPLEO-2633](https://jira.tpptechnology.com/browse/TPPLEO-2633)[HR Analysis] Case study - Job Experience tab
- [TPPLEO-2632](https://jira.tpptechnology.com/browse/TPPLEO-2632)[HR Analysis] Case study - Employee Profiles tab
- [TPPLEO-2631](https://jira.tpptechnology.com/browse/TPPLEO-2631)[HR Analysis] Case study - Company Profiles tab
- [TPPLEO-2612](https://jira.tpptechnology.com/browse/TPPLEO-2612)[HR Analysis] [DATA] Apply job functions to vectors

<a name="v5.16.1"></a>
# v5.16.1 Changelogs (04/10/2022)
## Bugfix
- Add missing `ENCODE_SENTENCE_API` env var for Preprocessor
- Add more `company_shorthand_name` into Coresignal Employees exported data files to make it more clear and avoid filename duplication. 

<a name="v5.15.2"></a>
# v5.15.2 Changelogs (13/10/2022)
## Hotfix
- Glassdoor Review scraper is being randomly blocked more, leading to reviews not fully scraped. Increase retry count and backoff time to mitigate the issue.
- Update Glassdoor Job scraper to accomodate new change in `posted_date`.

<a name="v5.15.1"></a>
# v5.15.1 Changelogs (16/09/2022)
## Features
- [TPPLEO-2591](https://jira.tpptechnology.com/browse/TPPLEO-2591) [HR Analysis] Vectorize Experience Titles - Export Processed Employee Profiles
- [TPPLEO-2592](https://jira.tpptechnology.com/browse/TPPLEO-2592) [HR Analysis] [Static Data] Crawling Employee Profiles
- [TPPLEO-2600](https://jira.tpptechnology.com/browse/TPPLEO-2600) [HR Analysis] [DATA] Leonardo company ID
- [TPPLEO-2607](https://jira.tpptechnology.com/browse/TPPLEO-2607) [HR Analysis] [DATA] Previous and next employee refinement
- Apply processing to existing Coresignal Employees data

<a name="v5.14.3"></a>
# v5.14.3 Changelogs (13/09/2022)
## Bugfix
- Change `nlp_type` to HRA to match webapp's changes

<a name="v5.14.2"></a>
# v5.14.2 Changelogs (07/09/2022)
## Bugfix
- [TPPLEO-2602](https://jira.tpptechnology.com/browse/TPPLEO-2602) [STAG][Data][Company Overview] - Employee Statistics of Coresignal Linkedin DS cannot be crawled successfully

<a name="v5.14.1"></a>
# v5.14.1 Changelogs (31/08/2022)
## Features
- [TPPLEO-2589](https://jira.tpptechnology.com/browse/TPPLEO-2589) HR Analysis - Previous/Next Company
- [TPPLEO-2590](https://jira.tpptechnology.com/browse/TPPLEO-2590) HR Analysis - Is starter/leaver

<a name="v5.13.2"></a>
# v5.13.2 Changelogs (24/08/2022)
## Features
- [TPPLEO-2571](https://jira.tpptechnology.com/browse/TPPLEO-2571) [HR Analytics][Data] First employee scrape
- [TPPLEO-2572](https://jira.tpptechnology.com/browse/TPPLEO-2572) [BA][HR Analytics][Data] Employee clean
- [TPPLEO-2573](https://jira.tpptechnology.com/browse/TPPLEO-2573) [HR Analytics][Data] Subsequent employee scrapes

<a name="v5.13.1"></a>
# v5.13.1 Changelogs (16/08/2022)
## Bugfix
- Handle review_stats for uploaded csv files

<a name="v5.12.2"></a>
# v5.12.2 Changelogs (14/08/2022)
## Refactor
- Remove all reference and code for `operation_service`, which is an unused feature and not fully developed.

<a name="v5.12.1"></a>
# v5.12.1 Changelogs (04/08/2022)
## Feature
- [TPPLEO-2569](https://jira.tpptechnology.com/browse/TPPLEO-2569) HR Analytics Data Source
- [TPPLEO-2570](https://jira.tpptechnology.com/browse/TPPLEO-2570) Company Profile, HR Analytics
- [TPPLEO-2574](https://jira.tpptechnology.com/browse/TPPLEO-2574) When uploading a CSV keep the ID available on "NLP Download" and "Customer review download"

## Bugfix
- [TPPLEO-2539](https://jira.tpptechnology.com/browse/TPPLEO-2539) [STAG][Job posting] Not matching data in the exported data file with data displayed
- [TPPLEO-2560](https://jira.tpptechnology.com/browse/TPPLEO-2560) [STAG][Analytics][Source profile statistics] - "0 records" is shown in result description when running chart that leads to there is data for results

## Maintenance
- [TPPLEO-2554](https://jira.tpptechnology.com/browse/TPPLEO-2554) [STAG][Administration][Crawler Health Check] - Issue with triggering data sources - fix Apple Store to reflect new changes from source website.
- Fix Apple Store Crawler to properly:
    - bypass countries without any reviews
    - handle errors in requesting for data

## Refactor
- Migration + code changes to merge separated `etl_company_datasource*` tables (3 for `review`, `review_stats`, `job`) into one single `etl_company_datasource`


<a name="v5.10.2"></a>
# v5.10.2 Changelogs (13/07/2022)
## Features
- [TPPLEO-2534](https://jira.tpptechnology.com/browse/TPPLEO-2534) [BA] [Analytics] VoE_12 chart: Self-reported employee scores by players over time

<a name="v5.10.1"></a>
# v5.10.1 Changelogs (05/07/2022)
## Bugfixes
- Fix a bug where if a company_datasource's `review_stats` is not triggered but used in a case study, case study will have no data.


<a name="v5.9.1"></a>
# v5.9.1 Changelogs (28/06/2022)
## Features
- [TPPLEO-2528](https://jira.tpptechnology.com/browse/TPPLEO-2528) [BA] [Analytics] Source profile statistics chart
## Optimizations
- `cs_data_loader` now loads `parent_review_id`, `technical_type`, `review_country`, `country_code`, `company_datasource_id`, and `url` to `voc`, `voe`, `voc_custom_dimension`, `voe_custom_dimension` BigQuery instead of loading to dedicated tables `staging.parent_review_mapping` and `staging.review_country_mapping`.
- `(voe_)nlp_output_case_study` tables are loaded with all country and parent review mapping data directly from staging tables.
- All case studies are now truly independent of each other: one summary table contains everything that particular case study needs.
- All chart queries for a case study now only queries directly from the summary table (2 in the case of VoE case studies), improving query time for some charts.
## Bugfixes
- Fix a bug where if a company_datasource is not triggered but used in a case study, case study cannot start. Mostly applied to `review_stats` not being triggered.

<a name="v5.8.1"></a>
# v5.8.1 Changelogs (15/06/2022)
## Features
- [TPPLEO-2529](https://jira.tpptechnology.com/browse/TPPLEO-2529) [BA] [DATA - Static Data] Crawling Profile Statistics

<a name="v5.7.1"></a>

# v5.7.1 Features and Bugfixing (30/05/2022)
## Features
- [TPPLEO-2527](https://jira.tpptechnology.com/browse/TPPLEO-2527) [BA] Job postings

<a name="v5.5.2"></a>

# v5.5.2 Features and Bugfixing (10/05/2022)
## Maintenance
- Revamp error propagation mechanism for all crawlers so that error in pages are reported in details

<a name="v5.5.1"></a>

# v5.5.1 Features and Bugfixing (06/05/2022)
## Bugfixes
- GLASSDOOR OVERVIEW - Overview module not available
- GLASSDOOR JOB - Job duplication for each crawl
- INDEED REVIEW - Reviews cannot be crawled
- TRUSTPILOT REVIEW - Reviews in the first page cannot be crawled due to redirect

<a name="v5.3.2"></a>

# v5.3.2 Features and Bugfixing (22/04/2022)
## Features
- Revamp error message sending for G2 and Ambitionbox so that page error is reported as well.
## Bugfixes
- `company_datasource_orchestrator` listen pubsub progress does not query to the correct table for job

<a name="v5.3.1"></a>

# v5.3.1 Features (19/04/2022)
## Features
- [TPPLEO-2502](https://jira.tpptechnology.com/browse/TPPLEO-2502) [BA] Implement Crawler Health Check: DP ETL error handling re-implementation.

<a name="v5.2.1"></a>

# v5.2.1 Features (24/03/2022)
## Features
- [TPPLEO-2498](https://jira.tpptechnology.com/browse/TPPLEO-2498) [BA] Ambitionbox scraper (VoE)

<a name="v5.1.3"></a>

# v5.1.3 Changes (08/03/2022)
## Changes
- [TPPLEO-2493](https://jira.tpptechnology.com/browse/TPPLEO-2493) [BA][US] Mouthshut scraper (VoC)

<a name="v5.1.2"></a>

# v5.1.2 Changes (02/03/2022)
## Changes
- [TPPLEO-2480](https://jira.tpptechnology.com/browse/TPPLEO-2480) [BA] Summary view data source / API: change MongoDB authentication method of Data Platform workers to using SSL keys.

<a name="v5.1.1"></a>

# v5.1.1 Bug Fixing (28/02/2022)
## Bugfixes
- Bump up webhook crawler's version of `gunicorn` to `20.1.0` to resolve an issue with thread silently crash due to uncaught `ENOTCON` error (socket error).
- [TPPLEO-2148](https://jira.tpptechnology.com/browse/TPPLEO-2148) [STAG][Analysis][Schedule CS] - Latest reviews are not shown on Customer Reviews on re-run case study (as schedule): fix a bug regarding data versions when resolving payloads for scheduled VoE case studies

<a name="v4.8.4"></a>

# v4.8.4 Maintenance (10/02/2022)
## Maintenance
- Capterra dispatcher and crawler now use Unblocker proxy to avoid being blocked

<a name="v4.8.3"></a>

# v4.8.3 Bug fixing (09/02/2022)
## Bugfixes
- Add connection pool and connection pre-ping for `company_datasource_orchestrator` to avoid overloaded and stale connection problems

<a name="v4.8.2"></a>

# v4.8.2 Maintenance (08/02/2022)
## Maintenance
- Modify Gartner crawler to get technical review details from a different URL due to the previous one being removed by Gartner

<a name="v4.8.1"></a>

# v4.8.1 Maintenance (07/02/2022)
## Maintenance
- Bump version of `google-play-scraper` to `1.0.3` to solve a problem with total number of reviews available.

<a name="v4.6.2"></a>

# v4.6.2 Features and Bug Fixing (10/12/2021)
## Bugfixes
- [TPPLEO-2467](https://jira.tpptechnology.com/browse/TPPLEO-2467) [DP] VMs for crawl jobs is scaled incorrectly
- Translation API SSL error when sending requests via HTTPS proxy

<a name="v4.6.1"></a>

# v4.6.1 Features and Bug Fixing (03/12/2021)
## Maintenance
- Switch proxy service of G2, Gartner, and Glassdoor (jobs, reviews, overview) crawler to BrightData Web Unblocker instead of Data Collector and Data Center IP

## Bugfixes
- [TPPLEO-2460](https://jira.tpptechnology.com/browse/TPPLEO-2460): update logic chart view for voe_job chart

## Hotfix (10/12/2021)
- [TPPLEO-2469](https://jira.tpptechnology.com/browse/TPPLEO-2469) [PROD][Analytics][Reviews statistics] - "No data to show" when running chart with company has "/" in name


<a name="v4.5.6"></a>

# v4.5.6 Features and Bug Fixing (30/11/2021)
## Maintenance
- [TPPLEO-2399](https://jira.tpptechnology.com/browse/TPPLEO-2399): add language zh-TW and zh-HK to Google Play Store crawler

## Bugfixes
- Google Play crawler missing reviews due to wrong number of `total_review`
- Glassdoor review crawler missing non-English reviews due to wrong language codes

## Known issues
- Glassdoor job crawler cannot get job posting past page 6


<a name="v4.5.5"></a>

# v4.5.5 Features and Bug Fixing (25/11/2021)
## Maintenance
- [TPPLEO-2448](https://jira.tpptechnology.com/browse/TPPLEO-2448): [DP][STAG] - Crawling issue with Glassdoor job crawler
- [TPPLEO-2452](https://jira.tpptechnology.com/browse/TPPLEO-2452): [DP][STAG] - Crawling issue with GARTNER crawler


<a name="v4.5.4"></a>

# v4.5.4 Features and Bug Fixing (24/11/2021)
## Bugfixes
- Gartner crawler fails completely when only a page fails to be scraped.
- Replace newlines in reviews crawled by G2 crawler to avoid malformed csv files


<a name="v4.5.3"></a>

# v4.5.3 Features and Bug Fixing (23/11/2021)
## Bugfixes
- Missing deployment environment variable for G2 Data Collector crawler
- Wrong data_type in web_sync when handling company_datasource's update action


<a name="v4.5.2"></a>

# v4.5.2 Features and Bug Fixing (23/11/2021)
## Bugfixes
- Missing deployment environment variable for G2 Data Collector crawler
- Wrong data_type in web_sync when handling company_datasource's update action


<a name="v4.5.1"></a>

# v4.5.1 Features and Bug Fixing (19/11/2021)
## Maintenance
- [TPPLEO-2446](https://jira.tpptechnology.com/browse/TPPLEO-2446): [DP][STAG] - Crawling issue with App Store Reviews URL
- [TPPLEO-2447](https://jira.tpptechnology.com/browse/TPPLEO-2447): [DP][STAG] - Crawling issue with G2 crawler
