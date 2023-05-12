from crawlers.capterra import execute_task as crawl_capterra_task
from crawlers.applestore import execute_task as crawl_applestore_task
from crawlers.googleplay import execute_task as crawl_googleplay_task
from crawlers.trustpilot import execute_task as crawl_trustpilot_task
from crawlers.g2 import execute_task as crawl_g2_task
from crawlers.glassdoor import execute_task as crawl_glassdoor_task
from crawlers.linkedin import execute_task as crawl_linkedin_task
from crawlers.indeed import execute_task as crawl_indeed_task
from crawlers.uploaded_csv import execute_task as crawl_from_csv
from crawlers.gartner import execute_task as crawl_gartner_task
from crawlers.reddit import execute_task as crawl_reddit_task
from crawlers.mouthshut import execute_task as crawl_mouthshut_task
from crawlers.ambitionbox import execute_task as crawl_ambitionbox_task
from crawlers.coresignal_linkedin import execute_task as crawl_coresignal_linkedin_task


SOURCE_CRAWLERS = {
    "capterra": crawl_capterra_task,
    "applestore": crawl_applestore_task,
    "googleplay": crawl_googleplay_task,
    "trustpilot": crawl_trustpilot_task,
    "g2": crawl_g2_task,
    "csv": crawl_from_csv,
    "glassdoor": crawl_glassdoor_task,
    "indeed": crawl_indeed_task,
    "linkedin": crawl_linkedin_task,
    "gartner": crawl_gartner_task,
    "reddit": crawl_reddit_task,
    "mouthshut": crawl_mouthshut_task,
    "ambitionbox": crawl_ambitionbox_task,
    "coresignal linkedin": crawl_coresignal_linkedin_task,
}


def get_crawler(source_code):
    return SOURCE_CRAWLERS.get(source_code.lower())
