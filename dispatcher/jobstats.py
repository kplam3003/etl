import json
import js2py
import requests
from logging import Logger
from bs4 import BeautifulSoup
from typing import List, Any
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter, Retry
from multiprocessing.dummy import Pool
from google.cloud import bigquery

import config
from core import etl_const
from database import ETLStepDetail, ETLCompanyBatch
import utils

logger = utils.load_logger()

class JobStats:
    def __init__(self, proxy_url: str, logger: Logger = None) -> None:
        self.proxy_url = proxy_url
        self.logger = logger
        self.session = requests.Session()
        self.request_kwargs = {
            "proxies": {"http": self.proxy_url, "https": self.proxy_url},
            "verify": False,
            "headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
            },
            "timeout": 60,
        }

        # Setup request session
        retry_after_status_codes = frozenset([407, 413, 429, 502, 503])
        retries = Retry(
            total=4,
            backoff_factor=1.0,
            status_forcelist=retry_after_status_codes,
        )
        self.session.mount(prefix="http://", adapter=HTTPAdapter(max_retries=retries))
        self.session.mount(prefix="https://", adapter=HTTPAdapter(max_retries=retries))

    def _test_proxy(self) -> str:
        response = self.session.get(
            url="https://api.ipify.org?format=json", **self.request_kwargs
        )
        data = response.json()
        return data["ip"]

    def _parse_js(self, js_content: str, obj_key: str) -> dict:
        result = {}

        # Normalize content
        js_content = js_content.replace("\\n", ";")
        js_content = js_content.replace("\\\\", "\\")

        # Trying load sub-content using `json` lib
        start_idx = js_content.index(obj_key)
        end_idx = js_content.index("}", start_idx)
        while True:
            sub_content = '{"' + js_content[start_idx : end_idx + 1]
            try:
                result = json.loads(sub_content)
                break
            except json.decoder.JSONDecodeError:
                if js_content.find("}", end_idx + 1) == -1:
                    break
                end_idx = js_content.index("}", end_idx + 1)

        return result

    def _get_nested_value(self, dict_val: dict, key_path: List[str]) -> Any:
        result = dict_val
        for key in key_path:
            if key not in result:
                return None
            result = result[key]
        return result

    def log_crawl_stats(self, batch: ETLCompanyBatch, item_count: int):
        bq_client = bigquery.Client()
        nlp_type = str(batch.nlp_type).lower()
        data_type = batch.meta_data["data_type"]
        logger.info(f"log crawl stats nlp_type : {nlp_type} and data_type {data_type}")

        bq_statistic_table = ""
        if nlp_type == etl_const.Meta_NLPType.VOE.value.lower():
            bq_statistic_table = config.GCP_BQ_TABLE_VOE_CRAWL_STATISTICS
        elif nlp_type == etl_const.Meta_NLPType.VOC.value.lower():
            bq_statistic_table = config.GCP_BQ_TABLE_VOC_CRAWL_STATISTICS
        elif nlp_type == etl_const.Meta_NLPType.HR.value.lower():
            bq_statistic_table = config.GCP_BQ_TABLE_HRA_CRAWL_STATISTICS

        row = {
            "request_id": batch.request_id,
            "company_datasource_id": batch.company_datasource_id,
            "step_detail_id": None,
            "created_at": batch.created_at.isoformat(),
            "company_id": batch.company_id,
            "company_name": batch.company_name,
            "source_id": batch.source_id,
            "source_name": batch.source_name,
            "batch_id": batch.batch_id,
            "num_reviews": item_count,
            "data_version": batch.data_version,
            "data_type": f"origin_{data_type}",
        }
        logger.info(f"Inserting row {row} into Big query table {bq_statistic_table}")
        bq_errors = bq_client.insert_rows_json(bq_statistic_table, [row])
        return bq_errors

    def get_ambitionbox_job_count(self, url: str) -> int:
        if "/jobs/" not in url.lower():
            self.logger.warning(f"URL not valid: {url}")
            return 0

        response = self.session.get(url=url, **self.request_kwargs)
        soup = BeautifulSoup(response.text, "html.parser")

        js_data = {}
        for elm in soup.select("script"):
            script_content = elm.decode_contents().strip()
            if script_content.startswith("window.__NUXT__"):
                script_res: js2py.base.JsObjectWrapper = js2py.eval_js(script_content)
                js_data = script_res.to_dict()
                break

        if not js_data:
            self.logger.warning(f"Failed on `get_ambitionbox_job_count`: {url}")
            return 0

        return int(js_data["data"][0]["pagination"]["Count"])

    def get_glassdoor_job_count(self, url: str) -> int:
        # NOTE: Job page may reports job count for a specific country.
        # So, must get data from Overview page

        # Find URL for Overview page
        response = self.session.get(url=url, **self.request_kwargs)
        soup = BeautifulSoup(response.text, "html.parser")

        logo_elm = soup.select_one(".logo a")
        if not logo_elm:
            self.logger.warning(f"Logo not found: {url}")
            return 0

        overview_url: str = logo_elm.attrs["href"]
        if overview_url.startswith("/"):
            overview_url = f"{urlparse(url)._replace(path=overview_url).geturl()}"

        # Request Overview page
        if overview_url != url:
            response = self.session.get(url=overview_url, **self.request_kwargs)

        # Parse data
        js_data = {}
        object_key = "employerHeaderData"
        soup = BeautifulSoup(response.text, "html.parser")
        for elm in soup.select("script"):
            script_content = elm.decode_contents().strip()
            if object_key in script_content:
                js_data = self._parse_js(js_content=script_content, obj_key=object_key)
                break

        if not js_data:
            self.logger.warning(f"Failed on `get_glassdoor_job_count`: {url}")
            return 0

        result = self._get_nested_value(
            dict_val=js_data,
            key_path=[
                "employerHeaderData",
                "sectionCounts",
                "jobCount",
            ],
        )
        if result:
            return int(result)

        self.logger.warning(f"Value not found: {url}. {script_content}")
        return 0

    def get_indeed_job_count(self, url: str) -> int:
        count_in_all_countries = 0
        parse_result = urlparse(url=url)

        def _get_count(country: str) -> int:
            country_url = parse_result._replace(netloc=f"{country}.indeed.com").geturl()
            response = None

            try:
                response = self.session.get(url=country_url, **self.request_kwargs)
            except:
                self.logger.warning(f"{country_url} : {response}")

            if not response:
                return 0

            if response.status_code == 404:
                return 0

            soup = BeautifulSoup(response.text, "html.parser")
            script_elm = soup.select_one("script#comp-initialData")
            if not script_elm:
                self.logger.warning(f"Failed on `get_indeed_job_count`: {url}")
                return 0

            js_data = json.loads(script_elm.decode_contents())
            return int(js_data["jobList"]["filteredJobCount"])

        with Pool(10) as p:
            results = p.map(_get_count, config.INDEED_COUNTRIES)
            count_in_all_countries = sum(results)

        return count_in_all_countries

    def get_linkedin_job_count(self, url: str) -> int:
        response = self.session.get(url=url, **self.request_kwargs)
        soup = BeautifulSoup(response.text, "html.parser")

        code_elm = soup.select_one("code#totalResults")
        if not code_elm:
            self.logger.warning(f"Failed on `get_linkedin_job_count`: {url}")
            return 0

        parts = code_elm.decode_contents().split("--")
        if len(parts) > 1 and parts[1].isdigit():
            return int(parts[1])

        self.logger.warning(f"Value not found: {url}.")
        return 0


if __name__ == "__main__":
    import logging

    print("TEST!")
    job_stats = JobStats(
        proxy_url=config.LUMINATI_WEBUNBLOCKER_HTTP_PROXY,
        logger=logging.getLogger("test"),
    )

    # results = job_stats._test_proxy()

    ## Ambitionbox
    # results = job_stats.get_ambitionbox_job_count(
    #     url="https://www.ambitionbox.com/jobs/accenture-jobs"
    # )
    # results = job_stats.get_ambitionbox_job_count(
    #     url="https://www.ambitionbox.com/jobs/medhaj-techno-dot-concept-pvt-jobs"
    # )

    ## Glassdoor
    # results = job_stats.get_glassdoor_job_count(
    #     url="https://www.glassdoor.co.uk/Jobs/Quantiphi-Jobs-E1143542.htm?filter.countryId=115"
    # )
    # results = job_stats.get_glassdoor_job_count(
    #     url="https://www.glassdoor.co.uk/Jobs/KPMG-Jobs-E2867.htm"
    # )

    ## Indeed
    results = job_stats.get_indeed_job_count(
        url="https://jobs.vn.indeed.com/cmp/Lazada/jobs"
    )
    # results = job_stats.get_indeed_job_count(
    #     url="https://uk.indeed.com/cmp/Doit/jobs"
    # )
    # results = job_stats.get_indeed_job_count(
    #     url="https://uk.indeed.com/cmp/McDonald's/jobs"
    # )

    ## Linkedin
    # results = job_stats.get_linkedin_job_count(
    #     url="https://www.linkedin.com/jobs/apple-jobs-worldwide?f_C=162479"
    # )

    print(results)
