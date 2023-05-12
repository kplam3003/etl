import re
import json
import random
import requests
import datetime as dt
import pandas as pd

from bs4 import BeautifulSoup
from google.cloud import pubsub_v1
from google.cloud import storage
from multiprocessing.pool import ThreadPool
from urllib.parse import unquote, urlencode
from typing import List, Tuple, Dict, Optional
from tenacity import retry, wait, stop, retry_if_exception_type

import config
from database import ETLCompanyBatch, ETLStep, ETLStepDetail, auto_session
from helpers import log_crawl_stats
from utils import load_logger
from config import LUMINATI_HTTP_PROXY


DELIMITER = "\t"

logger = load_logger()


class AppStore:
    USER_AGENTS = [
        # https://useragents.io/
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",  # noqa
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Safari/605.1.15",  # noqa
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15",  # noqa
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1",  # noqa
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",  # noqa
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.167 Safari/537.36",  # noqa
        "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36",  # noqa
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.129 Safari/537.36",  # noqa
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:103.0) Gecko/20100101 Firefox/103.0",  # noqa
        "Mozilla/5.0 (X11; Linux x86_64; rv:103.0) Gecko/20100101 Firefox/103.0",  # noqa
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0",  # noqa
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 11.6; rv:92.0) Gecko/20100101 Firefox/92.0",  # noqa
    ]
    ORIGIN = "https://apps.apple.com"

    meta: Optional[dict] = None
    cache: Optional[dict] = None
    country_apps_url: Optional[str] = None

    def __init__(self) -> None:
        self.logger = load_logger()
        self.proxy = LUMINATI_HTTP_PROXY
        self.page_size = 10
        self.pool_size = 4

    def load(self, apps_url: str, country_code: str) -> None:
        code_idx = 3
        url_parts = apps_url.split("/")
        country_apps_url = "/".join(
            url_parts[:code_idx] + [country_code] + url_parts[code_idx + 1 :]
        )

        self.meta, self.cache = self._get_meta_cache(apps_url=country_apps_url)
        self.country_apps_url = country_apps_url

    @staticmethod
    def _parse_rating_count(app_data: dict) -> int:
        rating_count = (
            app_data.get("attributes", {}).get("userRating", {}).get("ratingCount", 0)
        )
        return int(rating_count)

    @retry(
        wait=wait.wait_exponential(max=30),
        stop=stop.stop_after_attempt(5),
        retry=retry_if_exception_type(AssertionError),
    )
    def _get_meta_cache(self, apps_url: str) -> Tuple[dict, dict]:
        self.logger.info(f"_get_meta_cache(apps_url={apps_url!r})")
        headers = {"User-Agent": random.choice(AppStore.USER_AGENTS)}
        proxies = {"http": self.proxy}
        response = requests.get(
            url=apps_url, headers=headers, proxies=proxies, verify=False
        )  # noqa

        assert response.status_code == 200, "Retrying: Invalid response"

        soup = BeautifulSoup(response.text, "html.parser")

        # Get `meta` object
        meta = {}
        elm_meta = soup.select_one("meta[name='web-experience-app/config/environment']")
        if elm_meta is not None:
            meta: dict = json.loads(unquote(elm_meta.attrs["content"]))

        # Get `cache` object
        cache = {}
        elm_cache = soup.select_one("#shoebox-media-api-cache-apps")
        if elm_cache is not None:
            cache: dict = json.loads(elm_cache.decode_contents())
            cache: dict = json.loads(list(cache.values())[0])
            cache: dict = cache.get("d", [])[0]

        return (meta, cache)

    def get_review_stats(self, apps_url: str) -> dict:
        _, cache = self._get_meta_cache(apps_url=apps_url)
        user_rating: dict = cache.get("attributes", {}).get("userRating", {})
        rating_counts = {
            f"{idx+1} star": int(val)
            for idx, val in enumerate(user_rating.get("ratingCountList", []))
        }
        review_stats = {
            "total_ratings": user_rating.get("ratingCount"),
            "average_rating": user_rating.get("value"),
            **rating_counts,
        }

        return review_stats

    @retry(
        wait=wait.wait_exponential(max=30),
        stop=stop.stop_after_attempt(5),
        retry=retry_if_exception_type(AssertionError),
    )
    def _get_page_reviews(
        self, host: str, path: str, auth_token: str, offset: int
    ) -> Tuple[Optional[str], List[dict]]:
        # Construct page url
        params = {
            "platform": "web",
            "additionalPlatforms": "appletv,ipad,iphone,mac",
            "offset": offset,
        }
        api_url = f"{host}{path}&{urlencode(params)}"
        self.logger.info(f"_get_page_reviews(url={api_url!r})")

        # Make request
        headers = {
            "user-agent": random.choice(AppStore.USER_AGENTS),
            "authorization": f"Bearer {auth_token}",
            "origin": AppStore.ORIGIN,
        }
        proxies = {"http": self.proxy}
        response = requests.get(
            url=api_url,
            headers=headers,
            proxies=proxies,
            verify=False,
        )  # noqa

        assert response.status_code in [200, 404], "Retrying: Invalid response"

        # Extract data
        reviews = []
        next_path: Optional[str] = None
        if response.status_code == 200:
            data = response.json()
            reviews: List[dict] = data.get("data", [])
            next_path = data.get("next", None)

        return (next_path, reviews)

    def _get_max_offset(
        self, from_page: int, to_page: int
    ) -> Tuple[int, Dict[int, List[dict]]]:
        def _search_max_offset(
            host: str,
            path: str,
            auth_token: str,
            lower: int,
            upper: int,
            page_reviews: dict,
        ) -> Tuple[int, dict]:
            if upper < lower:
                return (lower, page_reviews)

            # Crawl reviews from `middle_offset`
            if lower == upper:
                middle_offset = lower
            else:
                offset_points = range(lower, upper, self.page_size)
                middle_offset = offset_points[int(len(offset_points) / 2)]
            next_path, reviews = self._get_page_reviews(
                host=host, path=path, auth_token=auth_token, offset=middle_offset
            )

            # Store crawled review
            if reviews:
                page_reviews[middle_offset] = reviews

            # Check for stop condition
            if (next_path is None) and len(reviews) > 0:
                return (middle_offset, page_reviews)

            # Recursive search: lower half
            if len(reviews) == 0:
                return _search_max_offset(
                    host=host,
                    path=path,
                    auth_token=auth_token,
                    lower=lower,
                    upper=middle_offset - self.page_size,
                    page_reviews=page_reviews,
                )

            # Recursive search: upper half
            return _search_max_offset(
                host=host,
                path=path,
                auth_token=auth_token,
                lower=middle_offset + self.page_size,
                upper=upper,
                page_reviews=page_reviews,
            )

        # Get initial data
        api_host = self.meta.get("API", {}).get("AppHost", "")
        auth_token = self.meta.get("MEDIA_API", {}).get("token", "")
        rating_count = self._parse_rating_count(app_data=self.cache)
        lower = (from_page - 1) * self.page_size
        upper = min((to_page - 1) * self.page_size, rating_count)
        next_path: str = (
            self.cache.get("relationships", {}).get("reviews", {}).get("next")
        )

        # Do binary search
        max_offset = 0
        page_reviews = {
            0: self.cache.get("relationships", {}).get("reviews", {}).get("data", [])
        }
        if next_path:
            api_path = next_path.split("&offset=")[0]
            max_offset, page_reviews = _search_max_offset(
                host=api_host,
                path=api_path,
                auth_token=auth_token,
                lower=lower,
                upper=upper,
                page_reviews=page_reviews,
            )

        return (max_offset, page_reviews)

    def get_all_reviews(
        self, apps_url: str, country_code: str, from_page: int, to_page: int
    ) -> List[dict]:
        if not self.cache:
            self.load(apps_url=apps_url, country_code=country_code)

        max_offset, page_reviews = self._get_max_offset(from_page, to_page)
        offset_points = range(
            (from_page - 1) * self.page_size,
            max_offset + 1,
            self.page_size,
        )
        remaining_offset_points = [
            offset for offset in offset_points if offset not in page_reviews
        ]

        if max_offset < (from_page - 1) * self.page_size:
            return []

        all_reviews = []
        for val in page_reviews.values():
            all_reviews.extend(val)

        next_path: str = (
            self.cache.get("relationships", {}).get("reviews", {}).get("next")
        )
        if not next_path:
            all_reviews = [
                dict(id=item.get("id", ""), **item.get("attributes", {}))
                for item in all_reviews
            ]
            return all_reviews

        api_host = self.meta.get("API", {}).get("AppHost", "")
        api_path = next_path.split("&offset=")[0]
        auth_token = self.meta.get("MEDIA_API", {}).get("token", "")
        args = zip(
            [api_host] * len(remaining_offset_points),
            [api_path] * len(remaining_offset_points),
            [auth_token] * len(remaining_offset_points),
            remaining_offset_points,
        )
        with ThreadPool(self.pool_size) as p:
            results = p.starmap(self._get_page_reviews, args)
            for _, reviews in results:
                all_reviews.extend(reviews)

        all_reviews = [
            dict(id=item.get("id", ""), **item.get("attributes", {}))
            for item in all_reviews
        ]

        return all_reviews


def crawl(url, step_detail, lang="en", batch=None, step=None, session=None):
    from_page, to_page = list(map(int, step_detail.paging.split(":")))
    total_pages = to_page - from_page + 1
    page_exceptions = []
    error_pages = []
    step_reviews = []

    # Crawl reviews
    try:
        country_code = str(step_detail.lang).strip()
        app_store = AppStore()
        app_store.load(apps_url=url, country_code=country_code)

        step_reviews = app_store.get_all_reviews(
            apps_url=url,
            country_code=country_code,
            from_page=from_page,
            to_page=to_page,
        )
    except Exception as e:
        page_exceptions = [e]

    # Publish progress using PubSub
    logger.info(
        f"Publishing crawling step detail {step_detail.step_detail_id} progress..."
    )
    data = {
        "type": "crawl",
        "event": "progress",
        "batch": batch.to_json(),
        "step": step.to_json(),
        "step_detail": step_detail.to_json(),
        "progress": {"total": total_pages, "current": total_pages},
    }
    data = json.dumps(data)
    data = data.encode("utf-8")
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        config.GCP_PROJECT_ID, config.GCP_PUBSUB_TOPIC_PROGRESS
    )
    future = publisher.publish(topic_path, data)
    future.result()

    # save crawled item count
    step_detail.item_count = len(step_reviews)
    session.commit()

    # write local csv
    the_df = pd.DataFrame(step_reviews)
    output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
    the_df.to_csv(output_file, sep=DELIMITER, index=False)

    # upload csv to GCS
    *path, filename = output_file.split("/")

    dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
    upload_google_storage(output_file, dst_file)
    app_store.logger.info(f"Uploaded file to gcs: {dst_file}")

    # Log crawl statistics to BigQuery
    log_crawl_stats(batch=batch, step_detail=step_detail, item_count=len(step_reviews))

    return page_exceptions, total_pages, error_pages


def crawl_review_stats(
        url, step_detail, lang="en", batch=None, step=None, session=None
):
    """
    Only crawl review stats for one country on provided URL when user create a company.
    Data of review stats is just for showing in UI, no need to aggregate from all countries.
    """
    app_store = AppStore()
    app_store.logger.info(
        f"Crawling apple store review_stats on url: {url} - step_detail_id: {step_detail.step_detail_id}"
    )

    page_exceptions = []
    error_pages = []
    try:
        review_stats = app_store.get_review_stats(apps_url=url)

        # save crawled item count
        step_detail.item_count = 1
        session.commit()

        # write local csv
        the_df = pd.DataFrame([review_stats])
        output_file = f"{config.OUTPUT_DIR}/{step_detail.step_detail_name.strip()}.csv"
        the_df.to_csv(output_file, sep=DELIMITER, index=False)

        # upload csv to GCS
        *path, filename = output_file.split("/")

        dst_file = f"crawl/{batch.batch_name.strip()}/{filename}"
        upload_google_storage(output_file, dst_file)
        app_store.logger.info(f"Uploaded file to gcs: {dst_file}")

        # Log crawl statistics to BigQuery
        log_crawl_stats(
            batch=batch,
            step_detail=step_detail,
            item_count=int(review_stats["total_ratings"]),
        )

    except Exception as e:
        page_exceptions.append(e)
        error_pages.append(0)

    return page_exceptions, 1, error_pages


@auto_session
def applestore_scraper(url, step_detail, batch=None, step=None, session=None, **kwargs):
    meta_data = step_detail.meta_data
    if not meta_data or meta_data.get("data_type") == "review":
        # handle crawl reviews
        return crawl(
            url, step_detail, batch=batch, step=step, session=session, **kwargs
        )
    elif meta_data.get("data_type") == "review_stats":
        return crawl_review_stats(
            url, step_detail, batch=batch, step=step, session=session, **kwargs
        )
    else:
        raise Exception(f"Not supported type={meta_data}")


@auto_session
def execute_task(task, session=None):
    url = task.get("url")
    ids = task.get("ids")
    crawler = applestore_scraper

    for step_detail_id in ids:
        step_detail = session.query(ETLStepDetail).get(step_detail_id)

        # Write crawling status to task db
        step_detail.status = "running"
        step_detail.updated_at = dt.datetime.today()
        session.commit()

        batch = session.query(ETLCompanyBatch).get(step_detail.batch_id)
        step = session.query(ETLStep).get(step_detail.step_id)

        # Crawl data from source website
        try:
            country_code = re.search(r"/([a-zA-Z]+)/app/", url).group(1).lower()
            page_exceptions, total_pages, error_pages = crawler(
                url,
                step_detail,
                lang=country_code,
                batch=batch,
                step=step,
                session=session,
            )

            # Publish Pub/Sub
            logger.info(
                f"Publishing crawled step detail {step_detail.step_detail_id} to Pub/Sub..."
            )
            data = {
                "type": "crawl",
                "event": "finish",
                "batch": batch.to_json(),
                "step": step.to_json(),
                "step_detail": step_detail.to_json(),
            }
            data = json.dumps(data)
            data = data.encode("utf-8")
            # create a new publisher everytime a message is to be published
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(
                config.GCP_PROJECT_ID, config.GCP_PUBSUB_TOPIC_AFTER_TASK
            )
            logger.info(f"Publish message to topic: {topic_path} - payload: {data}")
            future = publisher.publish(topic_path, data)
            result = future.result()
            logger.info(f"Message published to Pub/Sub {result}")

            if page_exceptions:
                utils.publish_step_detail_error(
                    batch=batch,
                    step=step,
                    step_detail=step_detail,
                    error_time=dt.datetime.now(),
                    page_exceptions=page_exceptions,
                    total_pages=total_pages,
                    error_pages=error_pages,
                    session=session,
                    logger=logger,
                )

        except Exception as e:
            logger.exception(
                f"Unable to crawl step detail {step_detail.step_detail_id} due to error",
                e,
            )
            data = {
                "type": "crawl",
                "event": "fail",
                "batch": batch.to_json(),
                "step": step.to_json(),
                "step_detail": step_detail.to_json(),
            }
            data = json.dumps(data)
            data = data.encode("utf-8")
            # create a new publisher everytime a message is to be published
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(
                config.GCP_PROJECT_ID, config.GCP_PUBSUB_TOPIC_AFTER_TASK
            )
            logger.info(f"Publish message to topic: {topic_path} - payload: {data}")
            future = publisher.publish(topic_path, data)
            result = future.result()
            logger.info(f"Message published to Pub/Sub {result}")


def upload_google_storage(src_file, dst_file):
    client = storage.Client()
    bucket = client.get_bucket(config.GCP_STORAGE_BUCKET)
    blob = bucket.blob(dst_file)
    blob.upload_from_filename(src_file)
