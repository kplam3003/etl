import json
import asyncio
import aiohttp
import random
import requests

from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, unquote, urlencode
from typing import List, Tuple, Union
from tenacity import retry, wait, stop, retry_if_exception_type, RetryError

from utils import load_logger, array_split
from config import APPLE_COUNTRIES, LUMINATI_HTTP_PROXY


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

    def __init__(self) -> None:
        self.logger = load_logger()
        self.country_codes = APPLE_COUNTRIES
        self.proxy = LUMINATI_HTTP_PROXY

    @staticmethod
    def parse_country_code(api_path: str) -> str:
        path_parts = api_path.split("/")
        code: str = path_parts[3]
        return code

    @staticmethod
    def parse_apps_url(apps_url: str) -> dict:
        parse_results = urlparse(apps_url)
        if parse_results.netloc != "apps.apple.com":
            raise ValueError(f"{apps_url} is not valid URL!")

        parts = parse_results.path.split("/")
        country_code = parts[1]
        id_part = parts[-1]

        return {"country_code": country_code, "app_id": int(id_part.replace("id", ""))}

    @staticmethod
    def parse_rating_count(app_data: dict) -> int:
        data: dict = app_data.get("data", [])[0]
        rating_count = (
            data.get("attributes", {}).get("userRating", {}).get("ratingCount", 0)
        )
        return int(rating_count)

    @retry(
        wait=wait.wait_exponential(max=30),
        stop=stop.stop_after_attempt(5),
        retry=retry_if_exception_type(AssertionError),
    )
    def get_meta_cache(self, apps_url: str) -> Tuple[dict, dict]:
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

    @retry(
        wait=wait.wait_exponential(max=30),
        stop=stop.stop_after_attempt(5),
        retry=retry_if_exception_type(AssertionError),
    )
    async def _request(
        self,
        session: aiohttp.ClientSession,
        url: str,
        auth_token: str,
    ) -> aiohttp.ClientResponse:
        print(f"{datetime.now().isoformat()}: get(url={url!r})")

        headers = {
            "user-agent": random.choice(AppStore.USER_AGENTS),
            "authorization": f"Bearer {auth_token}",
            "origin": "https://apps.apple.com",
        }
        response = await session.get(
            url=url,
            headers=headers,
            proxy=self.proxy,
            verify_ssl=False,
        )

        assert response.status in [200, 404], "Retrying: Invalid response"

        return response

    async def _requests(
        self,
        host: str,
        path: str,
        auth_token: str,
        params: dict,
        country_codes: List[str],
    ) -> Union[List[Tuple[str, dict]], None]:
        # Construct API URL for all countries
        code_idx = 3
        query_str = urlencode(params)
        path_parts = path.split("/")
        path_countries = [
            "/".join(path_parts[:code_idx] + [code] + path_parts[code_idx + 1 :])
            for code in country_codes
        ]
        api_urls = [f"{host}{item}&{query_str}" for item in path_countries]

        # Make concurrent requests
        session = aiohttp.ClientSession()
        tasks = [
            self._request(session=session, url=api_url, auth_token=auth_token)
            for api_url in api_urls
        ]

        # Get the response
        done, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

        # Extract data from responses
        results = []
        for task in done:
            try:
                response = task.result()
                if response.status != 200:
                    continue
                result = await response.json()
                results.append(
                    (
                        response.url.path,
                        result,
                    )
                )
            except RetryError:
                pass

        await session.close()
        return results

    def get_review_count(self, apps_url: str) -> List[Tuple[str, dict]]:
        meta, cache = self.get_meta_cache(apps_url=apps_url)
        api_host = meta.get("API", {}).get("AppHost", "")
        api_path = cache.get("href", "")
        auth_token = meta.get("MEDIA_API", {}).get("token", "")
        params = {
            "platform": "web",
            "additionalPlatforms": "appletv,ipad,iphone,mac",
            "extend": "customPromotionalText,customScreenshotsByType,description,developerInfo,distributionKind,editorialVideo,fileSizeByDevice,messagesScreenshots,privacy,privacyPolicyUrl,requirementsByDeviceFamily,supportURLForLanguage,versionHistory,websiteUrl,videoPreviewsByType",
            "include": "app-events,genres,developer,reviews,merchandised-in-apps,customers-also-bought-apps,developer-other-apps,top-in-apps,related-editorial-items",
            "limit[merchandised-in-apps]": 20,
            "omit[resource]": "autos",
            "meta": "robots",
            "sparseLimit[apps:related-editorial-items]": 20,
            "sparseLimit[apps:customers-also-bought-apps]": 20,
            "sparseLimit[apps:developer-other-apps]": 20,
        }

        results = []
        for codes in array_split(input=self.country_codes, sub_size=10):
            loop = asyncio.new_event_loop()
            batch_results = loop.run_until_complete(
                self._requests(
                    host=api_host,
                    path=api_path,
                    auth_token=auth_token,
                    params=params,
                    country_codes=codes,
                )
            )
            results.extend(batch_results)
            loop.run_until_complete(asyncio.sleep(0.250))
            loop.close()

        return results

    def get_review_stats(self, apps_url: str) -> dict:
        _, cache = self.get_meta_cache(apps_url=apps_url)
        user_rating: dict = cache.get("attributes", {}).get("userRating", {})
        rating_counts = {
            f"{idx+1} star": int(val)
            for idx, val in enumerate(user_rating.get("ratingCountList", []))
        }
        review_stats = {
            "total_ratings": user_rating.get("ratingCount"),
            **rating_counts,
        }

        return review_stats
