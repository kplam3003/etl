import json
import datetime as dt
import requests
import utils
import config

from typing import Dict, List, Any, Set, Tuple, Optional
from urllib.parse import urlparse
from pymongo import MongoClient, DESCENDING
from database import ETLCompanyBatch, ETLStepDetail


REQUEST_ACCEPT = "application/json"


def array_split(input: List[Any], sub_size: int) -> List[List[Any]]:
    if sub_size > len(input):
        return [input]
    sub_arrays = []
    sub_array_count = len(input) // sub_size
    if len(input) % sub_size != 0:
        sub_array_count += 1
    for i in range(sub_array_count):
        sub_array = input[i * sub_size : i * sub_size + sub_size]
        sub_arrays.append(sub_array)

    return sub_arrays


def check_crawled_employee(coresignal_ids: List[int]) -> dict:
    """
    Get `last_updated` from MongoDB
    """
    client = MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
    )
    db = client.get_database(config.MONGODB_DATABASE_NAME)
    coll = db[config.MONGODB_CORESIGNAL_EMPLOYEES_COLLECTION]

    # Split `coresignal_ids` into smaller chunks for querying
    # Server supports BSON document sizes up to 16777216 bytes (~1.5M elements in `$in`).
    chunks = array_split(input=coresignal_ids, sub_size=int(1e6))
    crawled_employees = {}
    project = {"_id": 0, "coresignal_id": 1, "coresignal_last_updated": 1}
    for chunk in chunks:
        _filter = {"coresignal_id": {"$in": chunk}}
        results = coll.find(filter=_filter, projection=project)
        crawled_employees.update(
            {item["coresignal_id"]: item["coresignal_last_updated"] for item in results}
        )

    client.close()
    return crawled_employees


def fetch_coresignal_company_ids(leo_company_id: int) -> Set[int]:
    """
    Fetch Coresignal Company IDs that has the same input Leonardo Company ID
    """
    client = MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
    )
    db = client.get_database(config.MONGODB_DATABASE_NAME)
    coll = db[config.MONGODB_CORESIGNAL_STATS_COLLECTION]
    crs = coll.find(
        {"company_id": leo_company_id},
        projection={"_id": 0, "company_id": 1, "coresignal_id": 1},
    )
    coresignal_company_ids = set()
    for d in crs:
        coresignal_company_ids.add(int(d["coresignal_id"]))

    client.close()
    return coresignal_company_ids


def fetch_coresignal_company_profile(
    company_shorthand_name: str, company_id: int
) -> Dict[str, Any]:
    """
    Fetch Coresignal Company Profile from Company S
    """
    client = MongoClient(
        config.MONGODB_DATABASE_URI,
        authMechanism="MONGODB-X509",
        tls=True,
        tlsCertificateKeyFile=config.MONGODB_DATABASE_KEY_FILE,
        tlsCAFile=config.MONGODB_DATABASE_ROOT_CA,
        tlsAllowInvalidHostnames=True,
    )
    db = client.get_database(config.MONGODB_DATABASE_NAME)
    coll = db[config.MONGODB_CORESIGNAL_STATS_COLLECTION]
    company_data = coll.find_one(
        {"coresignal_shorthand_name": company_shorthand_name, "company_id": company_id},
        sort=[("data_version", DESCENDING)],
        projection={
            "_id": 0,
            "company_id": 1,
            "coresignal_id": 1,
            "employees_count": 1,
        },
    )

    client.close()
    return company_data


def search_employee(
    coresignal_company_id: int,
    company_website_url: Optional[str],
    after: Optional[int] = None,
    employee_ids: List[int] = None,
) -> Tuple[List[int], Optional[str]]:
    """
    Using /v1​/linkedin/member/search​/filter API to search all employee of a company.
    The next page of results can be retrieved by submitting the original request with an additional parameter: after=[X-next-page-after]
    This API is FREE!
    """
    if employee_ids is None:
        employee_ids = []

    # Prepare request params
    api_url = "https://api.coresignal.com/cdapi/v1/linkedin/member/search/filter"
    if after:
        api_url = f"{api_url}?after={after}"

    headers = {
        "accept": REQUEST_ACCEPT,
        "Content-Type": REQUEST_ACCEPT,
        "Authorization": f"Bearer {config.CORESIGNAL_JWT}",
    }

    payload = {
        "experience_company_id": coresignal_company_id,
        "active_experience": False,
        "experience_deleted": False,
    }
    if company_website_url:
        payload.pop("experience_company_id")
        payload["experience_company_website_url"] = company_website_url

    # Make request to Coresignal's API
    res = requests.post(url=api_url, headers=headers, json=payload)
    if not res.ok:
        raise ValueError(
            f"Coresignal problem: cdapi/v1/linkedin/member/search/filter. {res.reason}"
        )
    employee_ids.extend(res.json())

    if str(res.headers.get("x-next-page-after")).isdigit():
        after = int(res.headers["x-next-page-after"])
    else:
        return (list(set(employee_ids)), after)

    return search_employee(
        coresignal_company_id=coresignal_company_id,
        company_website_url=company_website_url,
        after=after,
        employee_ids=employee_ids,
    )


def generate_stats_step(
    batch: ETLCompanyBatch, company_website_url: Optional[str], session=None
):
    logger = utils.load_logger()
    today = dt.datetime.today()
    etl_step = utils.load_etl_step(batch, session=session)

    step_detail = ETLStepDetail()
    step_detail.request_id = batch.request_id
    step_detail.step_detail_name = (
        f"stats-{etl_step.step_name}-{today.strftime('%Y%m%d')}-f{1:03d}"
    )
    step_detail.status = "waiting"
    step_detail.step_id = etl_step.step_id
    step_detail.batch_id = batch.batch_id
    step_detail.file_id = 1
    step_detail.paging = "1:1"
    step_detail.created_at = today
    step_detail.updated_at = today
    step_detail.progress_total = 1
    step_detail.progress_current = 0
    step_detail.lang = "en"
    step_detail.meta_data = {
        "version": "1.0",
        "data_type": "coresignal_stats",
        "url": batch.url,
        "company_website_url": company_website_url,
        "lang": "eng",
    }
    session.add(step_detail)
    session.commit()

    payload = {
        "batch_id": batch.batch_id,
        "url": batch.url,
        "ids": [step_detail.step_detail_id],
        "strategy": "coresignal_stats",
    }
    payload = json.dumps(payload)
    utils.publish_pubsub(
        config.GCP_PUBSUB_TOPIC_CRAWL, payload.encode("utf-8"), logger=logger
    )


def generate_employee_steps(
    batch: ETLCompanyBatch, company_website_url: Optional[str], session=None
):
    logger = utils.load_logger()
    today = dt.datetime.today()
    company_shorthand_name = urlparse(url=str(batch.url)).path.split("/")[2]
    company_id = batch.company_id

    # Get company stats
    coresignal_url = f"https://api.coresignal.com/cdapi/v1/linkedin/company/collect/{company_shorthand_name}"
    stats_res = requests.get(
        url=coresignal_url,
        headers={
            "accept": REQUEST_ACCEPT,
            "Authorization": f"Bearer {config.CORESIGNAL_JWT}",
        },
    )

    # NOTE: fetch from coresignal may fail due to insufficient credits
    # => try to also get from existing data in database
    if stats_res.ok:
        stats_data = stats_res.json()
    else:
        log_msg = f"Can not get coresignal data for {company_shorthand_name}: {stats_res.reason}. Will try to fetch from database."
        logger.warning(log_msg)
        stats_data = fetch_coresignal_company_profile(
            company_shorthand_name, company_id
        )

    if not stats_data:
        log_msg = f"Can not get coresignal data for {company_shorthand_name} from either Coresignal nor database"
        logger.exception(log_msg)
        raise ValueError(log_msg)

    # Update batch meta_data
    employees_count = (
        int(stats_data["employees_count"]) if stats_data["employees_count"] else 0
    )
    coresignal_company_id = int(stats_data["id"])
    logger.info(f"Total items found for url {batch.url}: {employees_count}")
    meta_data = batch.meta_data or {}
    meta_data["paging_info"] = {
        "total": employees_count,
        "coresignal_company_id": coresignal_company_id,
    }
    batch.meta_data = meta_data
    session.commit()

    # Make step details.
    ## get coresignal_company_ids that belongs to this leo company_id
    coresignal_company_ids_set = fetch_coresignal_company_ids(batch.company_id)
    coresignal_company_ids_set.add(coresignal_company_id)
    coresignal_company_ids = list(coresignal_company_ids_set)

    ## search employees for this coresignal company_id
    employee_ids, _ = search_employee(
        coresignal_company_id=coresignal_company_id,
        company_website_url=company_website_url,
    )
    steps = array_split(employee_ids, config.ROWS_PER_STEP)
    data_type = batch.meta_data.get("data_type")
    strategy = "coresignal_employees"
    lang = "en"

    etl_step = utils.load_etl_step(batch, session=session)

    logger.info(f"Generating step details, there are {len(steps)} steps")
    for step_num, sub_employee_ids in enumerate(steps):
        step_detail = ETLStepDetail()
        step_detail.request_id = batch.request_id
        step_detail.step_detail_name = f"{data_type}-{etl_step.step_name}-{today.strftime('%Y%m%d')}-{step_num}".strip()
        step_detail.status = "waiting"
        step_detail.step_id = etl_step.step_id
        step_detail.batch_id = batch.batch_id
        step_detail.file_id = step_num + 1
        step_detail.paging = ":"
        step_detail.created_at = today
        step_detail.updated_at = today
        step_detail.progress_total = len(sub_employee_ids)
        step_detail.progress_current = 0
        step_detail.lang = lang
        step_detail.meta_data = {
            "version": "1.0",
            "data_type": data_type,
            "url": batch.url,
            "lang": lang,
            "coresignal_company_id": coresignal_company_id,
            "coresignal_company_ids": coresignal_company_ids,
            "coresignal_employee_ids": sub_employee_ids,
        }
        session.add(step_detail)
        session.commit()

        payload = json.dumps(
            {
                "url": batch.url,
                "batch_id": batch.batch_id,
                "strategy": strategy,
                "ids": [step_detail.step_detail_id],
            }
        )
        payload = payload.encode("utf-8")
        utils.publish_pubsub(config.GCP_PUBSUB_TOPIC_CRAWL, payload, logger=logger)

    logger.info("Assign task completed")
