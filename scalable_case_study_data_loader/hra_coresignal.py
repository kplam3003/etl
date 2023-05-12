import sys

sys.path.append("../")
import config

import json
import pymongo

from datetime import datetime
from typing import List, Dict
from google.cloud import bigquery
from google.cloud.bigquery.table import TableReference
from pymongo.database import Database
from app import logger
from dateutil import parser


def _parse_date_from_string(
    date_string: str
):
    if date_string is None or date_string == "None":
        return None
    
    # will skip if date_string is already in YYYY-MM-DD format
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return date_string
    except Exception:
        try:
            return parser.parse(date_string, fuzzy=True, dayfirst=True).strftime("%Y-%m-%d")
        except Exception:
            return None


def _get_coresignal_company_ids(
    mongodb: Database, company_datasource_ids: List[int]
) -> Dict[int, int]:
    """
    Get 1:1 relationship from company datasource ID to coresignal ID using `coresignal_company_datasource` collection.
    """
    results = mongodb[config.MONGODB_CORESIGNAL_COMPANY_DATASOURCE].find(
        filter={"company_datasource_id": {"$in": company_datasource_ids}},
        projection={"_id": 0, "company_datasource_id": 1, "coresignal_company_id": 1},
    )

    mapper: Dict[int, int] = {}
    for item in results:
        company_datasource_id: int = item["company_datasource_id"]
        coresignal_company_id: int = item["coresignal_company_id"]
        if company_datasource_id in mapper:
            continue
        mapper[company_datasource_id] = coresignal_company_id

    return mapper


def _load_coresignal_stats(
    case_study_id: int,
    case_study_name: str,
    company_datasource_id: int,
    nlp_pack: str,
    mongodb: Database,
) -> None:
    """
    Check and load data from MongoDB `coresignal_stats` into BigQuery `staging.coresignal_stats`.
    """
    # Check if coresignal_stats already loaded
    query = f"""
        select count(*)
        from `{config.GCP_BQ_TABLE_CORESIGNAL_STATS}`
        where case_study_id = {case_study_id}
            and company_datasource_id = {company_datasource_id};
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()
    for row in rows:
        if row[0] != 0:
            return 0

    # Load data from MongoDB
    results = (
        mongodb[config.MONGODB_CORESIGNAL_STATS]
        .find(
            filter={"company_datasource_id": company_datasource_id},
            projection={"_id": 0, "raw": 1},
        )
        .sort("data_version", pymongo.DESCENDING)
        .limit(1)
    )
    result: dict = list(results)[0]

    # Insert data into BigQuery
    errors = bq_client.insert_rows_json(
        table=config.GCP_BQ_TABLE_CORESIGNAL_STATS,
        json_rows=[
            {
                "case_study_id": case_study_id,
                "case_study_name": case_study_name,
                "company_datasource_id": company_datasource_id,
                "nlp_pack": nlp_pack,
                "coresignal_company_id": result["raw"]["id"],
                "url": result["raw"]["url"],
                "name": result["raw"]["name"],
                "website": result["raw"]["website"],
                "size": result["raw"]["size"],
                "industry": result["raw"]["industry"],
                "followers": result["raw"]["followers"],
                "founded": result["raw"]["founded"],
                "created": result["raw"]["created"],
                "last_updated": result["raw"]["last_updated"],
                "type": result["raw"]["type"],
                "employees_count": result["raw"]["employees_count"],
                "headquarters_country_parsed": result["raw"][
                    "headquarters_country_parsed"
                ],
                "company_shorthand_name": result["raw"]["company_shorthand_name"],
            }
        ],
    )
    assert len(errors) == 0, json.dumps(errors)

    return None


def _load_coresignal_company_datasource(
    case_study_id: int,
    company_datasource: dict,
    mongodb: Database,
) -> List[int]:
    """
    If there is no data on `coresignal_company_datasource` for `case_study_id`,
    then load data from MongoDB, otherwise do nothing.
    """
    company_datasource_id = int(company_datasource["company_datasource_id"])
    company_id = int(company_datasource["company_id"])
    company_name = str(company_datasource["company_name"])
    source_id = int(company_datasource["source_id"])
    source_name = str(company_datasource["source_name"])

    # Check `coresignal_company_datasource` by selecting `coresignal_member_ids`
    coresignal_member_ids: List[int] = []
    query = f"""
        select coresignal_member_ids
        from `{config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE}`
        where case_study_id = {case_study_id} and company_datasource_id = {company_datasource_id};
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()
    if rows.total_rows != 0:
        coresignal_member_ids = list(rows)[0][0]
        return coresignal_member_ids

    # Get latest `data_version`
    data_version = 0
    results = (
        mongodb[config.MONGODB_CORESIGNAL_COMPANY_DATASOURCE]
        .find(
            filter={"company_datasource_id": company_datasource_id},
            projection={"_id": -1, "data_version": 1},
        )
        .sort("data_version", pymongo.DESCENDING)
        .limit(1)
    )
    results = list(results)
    if len(results) == 0:
        return []
    else:
        data_version = int(results[0]["data_version"])

    # Load data from MongoDB
    dt_now = datetime.utcnow().isoformat()
    results = mongodb[config.MONGODB_CORESIGNAL_COMPANY_DATASOURCE].find(
        filter={
            "company_datasource_id": company_datasource_id,
            "data_version": data_version,
        },
        projection={"coresignal_company_id": 1, "coresignal_member_ids": 1},
    )

    coresignal_company_id: int = 0
    for item in results:
        coresignal_company_id = int(item["coresignal_company_id"])
        coresignal_member_ids.extend(item["coresignal_member_ids"])
    new_row = {
        "case_study_id": case_study_id,
        "company_datasource_id": company_datasource_id,
        "company_id": company_id,
        "company_name": company_name,
        "source_id": source_id,
        "source_name": source_name,
        "coresignal_company_id": coresignal_company_id,
        "coresignal_member_ids": coresignal_member_ids,
        "created_at": dt_now,
    }

    errors = bq_client.insert_rows_json(
        table=config.GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE,
        json_rows=[new_row],
    )
    assert len(errors) == 0, json.dumps(errors)
    return coresignal_member_ids


def _load_coresignal_employees(
    mongodb: Database,
    case_study_id: int,
    case_study_name: str,
    company_datasource_id: int,
    coresignal_member_ids: List[int],
) -> int:
    """
    Load data into `coresignal_employees`.
    """
    # Check if employees already loaded
    query = f"""
        select count(*)
        from `{config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES}`
        where case_study_id = {case_study_id}
            and company_datasource_id = {company_datasource_id};
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()
    for row in rows:
        if row[0] != 0:
            return 0

    # Load data from MongoDB
    employees = []
    batch_size = 1000
    for i in range(0, len(coresignal_member_ids), batch_size):
        sub_ids = coresignal_member_ids[i : i + batch_size]
        results = mongodb[config.MONGODB_CORESIGNAL_EMPLOYEES].find(
            filter={"coresignal_id": {"$in": sub_ids}},
            projection={
                "_id": 0,
                # general
                "coresignal_id": 1,
                "name": 1,
                "title": 1,
                "url": 1,
                "industry": 1,
                "created": "$coresignal_created",
                "last_updated": "$coresignal_last_updated",
                "google_place_id": 1,
                "google_country": 1,
                "google_admin1": 1,
                "country": 1,
                "connection_count": 1,
            },
        )
        employees.extend(results)

    # Load google locations from MongoDB
    location_ids: List[str] = [
        emp["google_place_id"] for emp in employees if emp.get("google_place_id")
    ]
    location_ids = list(set(location_ids))
    google_locations = {}
    for i in range(0, len(location_ids), batch_size):
        sub_ids = location_ids[i : i + batch_size]
        results = mongodb[config.MONGODB_GOOGLE_MAPS_PLACES_CACHE].find(
            filter={"place_id": {"$in": sub_ids}},
            projection={"_id": 0, "place_id": 1, "address_components": 1},
        )
        for place in results:
            google_locations[place["place_id"]] = place

    # Prepare data before insert
    dt_now = datetime.utcnow().isoformat()
    for emp in employees:
        emp["coresignal_employee_id"] = emp.pop("coresignal_id")
        emp["case_study_id"] = case_study_id
        emp["case_study_name"] = case_study_name
        emp["company_datasource_id"] = company_datasource_id
        emp["created_at"] = dt_now
        if "google_place_id" in emp:
            place_id = emp.pop("google_place_id")
            address_components = []
            if place_id in google_locations:
                address_components = google_locations[place_id]["address_components"]
            emp["google_address_components"] = [
                {
                    "long_name": component.get("long_name"),
                    "short_name": component.get("short_name"),
                    "types": component.get("types"),
                }
                for component in address_components
            ]

    # Insert into `coresignal_employees`
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    tb_ref = TableReference.from_string(
        table_id=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES,
        default_project=config.GCP_PROJECT_ID,
    )
    table = bq_client.get_table(tb_ref)
    job_config = bigquery.LoadJobConfig(schema=table.schema)
    load_job = bq_client.load_table_from_json(
        destination=tb_ref,
        json_rows=employees,
        job_config=job_config,
    )
    _ = load_job.result()

    return len(employees)


def _load_coresignal_employees_experiences(
    mongodb: Database,
    case_study_id: int,
    company_datasource_id: int,
    company_datasource_ids: List[int],
    target_company_datasource_id: int,
    coresignal_member_ids: List[int],
) -> int:
    """
    Load data into `coresignal_employees_experiences`.
    """
    # Check if employees experiences already loaded
    query = f"""
        select count(*)
        from `{config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES}`
        where case_study_id = {case_study_id}
            and company_datasource_id = {company_datasource_id};
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()
    for row in rows:
        if row[0] != 0:
            return 0

    # Get Coresignal ID for companies in a case study
    coresignal_company_ids = _get_coresignal_company_ids(
        mongodb=mongodb, company_datasource_ids=company_datasource_ids
    )
    target_coresignal_company_id = coresignal_company_ids[target_company_datasource_id]
    cohort_coresignal_company_ids = list(coresignal_company_ids.values())

    # Load data employees from MongoDB
    employees = []
    batch_size = 1000
    for i in range(0, len(coresignal_member_ids), batch_size):
        sub_ids = coresignal_member_ids[i : i + batch_size]
        results = mongodb[config.MONGODB_CORESIGNAL_EMPLOYEES].find(
            filter={"coresignal_id": {"$in": sub_ids}},
            projection={
                "_id": 0,
                "coresignal_id": 1,
                "member_experience_collection.id": 1,
                "member_experience_collection.title": 1,
                "member_experience_collection.company_id": 1,
                "member_experience_collection.company_name": 1,
                "member_experience_collection.company_url": 1,
                "member_experience_collection.date_from": 1,
                "member_experience_collection.date_to": 1,
                "member_experience_collection.duration": 1,
                "member_experience_collection.previous_coresignal_company_id": 1,
                "member_experience_collection.previous_company_name": 1,
                "member_experience_collection.previous_experience_id": 1,
                "member_experience_collection.next_coresignal_company_id": 1,
                "member_experience_collection.next_company_name": 1,
                "member_experience_collection.next_experience_id": 1,
                "member_experience_collection.google_place_id": 1,
                "member_experience_collection.google_country": 1,
                "member_experience_collection.google_admin1": 1,
                "member_experience_collection.is_starter": 1,
                "member_experience_collection.is_leaver": 1,
                "member_education_collection.id": 1,
                "member_education_collection.date_from": 1,
                "member_education_collection.date_to": 1,
            },
        )
        employees.extend(results)

    # Load google locations from MongoDB
    location_ids: List[str] = [
        item["google_place_id"]
        for emp in employees
        for item in emp["member_experience_collection"]
        if "google_place_id" in item
    ]
    location_ids = list(set(location_ids))
    google_locations = {}

    for i in range(0, len(location_ids), batch_size):
        sub_ids = location_ids[i : i + batch_size]
        results = mongodb[config.MONGODB_GOOGLE_MAPS_PLACES_CACHE].find(
            filter={"place_id": {"$in": sub_ids}},
            projection={"_id": 0, "place_id": 1, "address_components": 1},
        )
        for place in results:
            google_locations[place["place_id"]] = place

    # Prepare data before insert
    employees_experiences = []
    for emp in employees:
        emp_exp_coll: List[dict] = emp["member_experience_collection"]
        for item in emp_exp_coll:
            emp_exp = {
                "coresignal_employee_id": emp["coresignal_id"],
                "case_study_id": case_study_id,
                "company_datasource_id": company_datasource_id,
                **item,
            }
            emp_exp["experience_id"] = emp_exp.pop("id")
            emp_exp["coresignal_company_id"] = emp_exp.pop("company_id")

            emp_exp["date_to"] = _parse_date_from_string(
                date_string=emp_exp["date_to"]
            )
        
            emp_exp["date_from"] = _parse_date_from_string(
                date_string=emp_exp["date_from"]
            )

            # Google place fields
            if "google_place_id" in emp_exp:
                place_id = emp_exp.pop("google_place_id")
                address_components = []
                if place_id in google_locations:
                    address_components = google_locations[place_id][
                        "address_components"
                    ]
                emp_exp["google_address_components"] = [
                    {
                        "long_name": component.get("long_name"),
                        "short_name": component.get("short_name"),
                        "types": component.get("types"),
                    }
                    for component in address_components
                ]

            # `is_target` field
            emp_exp["is_target"] = (
                True
                if (target_coresignal_company_id == emp_exp["coresignal_company_id"])
                else False
            )

            # `is_cohort` field
            emp_exp["is_cohort"] = (
                True
                if (emp_exp["coresignal_company_id"] in cohort_coresignal_company_ids)
                else False
            )

            employees_experiences.append(emp_exp)
            
    # Insert into `coresignal_employees_experiences`
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    tb_ref = TableReference.from_string(
        table_id=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES,
        default_project=config.GCP_PROJECT_ID,
    )
    table = bq_client.get_table(tb_ref)
    job_config = bigquery.LoadJobConfig(schema=table.schema)
    load_job = bq_client.load_table_from_json(
        destination=tb_ref,
        json_rows=employees_experiences,
        job_config=job_config,
    )
    _ = load_job.result()

    return len(employees_experiences)


def _load_coresignal_employees_education(
    mongodb: Database,
    case_study_id: int,
    company_datasource_id: int,
    coresignal_member_ids: List[int],
) -> int:
    """
    Load data into `coresignal_employees_education`.
    """
    # Check if employees education already loaded
    query = f"""
        select count(*)
        from `{config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EDUCATION}`
        where case_study_id = {case_study_id}
            and company_datasource_id = {company_datasource_id};
    """
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bq_client.query(query)
    rows = query_job.result()
    for row in rows:
        if row[0] != 0:
            return 0

    # Load data from MongoDB
    employees = []
    batch_size = 1000
    for i in range(0, len(coresignal_member_ids), batch_size):
        sub_ids = coresignal_member_ids[i : i + batch_size]
        results = mongodb[config.MONGODB_CORESIGNAL_EMPLOYEES].find(
            filter={"coresignal_id": {"$in": sub_ids}},
            projection={
                "_id": 0,
                "coresignal_id": 1,
                "member_education_collection.id": 1,
                "member_education_collection.title": 1,
                "member_education_collection.subtitle": 1,
                "member_education_collection.description": 1,
                "member_education_collection.date_from": 1,
                "member_education_collection.date_to": 1,
                "member_education_collection.school_url": 1,
            },
        )
        employees.extend(results)

    # Prepare data before insert
    employees_education = []
    for emp in employees:
        for item in emp["member_education_collection"]:
            emp_edu = {
                "coresignal_employee_id": emp["coresignal_id"],
                "case_study_id": case_study_id,
                "company_datasource_id": company_datasource_id,
                **item,
            }
            emp_edu["education_id"] = emp_edu.pop("id")

            emp_edu["date_to"] = _parse_date_from_string(
                date_string=emp_edu["date_to"]
            )
        
            emp_edu["date_from"] = _parse_date_from_string(
                date_string=emp_edu["date_from"]
            )
            
            employees_education.append(emp_edu)

    # Insert into `coresignal_employees_education`
    bq_client = bigquery.Client(project=config.GCP_PROJECT_ID)
    tb_ref = TableReference.from_string(
        table_id=config.GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EDUCATION,
        default_project=config.GCP_PROJECT_ID,
    )
    table = bq_client.get_table(tb_ref)
    job_config = bigquery.LoadJobConfig(schema=table.schema)
    load_job = bq_client.load_table_from_json(
        destination=tb_ref,
        json_rows=employees_education,
        job_config=job_config,
    )
    _ = load_job.result()

    return len(employees_education)
