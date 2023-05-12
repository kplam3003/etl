from datetime import datetime
from urllib.parse import urlparse
import json
import re

from pymongo import errors as pm_errors
import gcsfs

import config
from core import common_util, etl_const


def transform_func_coresignal_stats(payload, item):
    item_tmp = {}
    # common metadata
    item_tmp["company_datasource_id"] = payload["batch"]["company_datasource_id"]
    item_tmp["created_at"] = datetime.utcnow().isoformat()
    item_tmp["source_id"] = payload["batch"]["source_id"]
    item_tmp["source_name"] = payload["batch"]["source_name"]
    item_tmp["company_id"] = payload["batch"]["company_id"]
    item_tmp["company_name"] = payload["batch"]["company_name"]
    item_tmp["request_id"] = payload["batch"]["request_id"]
    item_tmp["case_study_id"] = payload["request"]["case_study_id"]
    item_tmp["nlp_type"] = payload["batch"]["nlp_type"]
    item_tmp["batch_id"] = payload["batch"]["batch_id"]
    item_tmp["batch_name"] = payload["batch"]["batch_name"]
    item_tmp["file_name"] = payload["step_detail"]["step_detail_name"]
    item_tmp["step_id"] = payload["step_detail"]["step_id"]
    item_tmp["data_version"] = payload["batch"]["data_version"]

    # actual data, file after preprocess must have these fields
    item_tmp["stats_collection_date"] = item.get("__profile_date__")
    item_tmp["coresignal_name"] = item.get("coresignal_name")
    item_tmp["coresignal_shorthand_name"] = item.get("coresignal_shorthand_name")
    item_tmp["coresignal_id"] = item.get("coresignal_id")
    item_tmp["coresignal_created"] = item.get("coresignal_created")
    item_tmp["coresignal_last_updated"] = item.get("coresignal_last_updated")
    item_tmp["coresignal_deleted"] = item.get("coresignal_deleted")
    item_tmp["coresignal_last_updated_ux"] = item.get("coresignal_last_updated_ux")
    item_tmp["employees_count"] = item.get("employees_count")

    # parse and put raw data in a key to be used later, maybe
    item_tmp["raw"] = json.loads(item["data"])

    return [item_tmp]


def transform_func_coresignal_employees(payload, item):
    item_tmp = {}
    # common metadata
    item_tmp["company_datasource_id"] = payload["batch"]["company_datasource_id"]
    item_tmp["created_at"] = datetime.utcnow().isoformat()
    item_tmp["source_id"] = payload["batch"]["source_id"]
    item_tmp["source_name"] = payload["batch"]["source_name"]
    item_tmp["company_id"] = payload["batch"]["company_id"]
    item_tmp["company_name"] = payload["batch"]["company_name"]
    item_tmp["request_id"] = payload["batch"]["request_id"]
    item_tmp["case_study_id"] = payload["request"]["case_study_id"]
    item_tmp["nlp_type"] = payload["batch"]["nlp_type"]
    item_tmp["batch_id"] = payload["batch"]["batch_id"]
    item_tmp["batch_name"] = payload["batch"]["batch_name"]
    item_tmp["file_name"] = payload["step_detail"]["step_detail_name"]
    item_tmp["step_id"] = payload["step_detail"]["step_id"]
    item_tmp["data_version"] = payload["batch"]["data_version"]

    # actual data
    item_tmp.update(json.loads(item["data"]))

    return [item_tmp]

 
def _load_coresignal_linkedin_employee(payload, items, mongodb_db, logger):
    """Load coresignal linkedin employees into MongoDB"""
    # mongodb collections
    employee_collection = config.MONGODB_HR_CORESIGNAL_EMPLOYEE_COLLECTION
    mapping_collection = config.MONGODB_HR_CORESIGNAL_CD_MAPPING_COLLECTION
    # payload info
    company_datasource_id = payload["batch"]["company_datasource_id"]
    metadata = payload["step_detail"]["meta_data"]
    step_detail_id = payload["step_detail"]["step_detail_id"]
    batch_id = payload["batch"]["batch_id"]
    batch_name = payload["batch"]["batch_name"]
    data_version = payload["batch"]["data_version"]
    
    num_items = len(items)
    inserted_items_count = 0
    replaced_items_count = 0
    has_exception = False
    inserted_errors = []
    all_coresignal_member_ids = []
    logger.info(f"Start loading {num_items} items into {employee_collection}")
    for item in items:
        try:
            coresignal_member_id = item["coresignal_id"]
            all_coresignal_member_ids.append(item["coresignal_id"])

            result = mongodb_db[employee_collection].replace_one(
                filter={
                    "coresignal_id": coresignal_member_id
                },
                replacement=item,
                upsert=True
            )
            replaced_items_count += result.modified_count
            inserted_items_count += 1 if result.upserted_id is not None else 0

        except Exception as exc:
            has_exception = True
            inserted_errors.append(str(exc))
            logger.exception(
                f"Error while loading item {coresignal_member_id} into {employee_collection} - Error: {exc}"
            )

    logger.info(f"Inserted {inserted_items_count} items, replaced {replaced_items_count} items")

    # NOTE: also loads into mapping table for coresignal employee
    logger.info(
        f"Start loading mapping with {len(all_coresignal_member_ids)} "
        f"member ids into {mapping_collection}"
    )
    coresignal_company_id = metadata["coresignal_company_id"]
    cd_coresignal_mappings = {
        "company_datasource_id": company_datasource_id,
        "coresignal_company_id": coresignal_company_id,
        "coresignal_member_ids": all_coresignal_member_ids,
        "batch_id": batch_id,
        "batch_name": batch_name,
        "step_detail_id": step_detail_id,
        "data_version": data_version,
    }
    results = mongodb_db[config.MONGODB_HR_CORESIGNAL_CD_MAPPING_COLLECTION].insert(
        cd_coresignal_mappings
    )
    
    # LEON-127: export to csv
    export_to_gcs(payload, items, logger)

    return inserted_items_count, has_exception, inserted_errors


def export_to_gcs(payload, items, logger):
    try:
        # construct path
        company_datasource_id = payload["batch"]["company_datasource_id"]
        data_type = payload["payload"]["data_type"]
        company_name = payload["payload"]["company_name"].replace(" ", "_")
        company_shorthand_name = urlparse(
            url=str(payload["batch"]["url"])
        ).path.split("/")[2]
        company_name = re.sub(r"[^a-zA-Z0-9_]", "_", company_name)
        file_id = payload["step_detail"]["file_id"]
        
        file_prefix = "{}/{}/{}".format(
            config.GCP_STORAGE_BUCKET, 
            f"export/{data_type}",
            company_datasource_id
        )
        file_name = f"{company_name}-{company_shorthand_name}-{company_datasource_id}-{file_id:03d}.json"
        file_path = "{}/{}".format(file_prefix, file_name,)
                
        # delete current file and write to GCS
        fs = gcsfs.GCSFileSystem(project=config.GCP_PROJECT_ID)
        fs.rm(file_path)
        with fs.open(file_path, "w") as f:
            f.write(json.dumps(items))
        
        logger.info(f"Exported json to {file_path}")
        return True

    except Exception as error:
        logger.exception(f"Export json fail: {error}")
        return False