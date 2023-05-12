import datetime as dt
import sys

from sqlalchemy.sql.expression import over
sys.path.append('../')

import config

from database import (
    ETLRequest,
    ETLCompanyBatch,
    auto_session
)
from logger_global import logger


# GET
@auto_session
def get_etl_request(request_id, session=None):
    """
    get etl request dict given request id
    """
    etl_request = (
        session
        .query(ETLRequest)
        .filter(ETLRequest.request_id == request_id)
        .order_by(ETLRequest.request_id.desc())
        .first()
    )

    if etl_request:
        return etl_request.to_json()



# CREATE
@auto_session
def create_etl_request(
    request_type,
    payload,
    case_study_id=None, # legacy
    case_study_name=None,
    data_type=None, # legacy
    is_translation=None,
     # legacy
    session=None):
    """
    Create a new etl_request in MDM database
    """
    etl_request = ETLRequest(
        request_type=request_type,
        case_study_id=case_study_id,
        case_study_name=case_study_name,
        status=config.STATUS_WAITING,
        is_translation=is_translation,
        payload=payload,
        data_type=data_type,
    )
    session.add(etl_request)
    session.commit()

    logger.info(f"[create_etl_request] new etl_request created, request_id: {etl_request.request_id}")
    return etl_request.to_json()


@auto_session
def create_etl_company_batch(
    request_id,
    company_datasource_id,
    company_id,
    company_name,
    source_id,
    source_name, 
    source_type,
    batch_type,
    url,
    nlp_type,
    data_version,
    is_translation,
    source_code=None,
    session=None,
    nlp_pack=None # legacy
):
    """
    Create waiting batches for crawl/recrawl requests for a company_datasource
    """
    if source_code == None: # Will remove if deployed to PROD 
        if source_id == config.SOURCE_ID_GOOGLE_PLAY:
            source_code = "GooglePlay"
        elif source_id == config.SOURCE_ID_APPLE_STORE:
            source_code = "AppleStore"
        elif source_id == config.SOURCE_ID_CAPTERRA:
            source_code = "Capterra"
        elif source_id == config.SOURCE_ID_G2:
            source_code = "G2"
        elif source_id == config.SOURCE_ID_TRUSTPILOT:
            source_code = "Trustpilot"
        elif source_id == config.SOURCE_ID_GLASSDOOR:
            source_code = "Glassdoor"
        elif source_id == config.SOURCE_ID_INDEED:
            source_code = "Indeed"
        elif source_id == config.SOURCE_ID_LINKEDIN:
            source_code = "Linkedin"
        elif source_type and source_type.lower() == 'csv':
            source_code = f"{source_id}-CSV"
        else:
            raise Exception(f"[create_etl_company_batch] not support source_id: {source_id}")

    today = dt.datetime.utcnow()
    batch_name = f"{nlp_type}-{today.strftime('%Y%m%d%H%M%S')}-{company_id}-{source_id}"
    etl_company_batch = ETLCompanyBatch(
        request_id=request_id,
        batch_name=batch_name,
        status=config.STATUS_WAITING,
        company_datasource_id=company_datasource_id,
        company_id=company_id,
        company_name=company_name,
        source_id=source_id,
        source_name=source_name,
        source_code=source_code,
        source_type=source_type,
        url=url,
        created_at=today,
        updated_at=today,
        data_version=data_version,
        nlp_pack=nlp_pack,
        nlp_type=nlp_type,
        is_translation=is_translation,
        meta_data={
            "data_type": batch_type
        }
    )
    session.add(etl_company_batch)
    session.commit()

    logger.info(f"[create_etl_company_batch] row etl_company_batch created, "
                f"batch_id: {etl_company_batch.batch_id}")
    return etl_company_batch.to_json()


@auto_session
def update_etl_request_status(
    request_id,
    status,
    logger,
    session=None
):
    """
    Update status of existing etl_request
    """
    etl_request = (
        session
        .query(ETLRequest)
        .filter(ETLRequest.request_id == request_id)
        .first()
    )
    etl_request.status = status
    session.commit()

    logger.info(f"[update_etl_request_status] "
                f"request_id {etl_request} status updated to {status}")
    return etl_request.to_json()


@auto_session
def update_etl_company_batch_status(
    batch_id, 
    status,
    logger,
    session=None
):
    """
    Update status of created etl_company_datasource
    """
    etl_company_batch = (
        session
        .query(ETLCompanyBatch)
        .filter(ETLCompanyBatch.batch_id == batch_id)
        .first()
    )
    etl_company_batch.status = status
    session.commit()

    logger.info(f"[update_etl_company_batch_status] ",
                f"company_datasource_id {batch_id} status updated to {status}")
    return etl_company_batch.to_json()


