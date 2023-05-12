from copy import deepcopy
import sys
sys.path.append('../')
import datetime as dt

from sqlalchemy import and_

from database import (
    ETLCompanyDatasourceCommon,
    auto_session
)
import config
from logger_global import logger


# GET
@auto_session
def get_etl_company_datasource(company_datasource_id, data_type, session=None):
    """
    Get the newest record for a given company_datasource_id
    """
    etl_company_datasource = (
        session
        .query(ETLCompanyDatasourceCommon)
        .filter(
            and_(
                ETLCompanyDatasourceCommon.company_datasource_id == company_datasource_id,
                ETLCompanyDatasourceCommon.data_type == data_type
            )
        )
        .order_by(ETLCompanyDatasourceCommon.request_id.desc())
        .first()
    )

    if etl_company_datasource:
        return etl_company_datasource.to_json()


@auto_session
def get_etl_company_datasource_by_ids(company_datasource_ids, data_type, session=None):
    """
    Get the newest record for a given company_datasource_id
    """
    etl_company_datasources = (
        session
        .query(ETLCompanyDatasourceCommon)
        .filter(
            and_(
                ETLCompanyDatasourceCommon.company_datasource_id.in_(company_datasource_ids),
                ETLCompanyDatasourceCommon.data_type == data_type
            )
        )
        .all()
    )
    return etl_company_datasources


# UPDATE
@auto_session
def update_existing_company_datasource(
    company_datasource_id,
    request_id,
    data_type,
    payload,
    session=None
):
    """
    Update existing etl_company_datasource
    - update to new request_id
    - add new request id to array to keep track
    - update status
    - set progress back to 0.0
    - replace payload with new one
    """
    etl_company_datasource = (
        session
        .query(ETLCompanyDatasourceCommon)
        .filter(
            and_(
                ETLCompanyDatasourceCommon.company_datasource_id == company_datasource_id,
                ETLCompanyDatasourceCommon.data_type == data_type
            )
        )
        .order_by(ETLCompanyDatasourceCommon.request_id.desc())
        .first()
    )
    etl_company_datasource.request_id = request_id
    etl_company_datasource.status = config.STATUS_WAITING
    if request_id not in etl_company_datasource.all_request_ids:
        logger.info(f'APPENDING REQUEST_ID {request_id} INTO {etl_company_datasource.all_request_ids}')
        new_all_request_ids = deepcopy(etl_company_datasource.all_request_ids)
        new_all_request_ids.append(request_id)
        etl_company_datasource.all_request_ids = new_all_request_ids

    etl_company_datasource.data_version = etl_company_datasource.data_version + 1
    etl_company_datasource.payload = payload
    etl_company_datasource.progress = 0.0
    session.commit()

    new_version = etl_company_datasource.data_version
    status = etl_company_datasource.status
    logger.info(f"[update_existing_company_datasource] "
                f"company_datasource_id {company_datasource_id} updated "
                f"to version {new_version}, status to {status}")
    return etl_company_datasource.to_json()


## NEW
@auto_session
def create_etl_company_datasource(
    company_datasource_id,
    request_id,
    company_id,
    company_name,
    source_id,
    source_name,
    source_code,
    source_type,
    nlp_type,
    urls,
    data_type,
    payload,
    session=None,
):
    """
    Create new company_datasource entry
    """
    now = dt.datetime.utcnow()
    etl_company_datasource = ETLCompanyDatasourceCommon(
        company_datasource_id=company_datasource_id,
        request_id=request_id,
        company_id=company_id,
        company_name=company_name,
        source_id=source_id,
        source_name=source_name,
        source_code=source_code,
        source_type=source_type,
        nlp_type=nlp_type,
        urls=urls,
        created_at=now,
        updated_at=now,
        data_version=1,
        status=config.STATUS_WAITING,
        progress=0.0,
        all_request_ids=[request_id],
        payload=payload,
        data_type=data_type
    )
    session.add(etl_company_datasource)
    session.commit()

    logger.info(f"[create_etl_company_datasource] new etl_company_datasource"
                f"with type {data_type} created, "
                f"id: {company_datasource_id}")
    return etl_company_datasource.to_json()


@auto_session
def get_etl_company_datasources_as_dict(etl_company_datasources, session=None):
    """
    Get a list of etl_company_datasources
    """
    logger.info(f"[get_etl_company_datasources_as_dict] Starting get etl_company_datasources_as_dict")
    etl_company_datasources_as_dict = {}
    for company_datasource_id, data_type in etl_company_datasources:
        etl_company_datasource = (
            session
            .query(ETLCompanyDatasourceCommon)
            .filter(
                and_(
                    ETLCompanyDatasourceCommon.company_datasource_id == company_datasource_id,
                    ETLCompanyDatasourceCommon.data_type == data_type
                )
            )
            .first()
        ).to_json()

        if etl_company_datasource:
            etl_company_datasources_as_dict[
                (company_datasource_id, data_type)
            ] = etl_company_datasource

    if etl_company_datasources_as_dict:
        logger.info(f"[get_etl_company_datasources_as_dict] "
                    f"queried {len(etl_company_datasources_as_dict)} company_datasources")
        return etl_company_datasources_as_dict

