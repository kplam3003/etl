import sys
import datetime as dt
sys.path.append('../')
from database import (
    ETLCaseStudy,
    ETLCaseStudyData,
    auto_session
)
import config
from logger_global import logger

from psycopg2.errors import ForeignKeyViolation
from sqlalchemy.exc import IntegrityError

@auto_session
def get_etl_case_study(case_study_id, session=None):
    """
    Get the newest record for a given case_study_id
    """
    etl_case_study = (
        session
        .query(ETLCaseStudy)
        .filter(ETLCaseStudy.case_study_id == case_study_id)
        .order_by(ETLCaseStudy.request_id.desc())
        .first()
    )

    if etl_case_study:
        return etl_case_study.to_json()


@auto_session
def create_etl_case_study(
    case_study_id,
    request_id,
    case_study_name,
    nlp_type,
    nlp_pack,
    payload,
    session=None,
):
    """
    Create new entry for new case_study request
    """
    now = dt.datetime.utcnow()
    etl_case_study = ETLCaseStudy(
        case_study_id=case_study_id,
        request_id=request_id,
        case_study_name=case_study_name,
        nlp_type=nlp_type,
        nlp_pack=nlp_pack,
        created_at=now,
        updated_at=now,
        status=config.STATUS_WAITING,
        progress=0.0,
        payload=payload,
    )
    session.add(etl_case_study)
    session.commit()
    logger.info(f"[create_etl_case_study] etl_case_study created; "
                f"case_study_id: {etl_case_study.case_study_id}")
    return etl_case_study.to_json()


@auto_session
def reset_etl_case_study_status_and_progress(
    case_study_id,
    payload,
    session=None
):
    """
    Update status of etl_case_study for dimension_change action
    """
    etl_case_study = (
        session
        .query(ETLCaseStudy)
        .filter(ETLCaseStudy.case_study_id == case_study_id)
        .first()
    )
    etl_case_study.status = 'waiting'
    etl_case_study.progress = 0.0
    etl_case_study.payload = payload
    session.commit()

    if etl_case_study:
        logger.info(f"[update_etl_case_study_for_dimension_change] "
                    f"etl_case_study {case_study_id} updated for dimension_change action")
        return etl_case_study.to_json()


@auto_session
def get_etl_case_study_data_by_cs_id(case_study_id, session=None):
    etl_case_study_data_list = (
        session
        .query(ETLCaseStudyData)
        .filter(ETLCaseStudyData.case_study_id == case_study_id)
        .all()
    )
    return etl_case_study_data_list


@auto_session
def update_etl_case_study_data_version(
    case_study_id,
    company_datasource_id,
    data_version,
    data_type,
    logger,
    session=None
):
    """
    Update data_version of a company_datasource within a case_study
    """
    etl_case_study_data = (
        session
        .query(ETLCaseStudyData)
        .filter(ETLCaseStudyData.case_study_id == case_study_id)
        .filter(ETLCaseStudyData.company_datasource_id == company_datasource_id)
        .filter(ETLCaseStudyData.data_type == data_type)
        .first()
    )
    etl_case_study_data.data_version = data_version
    session.commit()

    if etl_case_study_data:
        logger.info(f"[update_etl_case_study_data_version] "
                    f"case_study_id {case_study_id}, company_datasource_id"
                    f"{company_datasource_id}, data_type {data_type}"
                    f"updated to data_version {data_version}!")
        return etl_case_study_data.to_json()


@auto_session
def create_etl_case_study_data(
    case_study_id,
    request_id,
    company_datasource_id,
    nlp_type,
    nlp_pack,
    re_scrape,
    re_nlp,
    data_version,
    data_type,
    status,
    progress,
    payload,
    session=None,
):
    """
    create new entry for company_datasource that is used in a case_study
    - data_version is the latest one when this company_datasource is chosen
    - status is the status of the company_datasource_id upon being chosen
    - progress is set with same logic as status
    """
    
    now = dt.datetime.utcnow()
    etl_case_study_data = ETLCaseStudyData(
        case_study_id=case_study_id,
        request_id=request_id,
        company_datasource_id=company_datasource_id,
        nlp_type=nlp_type,
        nlp_pack=nlp_pack,
        re_scrape=re_scrape,
        re_nlp=re_nlp,
        created_at=now,
        updated_at=now,
        data_version=data_version,
        data_type=data_type,
        status=status,
        progress=progress,
        payload=payload,
    )
    try:
        session.add(etl_case_study_data)
        session.commit()
        logger.info(f"[create_etl_case_study_data] etl_case_study_data created; "
                    f"case_study_id: {case_study_id}, "
                    f"company_datasource_id: {company_datasource_id}, "
                    f"data_type: {data_type}, "
                    f"data_version: {data_version}")
        return etl_case_study_data.to_json()

    except (IntegrityError, ForeignKeyViolation):
        logger.warning(f"Foreign Key Error, company_datasource_id {company_datasource_id}, data_type {data_type}")
        return None


