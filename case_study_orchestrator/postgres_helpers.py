import sys

sys.path.append('../')

from sqlalchemy import and_

from core import pubsub_util
import config
from database import (
    ETLRequest,
    ETLCaseStudy,
    ETLCaseStudyData,
    ETLCompanyDatasourceCommon,
    ETLPostprocess,
    ETLCompanyBatch,
    auto_session
)

STATUS_INIT = 'initing' # to be updated to waiting
STATUS_WAITING = 'waiting' # to be updated to running


# GET
@auto_session
def get_etl_request(request_id, logger, session=None):
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


@auto_session
def get_etl_company_datasource(company_datasource_id, logger, session=None):
    """
    Get the newest record for a given company_datasource_id
    """
    etl_company_datasource = (
        session
        .query(ETLCompanyDatasourceCommon)
        .filter(ETLCompanyDatasourceCommon.company_datasource_id == company_datasource_id)
        .order_by(ETLCompanyDatasourceCommon.request_id.desc())
        .first()
    )
        
    if etl_company_datasource:
        return etl_company_datasource.to_json()
    

@auto_session
def get_etl_case_study(case_study_id, logger, session=None):
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
def get_etl_case_study_data_for_case_study_as_dict(case_study_id, logger, session=None):
    """
    Get all etl_case_study_data entries for a given case_study_id 
    """
    etl_case_study_data_list = (
        session
        .query(ETLCaseStudyData)
        .filter(ETLCaseStudyData.case_study_id == case_study_id)
        .all()
    )
    results = {}
    for etl_case_study_data in etl_case_study_data_list:
        _dict = etl_case_study_data.to_json()
        results[_dict['company_datasource_id']] = _dict
    
    if results:
        logger.info(f"[get_etl_case_study_data_for_case_study_as_dict] "
                    f"case_study_id {case_study_id} has {len(results)} company_datasources")
        return results
    

@auto_session
def get_unfinished_etl_case_study_data(logger, session=None):
    running_etl_case_study_data_list = (
        session
        .query(ETLCaseStudyData)
        .filter(ETLCaseStudyData.status.notin_(config.COMPLETE_STATUSES))
        .all()
    )
    return [cs_data.to_json() for cs_data in running_etl_case_study_data_list]


@auto_session
def get_unfinished_etl_postprocess_step_update_cs_data(logger, session=None):
    postprocess_step_update_cs_data = (
        session
        .query(ETLPostprocess)
        .filter(ETLPostprocess.postprocess_type == 'update_cs_data')
        .filter(ETLPostprocess.is_deprecated.is_(False))
        .filter(ETLPostprocess.status.notin_(config.COMPLETE_STATUSES))
        .all()
    )
    results = [cs_data.to_json() for cs_data in postprocess_step_update_cs_data]
    return results


@auto_session
def get_etl_case_study_data_status_and_progress(case_study_id, logger, session=None):
    """
    get the statuses and progresses of `update_cs_data` postprocess step
    """
    etl_case_study_data_list = (
        session
        .query(ETLCaseStudyData)
        .filter(ETLCaseStudyData.case_study_id == case_study_id)
        .all()
    )
    status_set = set()
    progresses = []
    for etl_case_study_data in etl_case_study_data_list:
        status_set.add(etl_case_study_data.status)
        progresses.append(etl_case_study_data.progress)
        
    return status_set, progresses


@auto_session
def get_active_etl_postprocess_by_case_study_and_type(case_study_id, postprocess_type, logger, session=None):
    """
    Query for latest etl_postprocess of type `postprocess_type` for given `case_study_id`
    """
    etl_postprocess = (
        session
        .query(ETLPostprocess)
        .filter(ETLPostprocess.is_deprecated.is_(False))
        .filter(ETLPostprocess.case_study_id == case_study_id)
        .filter(ETLPostprocess.postprocess_type == postprocess_type)
        .order_by(ETLPostprocess.request_id.desc())
        .first()
    )
    if etl_postprocess:
        return etl_postprocess.to_json()
    
    
@auto_session
def get_etl_postprocess(postprocess_id, logger, session=None):
    """
    Query for etl_postprocess of id `postprocess_id`
    """
    etl_postprocess = (
        session
        .query(ETLPostprocess)
        .filter(ETLPostprocess.postprocess_id == postprocess_id)
        .first()
    )
    if etl_postprocess:
        return etl_postprocess.to_json()
    
    
@auto_session
def get_active_etl_postprocesses(case_study_id, logger, session=None):
    """
    Query for etl_postprocess of id `postprocess_id`
    """
    active_etl_postprocesses = (
        session
        .query(ETLPostprocess)
        .filter(ETLPostprocess.is_deprecated.is_(False))
        .filter(ETLPostprocess.case_study_id == case_study_id)
        .all()
    )
    if active_etl_postprocesses:
        return [ep.to_json() for ep in active_etl_postprocesses]
    
    
@auto_session
def get_etl_company_batches(company_datasource_id, logger, session=None):
    """
    Query for company batches for a company_datasource_id
    """
    etl_company_batches = (
        session
        .query(ETLCompanyBatch)
        .filter(ETLCompanyBatch.company_datasource_id == company_datasource_id)
        .all()
    )
    if etl_company_batches:
        return [b.to_json() for b in etl_company_batches]


@auto_session
def get_etl_company_batches_for_company_datasources(company_datasource_ids, logger, session=None):
    """
    Query for company batches for multiple company_datasource_ids
    """
    etl_company_batches = []
    for company_datasource_id in company_datasource_ids:
        _this_batches = get_etl_company_batches(
            company_datasource_id=company_datasource_id,
            logger=logger,
            session=session
        )
        etl_company_batches.extend(_this_batches)
    
    return etl_company_batches


# CREATE
@auto_session
def create_etl_request(
    request_type,
    payload,
    logger, 
    case_study_id=None, # legacy
    case_study_name=None, # legacy
    is_translation=None, # legacy
    data_type=None, # legacy
    session=None):
    """
    Create a new etl_request in MDM database
    """
    etl_request = ETLRequest(
        request_type=request_type,
        case_study_id=case_study_id,
        case_study_name=case_study_name,
        status=STATUS_WAITING,
        is_translation=is_translation,
        payload=payload,
        data_type=data_type
    )
    session.add(etl_request)
    session.commit()

    logger.info(f"[create_etl_request] new etl_request created, request_id: {etl_request.request_id}")
    return etl_request.to_json()


@auto_session
def create_etl_postprocess(
    request_id,
    case_study_id,
    postprocess_type,
    logger,
    status='waiting',
    progress=0.0,
    meta_data=None,
    session=None
):
    """
    Set is_deprecated status of current etl_postprocess steps of a case study to true
    """
    etl_postprocess = ETLPostprocess(
        request_id=request_id,
        case_study_id=case_study_id,
        progress=progress,
        postprocess_type=postprocess_type,
        postprocess_error='',
        meta_data=meta_data,
        status=status,
        is_deprecated=False
    )
    
    session.add(etl_postprocess)
    session.commit()
    
    if etl_postprocess:
        return etl_postprocess.to_json()


# UPDATE
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
def update_etl_case_study_data_to_in_storage_status(
    company_datasource_ids,
    logger,
    session=None
):
    """
    Update status of etl_case_study company_datasource to its current status in
    etl_company_datasource
    """ 
    updated_ids = set()
    updated_case_study_ids = set()
    for company_datasource_id, data_type in company_datasource_ids:
        etl_case_study_data_list = (
            session
            .query(ETLCaseStudyData)
            .filter(ETLCaseStudyData.company_datasource_id == company_datasource_id)
            .filter(ETLCaseStudyData.data_type == data_type)
            .filter(ETLCaseStudyData.status.notin_(config.COMPLETE_STATUSES))
            .all()
        )
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
        )

        for etl_case_study_data in etl_case_study_data_list:
            # versions
            cs_data_version = etl_case_study_data.data_version
            in_storage_version = etl_company_datasource.data_version
            # progress
            cs_data_progress = etl_case_study_data.progress
            in_storage_progress = etl_company_datasource.progress
            # status
            cs_data_status = etl_case_study_data.status
            in_storage_status = etl_company_datasource.status

            # check
            if in_storage_version > cs_data_version:
                # new version means the current version is finished
                etl_case_study_data.status = 'finished'
                etl_case_study_data.progress = 1.0

            elif in_storage_progress > cs_data_progress:
                # update if have new progress but still not finished
                etl_case_study_data.status = in_storage_status
                etl_case_study_data.progress = in_storage_progress

            elif (in_storage_status != cs_data_status) and (in_storage_status in config.COMPLETE_STATUSES):
                # update if in_storage_status is completed but progress is not yet 1.0
                etl_case_study_data.status = in_storage_status
                etl_case_study_data.progress = 1.0

            else:
                continue

            updated_ids.add(company_datasource_id)
            updated_case_study_ids.add(etl_case_study_data.case_study_id)

    session.commit()
    logger.info(f'[update_etl_case_study_data_to_in_storage_status] '
                f'{len(updated_ids)} ids in {len(updated_case_study_ids)} case studies updated')
    return updated_case_study_ids, updated_ids

    
@auto_session
def update_etl_case_study_status_and_progress(case_study_id, logger, status=None, progress=None, session=None):
    """
    Update status and progress of a case study given id
    """
    etl_case_study = (
        session
        .query(ETLCaseStudy)
        .filter(ETLCaseStudy.case_study_id == case_study_id)
        .first()
    )
    if status:
        etl_case_study.status = status
    
    if progress:
        etl_case_study.progress = progress
    
    if status or progress:
        session.commit()
        return etl_case_study.to_json()


@auto_session
def deprecate_old_etl_postprocess(case_study_id, logger, session=None):
    """
    Set is_deprecated status of current etl_postprocess steps of a case study to true
    """
    etl_postprocess_steps = (
        session
        .query(ETLPostprocess)
        .filter(ETLPostprocess.case_study_id == case_study_id)
        .filter(ETLPostprocess.is_deprecated.is_(False))
        .all()
    )
    for etl_postprocess in etl_postprocess_steps:
        etl_postprocess.is_deprecated = True
        
    session.commit()


@auto_session
def update_etl_postprocess_status_and_progress(
    case_study_id, 
    postprocess_type,
    logger, 
    status=None, 
    progress=None, 
    session=None):
    """
    Update status and progress of a postprocess step given cs id and name
    """
    etl_postprocess = (
        session
        .query(ETLPostprocess)
        .filter(ETLPostprocess.is_deprecated.is_(False))
        .filter(ETLPostprocess.case_study_id == case_study_id)
        .filter(ETLPostprocess.postprocess_type == postprocess_type)
        .first()
    )
    if status:
        etl_postprocess.status = status
    
    if progress:
        etl_postprocess.progress = progress
    
    if status or progress:
        session.commit()
        return etl_postprocess.to_json()
    
    
@auto_session
def update_etl_postprocess(
    postprocess_id,
    logger,
    meta_data=None,
    is_deprecated=None,
    status=None, 
    progress=None,
    postprocess_error=None,
    session=None):
    """
    Update status and progress of a postprocess step given cs id and name
    """
    etl_postprocess = (
        session
        .query(ETLPostprocess)
        .filter(ETLPostprocess.postprocess_id == postprocess_id)
        .first()
    )
    if status:
        etl_postprocess.status = status
    if progress:
        etl_postprocess.progress = progress
    if is_deprecated:
        etl_postprocess.is_deprecated = is_deprecated
    if postprocess_error:
        etl_postprocess.postprocess_error = postprocess_error
    if meta_data:
        etl_postprocess.meta_data = meta_data
    
    has_update = any([status, progress, is_deprecated, postprocess_error, meta_data])
    if has_update:
        session.commit()
        return etl_postprocess.to_json()
    
    
@auto_session
def update_active_etl_postprocess_by_type_and_case_study_id(
    case_study_id,
    postprocess_type,
    logger,
    meta_data=None,
    is_deprecated=None,
    status=None, 
    progress=None,
    postprocess_error=None,
    session=None):
    """
    Update status and progress of a postprocess step given cs id and name
    """
    etl_postprocess = (
        session
        .query(ETLPostprocess)
        .filter(ETLPostprocess.is_deprecated.is_(False))
        .filter(ETLPostprocess.case_study_id == case_study_id)
        .filter(ETLPostprocess.postprocess_type == postprocess_type)
        .order_by(ETLPostprocess.postprocess_id.desc())
        .first()
    )
    if status:
        etl_postprocess.status = status
    if progress:
        etl_postprocess.progress = progress
    if is_deprecated:
        etl_postprocess.is_deprecated = is_deprecated
    if postprocess_error:
        etl_postprocess.postprocess_error = postprocess_error
    if meta_data:
        etl_postprocess.meta_data = meta_data
    
    has_update = any([status, progress, is_deprecated, postprocess_error, meta_data])
    if has_update:
        session.commit()
        return etl_postprocess.to_json()
    
    
@auto_session
def update_etl_case_study(
    case_study_id,
    logger,
    status=None, 
    progress=None,
    payload=None,
    session=None
):
    """
    Update status and progress of a postprocess step given cs id and name
    """
    etl_case_study = (
        session
        .query(ETLCaseStudy)
        .filter(ETLCaseStudy.case_study_id == case_study_id)
        .first()
    )
    if status:
        etl_case_study.status = status
    if progress:
        etl_case_study.progress = progress
    if payload:
        etl_case_study.payload = payload
    
    has_update = any([status, progress, payload])
    if has_update:
        session.commit()
        return etl_case_study.to_json()
    
