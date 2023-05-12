from core.pubsub_util import publish


def publish_error_message(
    project_id,
    topic_id,
    # info fields
    company_datasource_id,
    batch_id,
    step_detail_id,
    company_id,
    company_name,
    source_id,
    source_name,
    data_type,
    error_source,
    error_code,
    severity,
    exception,
    total_step_details,
    error_time,
    data_version,
    additional_info,
    # others
    logger,
):
    """
    Publish error details to internal error topic for error record.
    """
    if isinstance(exception, str):
        error_message = exception
    elif isinstance(exception, Exception):
        error_message = f"{type(exception).__name__}: {str(exception)}"[:200]
        
    error_payload = {
        "event": "error",
        "company_datasource_id": company_datasource_id,
        "batch_id": batch_id,
        "step_detail_id": step_detail_id,
        "company_id": company_id,
        "company_name": company_name,
        "source_id": source_id,
        "source_name": source_name,
        "data_type": data_type,
        "error_source": error_source,
        "error_code": error_code,
        "severity": severity,
        "error_message": error_message,
        "total_step_details": total_step_details,
        "error_time": error_time,
        "data_version": data_version,
        "additional_info": additional_info
    }

    publish(
        logger=logger, 
        gcp_product_id=project_id, 
        topic=topic_id,
        payload=error_payload
    )