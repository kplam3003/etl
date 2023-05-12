import sys

from sqlalchemy import and_

from core.logger import auto_logger

sys.path.append('../')

from database import (
    ETLCompanyDatasourceCommon,
    auto_session
)


@auto_session
@auto_logger
def get_etl_company_datasource_by_etl_request_id(
    etl_request_id, data_type, logger=None, session=None
):
    """
    Get the newest record for a given request_id
    """
    
    etl_company_datasource = (
        session
        .query(ETLCompanyDatasourceCommon)
        .filter(
            and_(
                ETLCompanyDatasourceCommon.request_id == etl_request_id,
                ETLCompanyDatasourceCommon.data_type == data_type,
            )
        )
        .first()
    )
        
    return etl_company_datasource
