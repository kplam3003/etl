from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, DateTime, JSON
from sqlalchemy.orm import sessionmaker
import config
import logging
import json

logger = logging.getLogger(config.LOGGER)

Base = declarative_base()
Base.metadata.schema = "mdm"


class ETLRequest(Base):
    __tablename__ = "etl_request"

    request_id = Column(Integer, primary_key=True)
    case_study_id = Column(Integer)
    case_study_name = Column(String(255))
    status = Column(String(255))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class ETLCompanyBatch(Base):
    __tablename__ = "etl_company_batch"

    batch_id = Column(Integer, primary_key=True)
    request_id = Column(Integer)
    batch_name = Column(String(255))
    status = Column(String(255))
    company_id = Column(Integer)
    company_name = Column(String)
    source_id = Column(Integer)
    source_name = Column(String)
    source_code = Column(String)
    source_type = Column(String)
    url = Column(String)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    nlp_pack = Column(String)
    nlp_type = Column(String)
    meta_data = Column(JSON)
    company_datasource_id = Column(Integer)
    data_version = Column(Integer)
    data_version_job = Column(Integer)

    def to_json(self):
        return {
            "batch_id": self.batch_id,
            "batch_name": self.batch_name,
            "company_id": self.company_id,
            "company_name": self.company_name,
            "source_id": self.source_id,
            "source_name": self.source_name,
            "source_code": self.source_code,
            "url": self.url,
            "status": self.status,
            "request_id": self.request_id,
            "nlp_pack": self.nlp_pack,
            "nlp_type": self.nlp_type,
            "created_at": self.created_at.isoformat() if self.created_at else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at else "",
            "meta_data": self.meta_data,
            "company_datasource_id": self.company_datasource_id,
            "data_version": self.data_version,
            "data_version_job": self.data_version_job,
        }


class ETLStep(Base):
    __tablename__ = "etl_step"

    step_id = Column(Integer, primary_key=True)
    step_name = Column(String)
    status = Column(String)
    request_id = Column(Integer)
    batch_id = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    step_type = Column(String)

    def to_json(self):
        return {
            "step_id": self.step_id,
            "step_name": self.step_name.strip(),
            "status": self.status,
            "request_id": self.request_id,
            "batch_id": self.batch_id,
            "step_type": self.step_type,
            "created_at": self.created_at.isoformat() if self.created_at else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at else "",
        }


class ETLStepDetail(Base):
    __tablename__ = "etl_step_detail"

    step_detail_id = Column(Integer, primary_key=True)
    step_detail_name = Column(String)
    paging = Column(String)
    request_id = Column(Integer)
    step_id = Column(Integer)
    batch_id = Column(Integer)
    status = Column(String)
    file_id = Column(Integer)
    progress_current = Column(Integer)
    progress_total = Column(Integer)
    item_count = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    lang = Column(String)
    meta_data = Column(JSON)

    def to_json(self):
        return {
            "step_detail_id": self.step_detail_id,
            "step_detail_name": self.step_detail_name.strip(),
            "paging": self.paging,
            "request_id": self.request_id,
            "step_id": self.step_id,
            "batch_id": self.batch_id,
            "status": self.status,
            "file_id": self.file_id,
            "progress_current": self.progress_current,
            "progress_total": self.progress_total,
            "item_count": self.item_count,
            "created_at": self.created_at.isoformat() if self.created_at else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at else "",
            "lang": self.lang,
            "meta_data": self.meta_data,
        }


class ETLDatasource(Base):
    __tablename__ = "etl_datasource"

    source_id = Column(Integer, primary_key=True)
    batch_id = Column(Integer)
    source_name = Column(String)
    source_url = Column(String)
    provider = Column(String)
    provider_job_id = Column(String)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    def to_json(self):
        return {
            "source_id": self.source_id,
            "batch_id": self.batch_id,
            "source_name": self.source_name,
            "source_url": self.source_url,
            "provider": self.provider,
            "provider_job_id": self.provider_job_id,
            "created_at": self.created_at.isoformat() if self.created_at else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at else "",
        }


engine = create_engine(config.DATABASE_URI)
Session = sessionmaker(bind=engine)


def auto_session(func):
    def wrapper(*args, session=None, **kwargs):
        if session is None:
            session = Session()
            try:
                result = func(*args, **kwargs, session=session)
            except:
                session.close()
                raise

            session.close()
            return result
        else:
            return func(*args, **kwargs, session=session)

    return wrapper
