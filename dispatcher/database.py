from sqlalchemy import create_engine, UniqueConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy import Column, Integer, String, DateTime, JSON, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.mutable import MutableDict
import config
import logging
from datetime import datetime

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
    payload = Column(MutableDict.as_mutable(JSON))


class ETLCompanyDatasourceCommon(Base):
    __tablename__ = 'etl_company_datasource'
    __table_args__ = (
        UniqueConstraint(
            'company_datasource_id',
            'data_type', 
            name='unique_company_datasource_id_data_type'
        ),
    )

    id = Column(Integer, primary_key=True)
    company_datasource_id = Column(Integer)
    request_id = Column(Integer, ForeignKey('etl_request.request_id'))
    company_id = Column(Integer)
    company_name = Column(String(255))
    source_id = Column(Integer)
    source_name = Column(String(255))
    source_code = Column(String(255))
    source_type = Column(String(100))
    nlp_type = Column(String(100))
    urls = Column(JSONB)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_version = Column(Integer)
    status = Column(String(100))
    progress = Column(Float)
    all_request_ids = Column(ARRAY(Integer))
    payload = Column(JSONB)
    data_type = Column(String(255))

    def to_json(self):
        return {
            'id': self.id,
            'company_datasource_id': self.company_datasource_id,
            'request_id': self.request_id,
            'company_id': self.company_id,
            'company_name': self.company_name,
            'source_id': self.source_id,
            'source_name': self.source_name,
            'source_code': self.source_code,
            'source_type': self.source_type,
            'nlp_type': self.nlp_type,
            'urls': self.urls,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'data_version': self.data_version,
            'status': self.status,
            'progress': self.progress,
            'all_request_ids': self.all_request_ids,
            'payload': self.payload,
            'data_type': self.data_type
        }


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
    meta_data = Column(MutableDict.as_mutable(JSON))
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
            "source_type": self.source_type,
            "url": self.url,
            "status": self.status,
            "request_id": self.request_id,
            "nlp_pack": self.nlp_pack,
            "nlp_type": self.nlp_type,
            "meta_data": self.meta_data,
            "created_at": self.created_at.isoformat() if self.created_at else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at else "",
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
    lang = Column(String)
    progress_current = Column(Integer)
    progress_total = Column(Integer)
    item_count = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
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
            "lang": self.lang,
            "progress_current": self.progress_current,
            "progress_total": self.progress_total,
            "item_count": self.item_count,
            "created_at": self.created_at.isoformat() if self.created_at else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at else "",
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
