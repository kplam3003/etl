from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, DateTime, Boolean, JSON, Float, UniqueConstraint, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import sessionmaker
from datetime import datetime


import config
import psycopg2

Base = declarative_base()
Base.metadata.schema = 'mdm'


class ETLRequest(Base):
    __tablename__ = 'etl_request'

    request_id = Column(Integer, primary_key=True)
    request_type = Column(String(255))
    case_study_id = Column(Integer)
    case_study_name = Column(String(255))
    status = Column(String(255))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    is_translation = Column(Boolean)
    payload = Column(JSONB)
    data_type = Column(String)

    def to_json(self):
        return {
            'request_id': self.request_id,
            'request_type': self.request_type,
            'case_study_id': self.case_study_id,
            'case_study_name': self.case_study_name,
            'status': self.status,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'is_translation': self.is_translation,
            'payload': self.payload,
            'data_type': self.data_type
        }


class ETLCaseStudy(Base):
    __tablename__ = 'etl_case_study'

    case_study_id = Column(Integer, primary_key=True)
    request_id = Column(Integer)
    case_study_name = Column(String(255))
    nlp_type = Column(String(100))
    nlp_pack = Column(String(200))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    status = Column(String(100))
    progress = Column(Float)
    payload = Column(JSONB)

    def to_json(self):
        return {
            'case_study_id': self.case_study_id,
            'request_id': self.request_id,
            'case_study_name': self.case_study_name,
            'nlp_type': self.nlp_type,
            'nlp_pack': self.nlp_pack,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'status': self.status,
            'progress': self.progress,
            'payload': self.payload
        }
        
        
class ETLCaseStudyData(Base):
    __tablename__ = 'etl_case_study_data'

    case_study_id = Column(Integer, primary_key=True)
    request_id = Column(Integer)
    company_datasource_id = Column(Integer, primary_key=True)
    nlp_type = Column(String(100))
    nlp_pack = Column(String(200))
    re_scrape = Column(Boolean)
    re_nlp = Column(Boolean)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    data_version = Column(Integer)
    data_version_job = Column(Integer)
    data_type = Column(String, primary_key=True)
    status = Column(String(100))
    progress = Column(Float)
    payload = Column(JSONB)

    def to_json(self):
        return {
            'case_study_id': self.case_study_id,
            'request_id': self.request_id,
            'company_datasource_id': self.company_datasource_id,
            'nlp_type': self.nlp_type,
            'nlp_pack': self.nlp_pack,
            're_scrape': self.nlp_pack,
            're_nlp': self.nlp_pack,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'data_version': self.data_version,
            'data_version_job': self.data_version_job,
            'data_type': self.data_type,
            'status': self.status,
            'progress': self.progress,
            'payload': self.payload
        }
        
        
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
    __tablename__ = 'etl_company_batch'

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
    nlp_type = Column(String)
    nlp_pack = Column(String)
    is_translation = Column(Boolean)
    meta_data = Column(JSON)
    company_datasource_id = Column(Integer)
    data_version = Column(Integer)
    data_version_job = Column(Integer)
    
    def to_json(self):
        return {
            'batch_id': self.batch_id,
            'request_id': self.request_id,
            'batch_name': self.batch_name,
            'company_id': self.company_id,
            'company_name': self.company_name,
            'source_id': self.source_id,
            'source_name': self.source_name,
            'source_code': self.source_code,
            'source_type': self.source_type,
            'url': self.url,
            'status': self.status,
            'request_id': self.request_id,
            'created_at': self.created_at.isoformat() if self.created_at else '',
            'updated_at': self.updated_at.isoformat() if self.updated_at else '',
            'nlp_pack': self.nlp_pack,
            'nlp_type': self.nlp_type,
            'is_translation': self.is_translation,
            'meta_data': self.meta_data,
            'company_datasource_id': self.company_datasource_id,
            'data_version': self.data_version,
            'data_version_job': self.data_version_job
        }


class ETLPostprocess(Base):
    __tablename__ = 'etl_postprocess'

    postprocess_id = Column(Integer, primary_key=True)
    request_id = Column(Integer)
    case_study_id = Column(Integer)
    progress = Column(Float)
    postprocess_type = Column(String)
    postprocess_error = Column(String)
    status = Column(String)
    meta_data = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_deprecated = Column(Boolean, default=False)

    def to_json(self):
        return {
            'postprocess_id': self.postprocess_id,
            'request_id': self.request_id,
            'case_study_id': self.case_study_id,
            'progress': self.progress,
            'postprocess_type': self.postprocess_type,
            'postprocess_error': self.postprocess_error,
            'meta_data': self.meta_data,
            'status': self.status,
            'created_at': self.created_at.isoformat() if self.created_at else '',
            'updated_at': self.updated_at.isoformat() if self.updated_at else '',
            'is_deprecated': self.is_deprecated
        }


engine = create_engine(config.DATABASE_URI, echo=False)
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


def auto_connection(func):
    def wrapper(*args, connection=None, cursor=None, **kwargs):
        if connection is None or cursor is None:
            connection = psycopg2.connect(user = config.DB_USER,
                                      password = config.DB_PASSWORD,
                                      host = config.DB_HOST,
                                      port = config.DB_PORT,
                                      database = config.DB_NAME)
            cursor = connection.cursor()
            try:
                result = func(*args, **kwargs, connection=connection, cursor=cursor)
            except:
                connection.close()
                raise
            
            connection.close()
            return result
        else:
            return func(*args, **kwargs, connection=connection, cursor=cursor)
    
    return wrapper
