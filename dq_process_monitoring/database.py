import sys
import os

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, DateTime, Boolean, Timestamp, Float
from sqlalchemy.orm import sessionmaker

import config
import logging
import json

# source imports
sys.path.append('../')


# declarative tables
Base = declarative_base()


class ETLLeadTimeMonitor(Base):
    __tablename__ = "etl_lead_time_monitor"
    # table columns
    # request - case study fields
    request_id = Column(Integer, nullable=False)
    case_study_id = Column(Integer, nullable=False)
    case_study_name = Column(String, nullable=False)
    request_status = Column(String, nullable=False)
    request_created_at = Column(Timestamp, nullable=False)
    request_updated_at = Column(Timestamp, nullable=False)
    request_lead_time = Column(Integer)
    # batch fields
    batch_id = Column(Integer, nullable=False)
    batch_name = Column(String, nullable=False)
    batch_status = Column(String, nullable=False)
    batch_created_at = Column(Timestamp, nullable=False)
    batch_updated_at = Column(Timestamp, nullable=False)
    batch_lead_time = Column(Integer)
    # step fields
    step_id = Column(Integer, nullable=False)
    step_type = Column(String, nullable=False)
    step_name = Column(String, nullable=False)
    step_status = Column(String, nullable=False)
    step_created_at = Column(Timestamp, nullable=False)
    step_updated_at = Column(Timestamp, nullable=False)
    step_lead_time = Column(Integer)
    # step detail fields
    step_detail_id = Column(Integer)
    step_detail_name = Column(String)
    step_detail_status = Column(String)
    step_detail_created_at = Column(Timestamp)
    step_detail_updated_at = Column(Timestamp)
    step_detail_lead_time = Column(Integer)
    item_count = Column(Integer)
    # company - source
    company_id = Column(Integer, nullable=False)
    company_name = Column(String)
    source_id = Column(Integer, nullable=False)
    source_name = Column(String)
    # info
    inserted_at = Column(Timestamp, nullable=False)

    def to_json(self):
        return {
            # request - case study fields
            'request_id': self.request_id,
            'case_study_id': self.case_study_id,
            'case_study_name': self.case_study_name,
            'request_status': self.request_status,
            'request_created_at': self.request_created_at.isoformat() if self.request_created_at else '',
            'request_updated_at': self.request_updated_at.isoformat() if self.request_updated_at else '',
            'request_lead_time': self.request_lead_time,
            # batch fields
            'batch_id': self.batch_id,
            'batch_name': self.batch_name,
            'batch_status': self.batch_status,
            'batch_created_at': self.batch_created_at.isoformat() if self.batch_created_at else '',
            'batch_updated_at': self.batch_updated_at.isoformat() if self.batch_updated_at else '',
            'batch_lead_time': self.batch_lead_time,
            # step fields
            'step_id': self.step_id,
            'step_type': self.step_type,
            'step_name': self.step_name,
            'step_status': self.step_status,
            'step_created_at': self.step_created_at.isoformat() if self.step_created_at else '',
            'step_updated_at': self.step_updated_at.isoformat() if self.step_updated_at else '',
            'step_lead_time': self.step_lead_time,
            # step detail fields
            'step_detail_id': self.step_detail_idm,
            'step_detail_name': self.step_detail_name,
            'step_detail_status': self.step_detail_status,
            'step_detail_created_at': self.step_detail_created_at.isoformat() if self.step_detail_created_at else '',
            'step_detail_updated_at': self.step_detail_updated_at.isoformat() if self.step_detail_updated_at else '',
            'step_detail_lead_time': self.step_detail_lead_time,
            'item_count': self.item_count,
            # company - source
            'company_id': self.company_id,
            'company_name': self.company_name,
            'source_id': self.source_id,
            'source_name': self.source_name,
            # info
            'inserted_at': self.inserted_at.isoformat() if self.inserted_at else ''
        }
        

class ETLBatchReviewMonitor(Base):
    __tablename__ = "etl_batch_review_monitor"
    # table columns
    # request - case study fields
    request_id = Column(Integer, nullable=False)
    case_study_id = Column(Integer, nullable=False)
    # batch fields
    batch_id = Column(Integer, nullable=False)
    batch_name = Column(String, nullable=False)
    # company - source
    company_id = Column(Integer, nullable=False)
    company_name = Column(String, nullable=False)
    source_id = Column(Integer, nullable=False)
    source_name = Column(String, nullable=False)
    # step fields
    step_type = Column(String, nullable=False)
    step_status = Column(String, nullable=False)
    step_lead_time = Column(Integer)
    # file number infos
    num_files = Column(Integer)
    total_rows = Column(Integer)
    num_rows_in_file = Column(Integer)
    num_unique_reviews = Column(Integer)
    rows_per_second = Column(Float)
    # reviews successfully processed
    num_input = Column(Integer)
    num_output = Column(Integer)
    success_percent = Column(Float)
    # info
    inserted_at = Column(Timestamp, nullable=False)

    def to_json(self):
        return {
            # request - case study fields
            'request_id': self.request_id,
            'case_study_id': self.case_study_id,
            # batch fields
            'batch_id': self.batch_id,
            'batch_name': self.batch_name,
            # company - source
            'company_id': self.company_id,
            'company_name': self.company_name,
            'source_id': self.source_id,
            'source_name': self.source_name,
            # step fields
            'step_type': self.step_type,
            'step_status': self.step_status,
            'step_lead_time': self.step_lead_time,
            # file number infos
            'num_files': self.num_files,
            'total_rows': self.total_rows,
            'num_rows_in_file': self.num_rows_in_file,
            'num_unique_reviews': self.num_unique_reviews,
            'rows_per_second': self.rows_per_second,
            # reviews successfully processed
            'num_input': self.num_input,
            'num_output': self.num_output,
            'successful_percent': self.success_percent
        }


# create sqlalchemy ORM session
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

