import sys
sys.path.append('../')

from dotenv import load_dotenv
load_dotenv()

import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil
import json
import random
from tests.data import SOURCES, COMPANIES, STATUSES


class TestRunningBatchStatus(unittest.TestCase):
    def setUp(self):
        self.session = Session()

        request = ETLRequest(
            case_study_id=1,
            case_study_name='South Retail',
            status='running',
            created_at=dt.datetime.now()
        )
        self.session.add(request)
        self.session.commit()

        batch = ETLCompanyBatch(
            request_id=request.request_id,
            batch_name='paypal-capterra-20201201',
            status='running',
            company_name='Paypal',
            source_name='Capterra',
            url='https://www.capterra.com/p/207944/PayPal3/',
            created_at=dt.datetime.now()
        )
        self.session.add(batch)
        self.session.commit()
        self.batch = batch

    def test_not_all_finished_steps(self):
        for i in range(10):
            step_type = random.choice(['crawl', 'translate', 'nlp', 'load'])
            etl_step = ETLStep(
                batch_id=self.batch.batch_id,
                request_id=self.batch.request_id,
                step_type=step_type,
                status=random.choice(['waiting', 'running']),
                step_name=f"{step_type}-paypal-capterra",
                created_at = dt.datetime.today()
            )
            self.session.add(etl_step)

        self.session.commit()

        # Run the function
        helpers.update_running_batches_status()

        # The status must still be running
        batch = self.session.query(ETLCompanyBatch).get(self.batch.batch_id)
        self.assertEquals(batch.status, 'running')
    
    def test_all_finished_steps(self):
        for i in range(10):
            step_type = random.choice(['crawl', 'translate', 'nlp', 'load'])
            etl_step = ETLStep(
                batch_id=self.batch.batch_id,
                request_id=self.batch.request_id,
                step_type=step_type,
                status='finished',
                step_name=f"{step_type}-paypal-capterra",
                created_at = dt.datetime.today()
            )
            self.session.add(etl_step)

        self.session.commit()

        # Run the function
        helpers.update_running_batches_status()

        # The status must still be finished
        batch = self.session.query(ETLCompanyBatch).get(self.batch.batch_id)
        self.assertEquals(batch.status, 'finished')
    
    def test_all_finished_with_error_steps(self):
        for i in range(10):
            step_type = random.choice(['crawl', 'translate', 'nlp', 'load'])
            etl_step = ETLStep(
                batch_id=self.batch.batch_id,
                request_id=self.batch.request_id,
                step_type=step_type,
                status=random.choice(['finished', 'completed with error']),
                step_name=f"{step_type}-paypal-capterra",
                created_at = dt.datetime.today()
            )
            self.session.add(etl_step)
        
        etl_step = ETLStep(
            batch_id=self.batch.batch_id,
            request_id=self.batch.request_id,
            step_type=step_type,
            status='completed with error',
            step_name=f"{step_type}-paypal-capterra",
            created_at = dt.datetime.today()
        )
        self.session.add(etl_step)

        self.session.commit()

        # Run the function
        helpers.update_running_batches_status()
        helpers.update_step_status(session=self.session, etl_step=etl_step)
        helpers.update_batch_status(session=self.session, etl_batch=self.batch)

        # The status must change accordingly
        batch = self.session.query(ETLCompanyBatch).get(self.batch.batch_id)
        self.assertEquals(batch.status, 'completed with error')

    
    def tearDown(self):
        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
        self.session.close()
