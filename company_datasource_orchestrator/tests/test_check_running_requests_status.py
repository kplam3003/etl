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


class TestUpdateRunningRequest(unittest.TestCase):
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
        self.request = request

    def test_not_all_finished_batches(self):
        for i in range(10):
            etl_batch = ETLCompanyBatch(
                request_id=self.request.request_id,
                batch_name=f'paypal-capterra-{i}',
                status=random.choice(['running', 'waiting', 'completed with error']),
                company_name='Paypal',
                source_name='Capterra',
                url='https://www.capterra.com/p/207944/PayPal3/',
                created_at=dt.datetime.now()
            )
            self.session.add(etl_batch)

        self.session.commit()

        # Run the function
        helpers.update_running_requests_status()

        # The status must still be running
        etl_request = self.session.query(ETLRequest).get(self.request.request_id)
        self.assertEquals(etl_request.status, 'running')
    
    def test_all_finished_batches(self):
        for i in range(10):
            etl_batch = ETLCompanyBatch(
                request_id=self.request.request_id,
                batch_name=f'paypal-capterra-{i}',
                status='finished',
                company_name='Paypal',
                source_name='Capterra',
                url='https://www.capterra.com/p/207944/PayPal3/',
                created_at=dt.datetime.now()
            )
            self.session.add(etl_batch)

        self.session.commit()

        # Run the function
        helpers.update_running_requests_status()

        # 
        etl_request = self.session.query(ETLRequest).get(self.request.request_id)
        self.assertEquals(etl_request.status, 'finished')
    
    def test_all_finished_with_error_batches(self):
        for i in range(10):
            etl_batch = ETLCompanyBatch(
                request_id=self.request.request_id,
                batch_name=f'paypal-capterra-{i}',
                status='finished',
                company_name='Paypal',
                source_name='Capterra',
                url='https://www.capterra.com/p/207944/PayPal3/',
                created_at=dt.datetime.now()
            )
            self.session.add(etl_batch)

        self.session.commit()

        etl_batch = ETLCompanyBatch(
            request_id=self.request.request_id,
            batch_name=f'paypal-capterra-{i}',
            status='completed with error',
            company_name='Paypal',
            source_name='Capterra',
            url='https://www.capterra.com/p/207944/PayPal3/',
            created_at=dt.datetime.now()
        )
        self.session.add(etl_batch)
        self.session.commit()

        # Run the function
        helpers.update_running_requests_status()

        # 
        etl_request = self.session.query(ETLRequest).get(self.request.request_id)
        self.assertEquals(etl_request.status, 'completed with error')
    
    
    def tearDown(self):
        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
        self.session.close()
