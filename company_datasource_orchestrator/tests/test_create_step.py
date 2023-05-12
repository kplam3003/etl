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


class TestCreateStep(unittest.TestCase):
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

    def test_create_step(self):
        etl_step = helpers.create_step(self.batch)

        # Must have file in output dir
        print(etl_step)

    
    def tearDown(self):
        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
        self.session.close()
