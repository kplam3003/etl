import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil


class TestGenerateCaterraSteps(unittest.TestCase):
    def setUp(self):
        self.session = Session()
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()

        request = ETLRequest(
            case_study_id=1,
            case_study_name="South Retail",
            status="running",
            created_at=dt.datetime.now(),
        )
        self.session.add(request)
        self.session.commit()

        batch = ETLCompanyBatch(
            request_id=request.request_id,
            batch_name="paypal-capterra-20201201",
            status="running",
            company_name="Paypal",
            source_name="Capterra",
            source_code="Capterra",
            url="https://www.capterra.com/p/207944/PayPal3/",
            created_at=dt.datetime.now(),
        )
        self.session.add(batch)
        self.session.commit()

        self.request = request
        self.batch = batch

        config.CRAWLER_IDS = ["crawler001", "crawler002"]
        config.BATCH_NAME = batch.batch_name

    def test_helper_genereate_steps(self):
        # Run the function
        helpers.generate_steps(self.batch)

    def tearDown(self):
        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
