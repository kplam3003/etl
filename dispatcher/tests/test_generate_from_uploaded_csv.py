import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil


class TestGenerateFromUploadedCsv(unittest.TestCase):
    def setUp(self):
        self.session = Session()

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
            batch_name="beanstalk-g2-20210203",
            status="running",
            company_name="Beanstalk",
            source_name="G2",
            source_code="G2",
            source_type="csv",
            url="gs://etl-datasource/upload/cs_000/csv_upload_sample_400k.csv",
            created_at=dt.datetime.now(),
        )
        self.session.add(batch)
        self.session.commit()

        self.request = request
        self.batch = batch

    def test_helper_genereate_upload_csv_steps(self):
        # Run the function
        helpers.generate_steps(self.batch)

        etl_details = (
            self.session.query(ETLStepDetail)
            .filter(ETLStepDetail.batch_id == self.batch.batch_id)
            .all()
        )
        self.assertEquals(len(etl_details), 4)

    def tearDown(self):
        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
