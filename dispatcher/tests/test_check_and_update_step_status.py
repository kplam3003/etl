from dotenv import load_dotenv

load_dotenv()

import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil


class TestCheckAndUpdateStepStatus(unittest.TestCase):
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
            batch_name="paypal-googleplay-20201201",
            status="running",
            company_name="Paypal",
            source_name="GooglePlay",
            url="https://play.google.com/store/apps/details?id=com.paypal.android.p2pmobile",
            created_at=dt.datetime.now(),
        )
        self.session.add(batch)
        self.session.commit()

        etl_step = ETLStep(
            step_name=f"crawl-{batch.company_name.lower()}-{batch.source_name.lower()}",
            batch_id=batch.batch_id,
            created_at=dt.datetime.now(),
            request_id=batch.request_id,
            status="running",
            step_type="crawl",
        )
        self.session.add(etl_step)
        self.session.commit()

        etl_translate_step = ETLStep(
            step_name=f"translate-{batch.company_name.lower()}-{batch.source_name.lower()}",
            batch_id=batch.batch_id,
            created_at=dt.datetime.now(),
            request_id=batch.request_id,
            status="running",
            step_type="translate",
        )
        self.session.add(etl_translate_step)
        self.session.commit()

        # Generate some translate step detail
        for i in range(10):
            etl_translate_step_detail = ETLStepDetail(
                request_id=batch.request_id,
                batch_id=batch.batch_id,
                step_id=etl_translate_step.step_id,
                file_id=i + 1,
                paging=f"{i*10+1}:{i*10+10}",
                created_at=dt.datetime.today(),
                status="running",
                step_detail_name=f"translate-{batch.company_name.lower()}-{batch.source_name.lower()}-20201203-f{i+1:03d}",
            )
            self.session.add(etl_translate_step_detail)
            self.session.commit()

        self.batch = batch
        self.etl_step = etl_step

    def test_with_any_still_running_or_waiting_step_details(self):
        # Insert some running
        etl_step_detail = ETLStepDetail(
            request_id=self.batch.request_id,
            batch_id=self.batch.batch_id,
            step_id=self.etl_step.step_id,
            file_id=1,
            paging=f"1:10",
            created_at=dt.datetime.today(),
            status="running",
            step_detail_name=f"crawl-{self.batch.company_name.lower()}-{self.batch.source_name.lower()}-20201203-f001",
        )
        self.session.add(etl_step_detail)
        self.session.commit()

        # Run the function
        helpers.check_and_update_step_status(self.batch)

        etl_step = self.session.query(ETLStep).get(self.etl_step.step_id)
        self.assertEquals(etl_step.status, "running")

    def test_with_all_finished_step_details(self):
        # Insert some running
        for i in range(10):
            etl_step_detail = ETLStepDetail(
                request_id=self.batch.request_id,
                batch_id=self.batch.batch_id,
                step_id=self.etl_step.step_id,
                file_id=i + 1,
                paging=f"{i*10+1}:{i*10+10}",
                created_at=dt.datetime.today(),
                status="finished",
                step_detail_name=f"crawl-{self.batch.company_name.lower()}-{self.batch.source_name.lower()}-20201203-f{i+1:03d}",
            )
            self.session.add(etl_step_detail)
            self.session.commit()

        # Run the function
        helpers.check_and_update_step_status(self.batch)

        etl_step = self.session.query(ETLStep).get(self.etl_step.step_id)
        self.assertEquals(etl_step.status, "finished")

    def test_with_all_finished_and_some_error_step_details(self):
        # Insert some finished
        for i in range(10):
            etl_step_detail = ETLStepDetail(
                request_id=self.batch.request_id,
                batch_id=self.batch.batch_id,
                step_id=self.etl_step.step_id,
                file_id=i + 1,
                paging=f"{i*10+1}:{i*10+10}",
                created_at=dt.datetime.today(),
                status="finished",
                step_detail_name=f"crawl-{self.batch.company_name.lower()}-{self.batch.source_name.lower()}-20201203-f{i+1:03d}",
            )
            self.session.add(etl_step_detail)
            self.session.commit()

        # Insert some error
        for i in range(10):
            etl_step_detail = ETLStepDetail(
                request_id=self.batch.request_id,
                batch_id=self.batch.batch_id,
                step_id=self.etl_step.step_id,
                file_id=i + 1,
                paging=f"{i*10+1}:{i*10+10}",
                created_at=dt.datetime.today(),
                status="completed with error",
                step_detail_name=f"crawl-{self.batch.company_name.lower()}-{self.batch.source_name.lower()}-20201203-f{i+1:03d}",
            )
            self.session.add(etl_step_detail)
            self.session.commit()

        # Run the function
        helpers.check_and_update_step_status(self.batch)

        etl_step = self.session.query(ETLStep).get(self.etl_step.step_id)
        self.assertEquals(etl_step.status, "completed with error")

    def test_with_no_step_details(self):
        # Run the function
        helpers.check_and_update_step_status(self.batch)
        etl_step = self.session.query(ETLStep).get(self.etl_step.step_id)

        # The status without any step detail must still be waiting
        self.assertEquals(etl_step.status, "waiting")

    def tearDown(self):
        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
