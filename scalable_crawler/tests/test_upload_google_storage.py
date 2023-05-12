from dotenv import load_dotenv

load_dotenv()

import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil
from google.cloud import storage


class TestUploadGoogleStorage(unittest.TestCase):
    def setUp(self):
        self.session = Session()

        request = ETLRequest(
            case_study_id=1,
            case_study_name="South Retail",
            status="running",
            created_at=dt.datetime.now(),
        )
        self.session.add(request)
        # self.session.commit()

        batch = ETLCompanyBatch(
            request_id=request.request_id,
            batch_name="paypal-capterra-20201201",
            status="running",
            company_name="Paypal",
            source_name="Capterra",
            url="https://www.capterra.com/p/207944/PayPal3/",
            created_at=dt.datetime.now(),
        )
        self.session.add(batch)

        etl_step = ETLStep(
            step_name=f"crawl-paypal-capterra",
            status="running",
            request_id=request.request_id,
            batch_id=batch.batch_id,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step)

        etl_step_detail = ETLStepDetail(
            step_detail_name="crawl-paypal-capterra-f001",
            status="running",
            paging="1:2",
            request_id=request.request_id,
            step_id=etl_step.step_id,
            batch_id=batch.batch_id,
            file_id=1,
            created_at=dt.datetime.now(),
        )
        self.session.add(etl_step_detail)

        self.session.commit()

        self.request = request
        self.batch = batch
        self.step = etl_step
        self.step_detail = etl_step_detail

    def test_upload_google_storage(self):
        # Retrieve the file
        src_file = helpers.crawl(self.batch.url, self.step_detail)

        # Upload the file
        *path, filename = src_file.split("/")
        dst_file = f"crawl/{self.batch.batch_name.strip()}/{filename}"
        helpers.upload_google_storage(src_file, dst_file)

        # Must have file in output dir
        self.assertEquals(
            len(os.listdir(config.OUTPUT_DIR)), 1, "Must have tasks in task dir"
        )

        # Delete after test, make sure clean what you put
        storage_client = storage.Client()
        bucket = storage_client.bucket(config.GCP_STORAGE_BUCKET)
        blob = bucket.blob(dst_file)
        blob.delete()

    def tearDown(self):
        # Clean output dir
        for f in os.listdir(config.OUTPUT_DIR):
            os.remove(f"{config.OUTPUT_DIR}/{f}")

        # Clean out task dir
        for f in os.listdir(config.TASKS_DIR):
            os.remove(f"{config.TASKS_DIR}/{f}")

        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
