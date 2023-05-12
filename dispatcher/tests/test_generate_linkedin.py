from dotenv import load_dotenv

load_dotenv()

import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil


class TestGenerateLinkedInSteps(unittest.TestCase):
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

        job_batch = ETLCompanyBatch(
            request_id=request.request_id,
            batch_name="manulife-linked-20201229",
            status="running",
            company_name="Manulife",
            source_name="LinkedIn",
            source_code="LinkedIn",
            url="https://www.linkedin.com/jobs/search?keywords=manulife&location=Worldwide&geoId=92000000&f_C=2691&trk=public_jobs_jobs-search-bar-recent-searches&position=1&pageNum=0",
            created_at=dt.datetime.now(),
            meta_data={"data_type": "job"},
        )
        self.session.add(job_batch)
        self.session.commit()

        overview_batch = ETLCompanyBatch(
            request_id=request.request_id,
            batch_name="manulife-linked-20201229",
            status="running",
            company_name="Manulife",
            source_name="LinkedIn",
            source_code="LinkedIn",
            url="https://ca.linkedin.com/company/manulife-financial",
            created_at=dt.datetime.now(),
            meta_data={"data_type": "overview"},
        )
        self.session.add(overview_batch)
        self.session.commit()

        self.request = request
        self.job_batch = job_batch
        self.overview_batch = overview_batch

        config.CRAWLER_IDS = ["crawler001", "crawler002"]
        config.BATCH_NAME = job_batch.batch_name

    def test_helper_genereate_overview_steps(self):
        # Run the function
        helpers.generate_steps(self.overview_batch)
        print("pass")

    def test_helper_genereate_job_steps(self):
        # Run the function
        helpers.generate_steps(self.job_batch)
        print("pass")

    def tearDown(self):
        # Clean task directory
        for crawler_id in config.CRAWLER_IDS:
            try:
                os.remove(f"{config.TASKS_DIR}/{crawler_id}")
            except:
                pass

        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
