from dotenv import load_dotenv

load_dotenv()

import unittest
import datetime as dt
import config
from database import Session, ETLCompanyBatch, ETLRequest, ETLStep, ETLStepDetail
import helpers
import os
import shutil


class TestGenerateIndeedSteps(unittest.TestCase):
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

        overview_batch = ETLCompanyBatch(
            request_id=request.request_id,
            batch_name="manulife-indeed-20201229",
            status="running",
            company_name="Manulife",
            source_name="Indeed",
            source_code="Indeed",
            url="https://www.indeed.com/cmp/Manulife?from=cmp-search-autocomplete",
            created_at=dt.datetime.now(),
            meta_data={"data_type": "overview"},
        )
        self.session.add(overview_batch)
        self.session.commit()

        job_batch = ETLCompanyBatch(
            request_id=request.request_id,
            batch_name="manulife-indeed-20201229",
            status="running",
            company_name="Manulife",
            source_name="Indeed",
            source_code="Indeed",
            url="https://www.indeed.com/cmp/Manulife/jobs",
            created_at=dt.datetime.now(),
            meta_data={"data_type": "job"},
        )
        self.session.add(job_batch)
        self.session.commit()

        review_batch = ETLCompanyBatch(
            request_id=request.request_id,
            batch_name="manulife-indeed-20201229",
            status="running",
            company_name="Manulife",
            source_name="Indeed",
            source_code="Indeed",
            url="https://www.indeed.com/cmp/Manulife/reviews",
            created_at=dt.datetime.now(),
            meta_data={"data_type": "review"},
        )
        self.session.add(review_batch)
        self.session.commit()

        self.request = request
        self.overview_batch = overview_batch
        self.job_batch = job_batch
        self.review_batch = review_batch

        config.CRAWLER_IDS = ["crawler001", "crawler002"]
        config.BATCH_NAME = overview_batch.batch_name

    def test_helper_genereate_overview_steps(self):
        # Run the function
        helpers.generate_steps(self.overview_batch)
        print("pass")

    def test_helper_genereate_review_steps(self):
        # Run the function
        helpers.generate_steps(self.review_batch)
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
