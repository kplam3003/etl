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
import subscribers
from tests.data import SOURCES, COMPANIES, STATUSES, ETL_STEP_TYPES, now


class TestStatusUpdater(unittest.TestCase):
    def setUp(self):
        self.session = Session()

    def test_all_finished_requests(self):
        for i in range(5):
            request = ETLRequest(
                case_study_id=1,
                case_study_name='South Retail',
                status='finished',
                created_at=dt.datetime.now()
            )
            self.session.add(request)

        self.session.commit()

        # Run the function
        subscribers.status_updater(run_once=True)
    
    def test_have_unfinished_requests(self):
        for status in [*['finished']*2, *random.choices(['waiting', 'running', 'completed with error'], k=3)]:
            request = ETLRequest(
                case_study_id=random.randint(1,100),
                case_study_name='South Retail',
                status=status,
                created_at=dt.datetime.now()
            )
            self.session.add(request)

            if status is not 'finished':
                for status in STATUSES:
                    batch = ETLCompanyBatch(
                        request_id=request.request_id,
                        batch_name=f'{random.choice(COMPANIES).lower()}-{random.choice(SOURCES).lower}-{now()}',
                        status=status,
                        company_name=random.choice(COMPANIES),
                        source_name=random.choice(SOURCES),
                        url='https://www.capterra.com/p/207944/PayPal3/',
                        created_at=dt.datetime.now()
                    )
                    self.session.add(batch)
                    self.session.commit()

                    for step_type in ETL_STEP_TYPES:
                        step = ETLStep(
                            request_id=request.request_id,
                            batch_id=batch.batch_id,
                            step_name=f"{step_type}-{batch.batch_name}",
                            step_type=step_type,
                            status=random.choice(STATUSES),
                            created_at=dt.datetime.now(),
                            updated_at=dt.datetime.now()
                        )
                        self.session.add(step)
                        self.session.commit()

                        for i in range(random.choice([3, 5, 10])):
                            step_detail = ETLStepDetail(
                                request_id=request.request_id,
                                batch_id=batch.batch_id,
                                step_id=step.step_id,
                                step_detail_name=f"{step.step_name}-f{i+1:03d}",
                                paging='-:-',
                                status='finished',
                                created_at=dt.datetime.now(),
                                updated_at=dt.datetime.now()
                            )
                            self.session.add(step_detail)
                            self.session.commit()

        self.session.commit()

        # Run the function
        subscribers.status_updater(run_once=True)
      
    
    def tearDown(self):
        # Clean database
        self.session.query(ETLStepDetail).delete()
        self.session.query(ETLStep).delete()
        self.session.query(ETLCompanyBatch).delete()
        self.session.query(ETLRequest).delete()
        self.session.commit()
        self.session.close()
