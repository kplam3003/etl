import os
from datetime import datetime
from copy import deepcopy
import sys

sys.path.append("../")
sys.path.append("../../")

import unittest


from app import transform_func_voe_review_indeed


# set up test case
class IndeedPreprocessorTest(unittest.TestCase):
    def setUp(self):
        self.payload = {"batch": {"source_name": "Indeed", "company_id": 2}}

        self.date = "2021-01-01"
        self.reviewId = "abc123"
        self.rating = 5

        self.base_item = {
            "date": self.date,
            "reviewId": self.reviewId,
            "rating": self.rating,
        }

        self.full_review_dict = {
            "review": "general comment",
            "pros": "pros comment",
            "cons": "cons comment",
        }

        self.technical_types = (
            "overall",
            "pros",
            "cons",
            "problems_solved",
            "recommendations",
            "switch_reasons",
            "all",
        )

    def _construct_test_result(self, review_dict):
        test_item = self.base_item.copy()
        test_item.update(review_dict)
        result = transform_func_voe_review_indeed(self.payload, test_item)

        return result

    def _sanity_tests(self, results):
        for res in results:
            # required fields
            self.assertIn("review", res)
            self.assertIn("review_id", res)
            self.assertIn("parent_review_id", res)
            self.assertIn("technical_type", res)
            self.assertIn("date", res)

            # technical type values
            self.assertIn(
                res["technical_type"],
                self.technical_types,
                f"invalid technical type: {res['technical_type']}",
            )

    def test_full_review_fields(self):
        # construct preprocessing results for testing
        results = self._construct_test_result(self.full_review_dict)

        # sanity tests
        self._sanity_tests(results=results)

        # general tests
        self.assertEqual(len(results), 3, "incorrect number of items returned")

    def test_1_missing_review_field(self):
        # construct preprocessing results for testing
        for key in self.full_review_dict.keys():
            _temp_review_dict = deepcopy(self.full_review_dict)
            del _temp_review_dict[key]
            results = self._construct_test_result(_temp_review_dict)

            # sanity tests
            self._sanity_tests(results=results)

            # test for number of items
            self.assertEqual(
                len(results),
                2,
                f"incorrect number of items returned for missing field {key}",
            )

    def test_1_empty_review_fields(self):
        # construct preprocessing results for testing
        for key in self.full_review_dict.keys():
            _temp_review_dict = deepcopy(self.full_review_dict)
            _temp_review_dict[key] = ""
            results = self._construct_test_result(_temp_review_dict)

            # sanity tests
            self._sanity_tests(results=results)

            # test for number of items
            self.assertEqual(
                len(results),
                2,
                f"incorrect number of items returned for empty field {key}",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
