import os
from datetime import datetime
from copy import deepcopy
import sys

sys.path.append("../")
sys.path.append("../../")

import unittest


from app import transform_capterra


# set up test case
class CapterraPreprocessorTest(unittest.TestCase):
    def setUp(self):
        self.payload = {"batch": {"source_name": "Capterra", "company_id": 2}}

        self.writtenOn = "2021-01-01"
        self.reviewId = "abc123"
        self.overallRating = 5
        self.reviewer = r'{"fullName": "HH"}'

        self.base_item = {
            "writtenOn": self.writtenOn,
            "reviewer": self.reviewer,
            "reviewId": self.reviewId,
            "overallRating": self.overallRating,
        }

        self.full_review_dict = {
            "generalComments": "general comment",
            "prosText": "pros comment",
            "consText": "cons comment",
            "switchingReasons": "reasons for switching",
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
        result = transform_capterra(self.payload, test_item)

        return result

    def _sanity_tests(self, results):
        for res in results:
            # required fields
            self.assertIn("review", res)
            self.assertIn("review_id", res)
            self.assertIn("parent_review_id", res)
            self.assertIn("technical_type", res)
            self.assertIn("__review_date__", res)

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
        self.assertEqual(len(results), 4, "incorrect number of items returned")

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
                3,
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
                3,
                f"incorrect number of items returned for empty field {key}",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
