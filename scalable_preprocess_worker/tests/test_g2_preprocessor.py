import os
from datetime import datetime
from copy import deepcopy
import sys

sys.path.append("../")
sys.path.append("../../")

import unittest

from app import transform_g2


# set up test case
class G2TransformTestCase(unittest.TestCase):
    def setUp(self):
        self.payload = {"batch": {"source_name": "G2", "company_id": 2}}

        self.writtenOn = "2021-01-01"
        self.date = datetime.utcnow().__str__()
        self.id_ = "abc123"
        self.rating_value = 5

        self.base_item = {
            "writtenOn": self.writtenOn,
            "date": self.date,
            "id": self.id_,
            "rating_value": self.rating_value,
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
        result = transform_g2(self.payload, test_item)

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
        full_review_dict = {
            "review_text": (
                "What do you like best? This is pros"
                "What do you dislike? This is cons"
                "Recommendations to others considering the product: This is my recommendation"
                "What problems are you solving with the product? What benefits have you realized? These are my problems solved"
            )
        }
        # construct preprocessing results for testing
        results = self._construct_test_result(full_review_dict)

        # sanity tests
        self._sanity_tests(results=results)

        # general tests
        self.assertEqual(len(results), 4, "incorrect number of items returned")

    def test_missing_recommendation_field(self):
        full_review_dict = {
            "review_text": (
                "What do you like best? This is pros"
                "What do you dislike? This is cons"
                "What problems are you solving with the product? What benefits have you realized? These are my problems solved"
            )
        }
        # construct preprocessing results for testing
        results = self._construct_test_result(full_review_dict)

        # sanity tests
        self._sanity_tests(results=results)

        # general tests
        self.assertEqual(len(results), 3, "incorrect number of items returned")


if __name__ == "__main__":
    unittest.main(verbosity=2)
