import unittest
from unittest import mock

from src.lambdas.dlc_cancel_override_statemachine_fn import evaluate_request


class TestEvaluateRequest(unittest.TestCase):
    @mock.patch("src.lambdas.dlc_cancel_override_statemachine_fn.get_contiguous_request")
    def test_evaluate_request_no_replace(self, mock_get_contiguous_req):
        # --- Arrange ---
        request = {
            "request": {
                "correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
                "site": "4407114520",
                "switch_addresses": ["LG022102554"],
                "status": "ON",
                "current_stage": "DEPLOYED",
                "start_datetime": "2022-07-29T21:13:00+00:00",
                "end_datetime": "2022-07-29T21:17:00+00:00	",
                "policy_id": "8571"
            }
        }

        mock_get_contiguous_req.return_value = ({'overrdValue': 'ON', 'rqstStrtDt': "2022-07-29T21:13:00+00:00",
                                                 'rqstEndDt': "2022-07-29T21:13:00+00:00"}, None)

        # --- Act ---
        response: dict = evaluate_request(request)

        # --- Assert ---
        self.assertEqual({'cancelled_correlation_id': '4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c',
                          'policy_id': '8571',
                          'replace': False}, response)
