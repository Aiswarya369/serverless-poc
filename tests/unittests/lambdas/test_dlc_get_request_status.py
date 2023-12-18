import unittest
from unittest.mock import Mock

from src.lambdas import dlc_get_request_status
from src.lambdas.dlc_get_request_status import get_status, RequestNotFoundException


class TestDlcGetRequestStatus(unittest.TestCase):
    correlation_id: str = "CORRELATION_ID"
    status: str = "STATUS"

    def test_get_status_exists(self):
        # --- Arrange ---
        mock_dynamodb_table: Mock = Mock()
        mock_dynamodb_table.get_item.return_value = {
            "Item": {
                "currentStg": self.status
            }
        }

        dlc_get_request_status.DYNAMODB_RESOURCE = Mock()
        dlc_get_request_status.DYNAMODB_RESOURCE.Table = Mock()
        dlc_get_request_status.DYNAMODB_RESOURCE.Table.return_value = mock_dynamodb_table

        # --- Act ---
        got_status: str = get_status(self.correlation_id)

        # --- Assert ---
        self.assertEqual(self.status, got_status)

    def test_get_status_not_exists(self):
        # --- Arrange ---
        mock_dynamodb_table: Mock = Mock()
        mock_dynamodb_table.get_item.return_value = {}

        dlc_get_request_status.DYNAMODB_RESOURCE = Mock()
        dlc_get_request_status.DYNAMODB_RESOURCE.Table = Mock()
        dlc_get_request_status.DYNAMODB_RESOURCE.Table.return_value = mock_dynamodb_table

        # --- Act ---
        with self.assertRaises(RequestNotFoundException) as context:
            get_status(self.correlation_id)

        # --- Assert ---
        self.assertEqual("Correlation id not found", context.exception.message)
