import unittest

from src.utils.request_validator import RequestValidator, ValidationError

OVERRIDE_DURATION: int = 30


class TestRequestValidator(unittest.TestCase):
    def test_request_empty(self):
        # --- Arrange ---
        request: dict = {}

        # --- Act ---
        errors = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], ValidationError("Request is empty"))

    def test_request_missing_site(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "start_datetime": "2100-11-10T02:49:14+10:00",
            "end_datetime": "2100-11-10T03:49:14+10:00",
            "status": "ON",
            "switch_addresses": [
                "LG000001/E3"
            ]
        }

        # --- Act ---
        errors = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], ValidationError("Site details required"))

    def test_request_site_empty(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "",
            "start_datetime": "2100-11-10T02:49:14+10:00",
            "end_datetime": "2100-11-10T03:49:14+10:00",
            "status": "ON",
            "switch_addresses": [
                "LG000001/E3"
            ]
        }

        # --- Act ---
        errors = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], ValidationError("Site details required"))

    def test_request_missing_switch_addresses(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2100-11-10T02:49:14+10:00",
            "end_datetime": "2100-11-10T03:49:14+10:00",
            "status": "ON"
        }

        # --- Act ---
        errors = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], ValidationError("Switch addresses (Meter Details) required"))

    def test_request_switch_addresses_empty_list(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2100-11-10T02:49:14+10:00",
            "end_datetime": "2100-11-10T03:49:14+10:00",
            "status": "ON",
            "switch_addresses": []
        }

        # --- Act ---
        errors = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], ValidationError("Switch addresses (Meter Details) required"))

    def test_request_switch_addresses_empty_str(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2100-11-10T02:49:14+10:00",
            "end_datetime": "2100-11-10T03:49:14+10:00",
            "status": "ON",
            "switch_addresses": ""
        }

        # --- Act ---
        errors = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], ValidationError("Switch addresses (Meter Details) required"))

    def test_request_switch_addresses_over_1(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2100-11-10T02:49:14+10:00",
            "end_datetime": "2100-11-10T03:49:14+10:00",
            "status": "ON",
            "switch_addresses": [
                "METERA",
                "METERB"
            ]
        }

        # --- Act ---
        errors = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], ValidationError("Multiple switch addresses supplied - expected one"))

    def test_request_status_missing(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2100-11-10T02:49:14+10:00",
            "end_datetime": "2100-11-10T03:49:14+10:00",
            "switch_addresses": [
                "METERA"
            ]
        }

        # --- Act ---
        errors = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], ValidationError("DLC status required"))

    def test_request_status_not_on_off(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2100-11-10T02:49:14+10:00",
            "end_datetime": "2100-11-10T03:49:14+10:00",
            "status": "What is this!!!",
            "switch_addresses": [
                "METERA"
            ]
        }

        # --- Act ---
        errors = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], ValidationError("DLC status should be either ON or OFF"))

    def test_request_start_datetime_wrong_format(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2100-11-10T03:49:14",
            "end_datetime": "2100-11-10T03:49:14+10:00",
            "status": "ON",
            "switch_addresses": [
                "METERA"
            ]
        }

        # --- Act ---
        errors = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            errors[0],
            ValidationError("Invalid start datetime format supplied - should be YYYY-mm-ddTHH:MM:SS+zz:zz")
        )

    def test_request_end_datetime_wrong_format(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2100-11-10T03:49:14+00:00",
            "end_datetime": "2100-11-10 03:49:14+10:00",
            "status": "ON",
            "switch_addresses": [
                "METERA"
            ]
        }

        # --- Act ---
        errors = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            errors[0],
            ValidationError("Invalid end datetime format supplied - should be YYYY-mm-ddTHH:MM:SS+zz:zz")
        )

    def test_request_end_datetime_with_start_datetime_missing(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "end_datetime": "2100-11-10T03:49:14+10:00",
            "status": "ON",
            "switch_addresses": [
                "METERA"
            ]
        }

        # --- Act ---
        errors = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], ValidationError("Cannot have an end_datetime without a start_datetime"))

    def test_request_no_end_date_derived_before_now(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2000-01-01T00:00:00+00:00",
            "status": "ON",
            "switch_addresses": [
                "METERA"
            ]
        }

        # --- Act ---
        errors: list = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            errors[0],
            ValidationError("No end date supplied: request's derived end date would be in the past")
        )

    def test_request_end_date_equals_start_date(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2100-01-01T00:00:00+00:00",
            "end_datetime": "2100-01-01T00:00:00+00:00",
            "status": "ON",
            "switch_addresses": [
                "METERA"
            ]
        }

        # --- Act ---
        errors: list = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            errors[0],
            ValidationError("Request's end date is the same as the start date")
        )

    def test_request_end_date_before_start_date(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2200-01-01T00:00:00+00:00",
            "end_datetime": "2100-01-01T00:00:00+00:00",
            "status": "ON",
            "switch_addresses": [
                "METERA"
            ]
        }

        # --- Act ---
        errors: list = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            errors[0],
            ValidationError("Request's end date is before the start date")
        )

    def test_request_end_date_in_past(self):
        # --- Arrange ---
        request: dict = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2000-01-01T00:00:00+00:00",
            "end_datetime": "2001-01-01T00:00:00+00:00",
            "status": "ON",
            "switch_addresses": [
                "METERA"
            ]
        }

        # --- Act ---
        errors: list = RequestValidator.validate_dlc_override_request(request, OVERRIDE_DURATION)

        # --- Assert ---
        self.assertEqual(len(errors), 1)
        self.assertEqual(
            errors[0],
            ValidationError("Request's end date is in the past")
        )
