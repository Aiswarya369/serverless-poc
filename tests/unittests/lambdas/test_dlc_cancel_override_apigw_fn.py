import unittest
from http import HTTPStatus
from unittest import mock

from msi_common import Stage

from src.lambdas.dlc_cancel_override_apigw_fn import initiate_step_function, process_request
from src.utils.request_validator import ValidationError


class TestInitiateStepFunction(unittest.TestCase):
    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.StateMachineHandler")
    def test_initiate_step_function_in_progress(self, mock_sm_handler):
        # --- Arrange ---
        correlation_id = "12345"
        request = {}

        mock_sm_handler.return_value.initiate.return_value = {
            "ResponseMetadata":
                {"HTTPStatusCode": 200},
            "startDate": "2022-01-02 10:00:00",
            "executionArn": "1234567890"
        }

        # --- Act ---
        response: dict = initiate_step_function(correlation_id, request)

        # --- Assert ---
        self.assertEqual({'body': '{"message": "DLC cancel request in progress", "correlation_id": '
                                  '"12345"}',
                          'statusCode': 200}, response)

    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.StateMachineHandler")
    def test_initiate_step_function_400_error(self, mock_sm_handler):
        # --- Arrange ---
        correlation_id = "12345"
        request = {}

        mock_sm_handler.return_value.initiate.return_value = {"ResponseMetadata": {"HTTPStatusCode": 400}, "startDate": "2022-01-02 10:00:00"}

        # --- Act ---
        with self.assertRaises(RuntimeError) as exc_info:
            initiate_step_function(correlation_id, request)

        # --- Assert ---
        self.assertEqual('Launching DLC cancel request step function resulted in 400 status code', str(exc_info.exception))


class TestProcessRequest(unittest.TestCase):

    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.initiate_step_function")
    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.get_header_record")
    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.RequestValidator")
    def test_process_request(self, mock_request_validator, mock_get_header_record, mock_initiate):
        # --- Arrange ---
        data = {
            "pathParameters": {
                "subscription_id": "2d45fe05f3e44be38673b9ffd11fa2db"
            },
            "queryStringParameters": {
                "correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
                "subscriber": "ORIGIN"
            }
        }

        mock_request_validator.validate_subscription.return_value = ({'subName': "ORIGIN"}, None)
        mock_get_header_record.return_value = {
            "subId": "2d45fe05f3e44be38673b9ffd11fa2db",
            "currentStg": Stage.RECEIVED.value,
            "rqstEndDt": "2050-12-12 10:00:00+10:00",
            "plcyId": "56789",
            "site": "site",
            "mtrSrlNo": "msn",
            "overrdValue": "overrdValue",
            "rqstStrtDt": "rqstStrtDt",
            "extnddBy": "extnddBy",
        }

        mock_initiate.return_value = {'body': '{"message": "DLC cancel request in progress", '
                                              '"correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c"}',
                                      'statusCode': 200}
        # --- Act ---
        response = process_request(data)

        # --- Assert ---
        self.assertEqual({'body': '{"message": "DLC cancel request in progress", '
                                  '"correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c"}',
                          'statusCode': 200}, response)

        expected_request = {
            "correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
            "site": "site",
            "switch_addresses": ["msn"],
            "status": "overrdValue",
            "meter_serial_number": "msn",
            "current_stage": Stage.RECEIVED.value,
            "start_datetime": "rqstStrtDt",
            "end_datetime": "2050-12-12 10:00:00+10:00",
            "policy_id": 56789,
            "extended_by": "extnddBy",
            "subscription_id": "2d45fe05f3e44be38673b9ffd11fa2db"
        }
        mock_initiate.assert_called_once_with("4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c", expected_request)

    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.RequestValidator")
    def test_process_request_invalid_subscription(self, mock_request_validator):
        # --- Arrange ---
        data = {
            "pathParameters": {
                "subscription_id": "2d45fe05f3e44be38673b9ffd11fa2db"
            },
            "queryStringParameters": {
                "correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
                "subscriber": "ORIGIN"
            }
        }

        mock_request_validator.validate_subscription.return_value = (
            None, [ValidationError("No active subscription found for the supplied site and subscription id")])

        # --- Act ---
        response = process_request(data)

        # --- Assert ---
        self.assertEqual({'body': '{"message": "Subscription id 2d45fe05f3e44be38673b9ffd11fa2db is not '
                                  'valid", "correlation_id": '
                                  '"4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c"}',
                          'statusCode': HTTPStatus.BAD_REQUEST}, response)

    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.RequestValidator")
    def test_process_request_invalid_subscriber(self, mock_request_validator):
        # --- Arrange ---
        data = {
            "pathParameters": {
                "subscription_id": "2d45fe05f3e44be38673b9ffd11fa2db"
            },
            "queryStringParameters": {
                "correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
                "subscriber": "ORIGIN"
            }
        }

        mock_request_validator.validate_subscription.return_value = ({'subName': "NOT_ORIGIN"}, None)

        # --- Act ---
        response = process_request(data)

        # --- Assert ---
        self.assertEqual({'body': '{"message": "Given subscriber ORIGIN does not own the subscription '
                                  '2d45fe05f3e44be38673b9ffd11fa2db", "correlation_id": '
                                  '"4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c"}',
                          'statusCode': HTTPStatus.BAD_REQUEST}, response)

    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.get_header_record")
    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.RequestValidator")
    def test_process_request_no_header(self, mock_request_validator, mock_get_header_record):
        # --- Arrange ---
        data = {
            "pathParameters": {
                "subscription_id": "2d45fe05f3e44be38673b9ffd11fa2db"
            },
            "queryStringParameters": {
                "correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
                "subscriber": "ORIGIN"
            }
        }

        mock_request_validator.validate_subscription.return_value = ({'subName': "ORIGIN"}, None)
        mock_get_header_record.return_value = None
        # --- Act ---
        response = process_request(data)

        # --- Assert ---
        self.assertEqual({'body': '{"message": "Correlation id 4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c not found", '
                                  '"correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c"}',
                          'statusCode': HTTPStatus.BAD_REQUEST}, response)

    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.get_header_record")
    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.RequestValidator")
    def test_process_request_subscription_not_match(self, mock_request_validator, mock_get_header_record):
        # --- Arrange ---
        data = {
            "pathParameters": {
                "subscription_id": "2d45fe05f3e44be38673b9ffd11fa2db"
            },
            "queryStringParameters": {
                "correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
                "subscriber": "ORIGIN"
            }
        }

        mock_request_validator.validate_subscription.return_value = ({'subName': "ORIGIN"}, None)
        mock_get_header_record.return_value = {"subId": "NOT_2d45fe05f3e44be38673b9ffd11fa2db"}
        # --- Act ---
        response = process_request(data)

        # --- Assert ---
        self.assertEqual({
            'body': '{"message": "Subscription id 2d45fe05f3e44be38673b9ffd11fa2db does not match the subscription id '
                    'of the override request to cancel", '
                    '"correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c"}',
            'statusCode': HTTPStatus.BAD_REQUEST}, response)

    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.get_header_record")
    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.RequestValidator")
    def test_process_request_wrong_stage(self, mock_request_validator, mock_get_header_record):
        # --- Arrange ---
        data = {
            "pathParameters": {
                "subscription_id": "2d45fe05f3e44be38673b9ffd11fa2db"
            },
            "queryStringParameters": {
                "correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
                "subscriber": "ORIGIN"
            }
        }

        mock_request_validator.validate_subscription.return_value = ({'subName': "ORIGIN"}, None)
        mock_get_header_record.return_value = {
            "subId": "2d45fe05f3e44be38673b9ffd11fa2db",
            "currentStg": Stage.CANCELLED.value
        }
        # --- Act ---
        response = process_request(data)

        # --- Assert ---
        self.assertEqual({
            'body': '{"message": "Load control request in state: CANCELLED - cannot cancel from this state", '
                    '"correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c"}',
            'statusCode': HTTPStatus.BAD_REQUEST}, response)

    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.get_header_record")
    @mock.patch("src.lambdas.dlc_cancel_override_apigw_fn.RequestValidator")
    def test_process_request_end_in_past(self, mock_request_validator, mock_get_header_record):
        # --- Arrange ---
        data = {
            "pathParameters": {
                "subscription_id": "2d45fe05f3e44be38673b9ffd11fa2db"
            },
            "queryStringParameters": {
                "correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
                "subscriber": "ORIGIN"
            }
        }

        mock_request_validator.validate_subscription.return_value = ({'subName': "ORIGIN"}, None)
        mock_get_header_record.return_value = {
            "subId": "2d45fe05f3e44be38673b9ffd11fa2db",
            "currentStg": Stage.RECEIVED.value,
            "rqstEndDt": "2000-12-12 10:00:00+10:00"
        }
        # --- Act ---
        response = process_request(data)

        # --- Assert ---
        self.assertEqual({
            'body': '{"message": "Request given has an end date in the past so is already completed", '
                    '"correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c"}',
            'statusCode': HTTPStatus.BAD_REQUEST}, response)
