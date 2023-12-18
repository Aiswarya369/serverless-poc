import datetime
import json
import uuid
from http import HTTPStatus
from unittest import TestCase, mock
from datetime import datetime, timezone, timedelta

from cresconet_aws.support import SupportMessage
from msi_common import Stage

from src.lambdas.dlc_override_apigw_fn import job_entry, DEFAULT_OVERRIDE_DURATION_MINUTES, LOAD_CONTROL_SERVICE_NAME, \
    INSTANCE_DATE_FORMAT, create_correlation_id, UTC_OFFSET, report_errors, KINESIS_DATA_STREAM_NAME, format_response, \
    add_request_on_throttle_queue, OVERRIDE_THROTTLING_QUEUE, lambda_handler
from src.model.LCMeterEvent import LCMeterEvent
from src.utils.request_validator import ValidationError

# Constants
FILE_PREFIX = "src.lambdas.dlc_override_apigw_fn"


class TestJobEntry(TestCase):

    def test_event_missing_body(self):
        # --- Arrange ---
        event = {}

        # --- Act, Assert ---
        self.assertRaises(RuntimeError, job_entry, event)

    def test_raises_error_if_body_is_not_json(self):
        # --- Arrange ---
        event = {
            "body": "asdasd"
        }

        # --- Act, Assert ---
        self.assertRaises(json.JSONDecodeError, job_entry, event)

    @mock.patch(f"{FILE_PREFIX}.RequestValidator")
    def test_receives_bad_request_on_request_validation_errors(self, mock_request_validator: mock.Mock):
        # --- Arrange ---
        mock_request_validator.validate_dlc_override_request.return_value = [ValidationError("some error")]
        request = {}
        event = {
            "body": json.dumps(request),
            "pathParameters": {"subscription_id": "some_random_subscription_id"}
        }
        expected_response = {
            'statusCode': HTTPStatus.BAD_REQUEST.value,
            'body': json.dumps({
                "correlation_id": None,
                "message": "Invalid request: found 1 error(s)",
                "errorDetails": ["some error"]
            })
        }

        # --- Act ---
        response = job_entry(event)

        # --- Assert ---
        self.assertEqual(response, expected_response)
        mock_request_validator.validate_dlc_override_request.assert_called_once_with(request,
                                                                                     DEFAULT_OVERRIDE_DURATION_MINUTES)

    @mock.patch(f"{FILE_PREFIX}.datetime")
    @mock.patch(f"{FILE_PREFIX}.create_tracker")
    @mock.patch(f"{FILE_PREFIX}.create_correlation_id")
    @mock.patch(f"{FILE_PREFIX}.RequestValidator")
    def test_raises_error_if_it_fails_to_create_tracker(self, mock_request_validator: mock.Mock,
                                                        mock_create_correlation_id: mock.Mock,
                                                        mock_create_tracker: mock.Mock,
                                                        mock_datetime: mock.Mock):
        # --- Arrange ---
        current_time = datetime.now(tz=timezone.utc)
        mock_datetime.now.return_value = current_time
        mock_request_validator.validate_dlc_override_request.return_value = []
        correlation_id = "some_random_correlation_id"
        mock_create_correlation_id.return_value = correlation_id
        mock_create_tracker.side_effect = Exception()
        request = {
            "site": "XXXXX",
            "status": "ON",
            "switch_addresses": "LG123XXX"
        }
        event = {
            "pathParameters": {"subscription_id": "some_random_subscription_id"},
            "body": json.dumps(request),
        }

        # --- Act, Assert ---
        self.assertRaises(Exception, job_entry, event)
        mock_datetime.now.assert_called_once_with(tz=timezone.utc)
        mock_create_correlation_id.assert_called_once_with("XXXXX", current_time)
        mock_create_tracker.assert_called_once_with(
            correlation_id=correlation_id,
            sub_id="some_random_subscription_id",
            request_site="XXXXX",
            serial_no="LG123XXX",
            override="ON"
        )

    @mock.patch(f"{FILE_PREFIX}.report_errors")
    @mock.patch(f"{FILE_PREFIX}.datetime")
    @mock.patch(f"{FILE_PREFIX}.create_tracker")
    @mock.patch(f"{FILE_PREFIX}.create_correlation_id")
    @mock.patch(f"{FILE_PREFIX}.RequestValidator")
    def test_invalid_subscription_results_in_bad_request(self, mock_request_validator: mock.Mock,
                                                         mock_create_correlation_id: mock.Mock,
                                                         mock_create_tracker: mock.Mock,
                                                         mock_datetime: mock.Mock,
                                                         mock_report_errors: mock.Mock):
        # --- Arrange ---
        current_time = datetime.now(tz=timezone.utc)
        subscription_errors = [ValidationError("some error")]
        mock_datetime.now.return_value = current_time
        mock_request_validator.validate_dlc_override_request.return_value = []
        mock_request_validator.validate_subscription.return_value = ({}, subscription_errors)
        correlation_id = "some_random_correlation_id"
        mock_create_correlation_id.return_value = correlation_id
        mock_create_tracker.return_value = None
        request = {
            "site": "XXXXX",
            "status": "ON",
            "switch_addresses": "LG123XXX"
        }
        event = {
            "pathParameters": {"subscription_id": "some_random_subscription_id"},
            "body": json.dumps(request),
        }
        expected_response = {
            'statusCode': HTTPStatus.BAD_REQUEST.value,
            'body': json.dumps({
                "correlation_id": correlation_id,
                "message": "Invalid request: found 1 subscription error(s)",
                "errorDetails": ["some error"]
            })
        }

        # --- Act, Assert ---
        response = job_entry(event)
        self.assertEqual(response, expected_response)
        mock_report_errors.assert_called_once_with(correlation_id, current_time, subscription_errors)
        mock_request_validator.validate_subscription.assert_called_once_with("some_random_subscription_id",
                                                                             LOAD_CONTROL_SERVICE_NAME)

    @mock.patch(f"{FILE_PREFIX}.report_errors")
    @mock.patch(f"{FILE_PREFIX}.datetime")
    @mock.patch(f"{FILE_PREFIX}.create_tracker")
    @mock.patch(f"{FILE_PREFIX}.create_correlation_id")
    @mock.patch(f"{FILE_PREFIX}.RequestValidator")
    def test_invalid_request_duration_results_in_bad_request(self, mock_request_validator: mock.Mock,
                                                             mock_create_correlation_id: mock.Mock,
                                                             mock_create_tracker: mock.Mock,
                                                             mock_datetime: mock.Mock,
                                                             mock_report_errors: mock.Mock):
        # --- Arrange ---
        current_time = datetime.now(tz=timezone.utc)
        duration_errors = [ValidationError("some error")]
        mock_datetime.now.return_value = current_time
        mock_request_validator.validate_dlc_override_request.return_value = []
        mock_request_validator.validate_subscription.return_value = ({}, [])
        mock_request_validator.validate_request_duration.return_value = duration_errors
        correlation_id = "some_random_correlation_id"
        mock_create_correlation_id.return_value = correlation_id
        mock_create_tracker.return_value = None
        request = {
            "site": "XXXXX",
            "status": "ON",
            "switch_addresses": "LG123XXX"
        }
        event = {
            "pathParameters": {"subscription_id": "some_random_subscription_id"},
            "body": json.dumps(request),
        }
        expected_response = {
            'statusCode': HTTPStatus.BAD_REQUEST.value,
            'body': json.dumps({
                "correlation_id": correlation_id,
                "message": "Invalid request: found 1 error(s)",
                "errorDetails": ["some error"]
            })
        }

        # --- Act, Assert ---
        response = job_entry(event)
        self.assertEqual(response, expected_response)
        mock_report_errors.assert_called_once_with(correlation_id, current_time, duration_errors)
        mock_request_validator.validate_request_duration.assert_called_once_with(
            {**request, "correlation_id": correlation_id},
            DEFAULT_OVERRIDE_DURATION_MINUTES)

    @mock.patch(f"{FILE_PREFIX}.add_request_on_throttle_queue")
    @mock.patch(f"{FILE_PREFIX}.report_errors")
    @mock.patch(f"{FILE_PREFIX}.datetime")
    @mock.patch(f"{FILE_PREFIX}.create_tracker")
    @mock.patch(f"{FILE_PREFIX}.create_correlation_id")
    @mock.patch(f"{FILE_PREFIX}.RequestValidator")
    def test_success(self, mock_request_validator: mock.Mock,
                     mock_create_correlation_id: mock.Mock,
                     mock_create_tracker: mock.Mock,
                     mock_datetime: mock.Mock,
                     mock_report_errors: mock.Mock,
                     mock_add_request_on_throttle_queue: mock.Mock):
        # --- Arrange ---
        current_time = datetime.now(tz=timezone.utc)
        mock_datetime.now.return_value = current_time
        mock_request_validator.validate_dlc_override_request.return_value = []
        mock_request_validator.validate_subscription.return_value = ({}, [])
        mock_request_validator.validate_request_duration.return_value = []
        correlation_id = "some_random_correlation_id"
        mock_create_correlation_id.return_value = correlation_id
        mock_create_tracker.return_value = None
        request = {
            "site": "XXXXX",
            "status": "ON",
            "switch_addresses": ["LG123XXX"]
        }
        event = {
            "pathParameters": {"subscription_id": "some_random_subscription_id"},
            "body": json.dumps(request),
        }
        expected_response = {
            'statusCode': HTTPStatus.OK.value,
            'body': json.dumps({
                "correlation_id": correlation_id,
                "message": "DLC request accepted",
            })
        }
        mock_add_request_on_throttle_queue.return_value = expected_response

        # --- Act, Assert ---
        response = job_entry(event)
        self.assertEqual(response, expected_response)
        mock_report_errors.assert_not_called()
        mock_add_request_on_throttle_queue.assert_called_once_with(correlation_id,
                                                                   {**request, "correlation_id": correlation_id})


class TestCreateCorrelationId(TestCase):
    @mock.patch(f"{FILE_PREFIX}.uuid")
    def test_success(self, mock_uuid: mock.Mock):
        # --- Arrange ---
        site = "some_random_siteXXX"
        now = datetime.now(tz=timezone.utc)
        uuid_value = uuid.uuid4()
        mock_uuid.uuid4.return_value = uuid_value
        expected_date_component = (now + timedelta(hours=UTC_OFFSET)).strftime(INSTANCE_DATE_FORMAT)
        expected_response = f"some_random_siteXXX-{expected_date_component}-{str(uuid_value)}"

        # --- Act ---
        response = create_correlation_id(site, now)

        # --- Assert ---
        self.assertEqual(response, expected_response)


@mock.patch(f"{FILE_PREFIX}.assemble_event_payload")
@mock.patch(f"{FILE_PREFIX}.deliver_to_kinesis")
@mock.patch(f"{FILE_PREFIX}.update_tracker")
class TestReportErrors(TestCase):

    def test_error_message_is_list_of_errors(self, mock_update_tracker: mock.Mock, mock_deliver_to_kinesis: mock.Mock,
                                             mock_assemble_event_payload: mock.Mock):
        # --- Arrange ---
        error_message = [ValidationError("some_error"), ValidationError("some_other_error")]
        error_datetime = datetime.now(tz=timezone.utc)
        correlation_id = "some_random_correlation_id"
        event_payload = LCMeterEvent(
            subscription_id="subscription_id",
            correlation_id=correlation_id,
            site="site",
            meter_serial_number="meter_serial_no",
            event_description="some_error; some_other_error",
            milestone=Stage.DECLINED,
            event_datetime_str=error_datetime.isoformat(timespec='seconds')
        ).as_camelcase_dict()
        mock_assemble_event_payload.return_value = event_payload

        # --- Act ---
        report_errors(correlation_id, error_datetime, error_message)

        # --- Assert ---
        mock_update_tracker.assert_called_once_with(correlation_id=correlation_id,
                                                    stage=Stage.DECLINED,
                                                    event_datetime=error_datetime,
                                                    message="some_error; some_other_error")
        mock_assemble_event_payload.assert_called_once_with(correlation_id, Stage.DECLINED, error_datetime,
                                                            "some_error; some_other_error")
        mock_deliver_to_kinesis.assert_called_once_with(event_payload, KINESIS_DATA_STREAM_NAME)

    def test_error_message_is_string(self, mock_update_tracker: mock.Mock, mock_deliver_to_kinesis: mock.Mock,
                                     mock_assemble_event_payload: mock.Mock):
        # --- Arrange ---
        error_message = "some_extreme_error"
        error_datetime = datetime.now(tz=timezone.utc)
        correlation_id = "some_random_correlation_id"
        event_payload = LCMeterEvent(
            subscription_id="subscription_id",
            correlation_id=correlation_id,
            site="site",
            meter_serial_number="meter_serial_no",
            event_description=error_message,
            milestone=Stage.DECLINED,
            event_datetime_str=error_datetime.isoformat(timespec='seconds')
        ).as_camelcase_dict()
        mock_assemble_event_payload.return_value = event_payload

        # --- Act ---
        report_errors(correlation_id, error_datetime, error_message)

        # --- Assert ---
        mock_update_tracker.assert_called_once_with(correlation_id=correlation_id,
                                                    stage=Stage.DECLINED,
                                                    event_datetime=error_datetime,
                                                    message=error_message)
        mock_assemble_event_payload.assert_called_once_with(correlation_id, Stage.DECLINED, error_datetime,
                                                            error_message)
        mock_deliver_to_kinesis.assert_called_once_with(event_payload, KINESIS_DATA_STREAM_NAME)


class TestFormatResponse(TestCase):

    def test_dictionary_response_body(self):
        # --- Arrange ---
        status_code = HTTPStatus.OK
        body = {"some": "random", "data": 23}
        expected_response = {
            'statusCode': status_code.value,
            'body': json.dumps(body)
        }

        # --- Act ---
        response = format_response(status_code, body)

        # --- Assert ---
        self.assertEqual(response, expected_response)

    def test_string_response_body(self):
        # --- Arrange ---
        status_code = HTTPStatus.OK
        body = "Some random body content"
        expected_response = {
            'statusCode': status_code.value,
            'body': body
        }

        # --- Act ---
        response = format_response(status_code, body)

        # --- Assert ---
        self.assertEqual(response, expected_response)


@mock.patch(f"{FILE_PREFIX}.report_errors")
@mock.patch(f"{FILE_PREFIX}.send_message_to_support")
@mock.patch(f"{FILE_PREFIX}.send_sqs_message")
class TestAddRequestOnThrottleQueue(TestCase):

    @mock.patch(f"{FILE_PREFIX}.datetime")
    def test_exception_thrown_on_send_sqs_message(self, mock_datetime: mock.Mock,
                                                  mock_send_sqs_message: mock.Mock,
                                                  mock_send_message_to_support: mock.Mock,
                                                  mock_report_errors: mock.Mock):
        # --- Arrange ---
        current_time = datetime.now(tz=timezone.utc)
        mock_datetime.now.return_value = current_time
        correlation_id = "some_random_correlation_id"
        request = {
            "site": "XXXXX",
            "status": "ON",
            "switch_addresses": ["LG123XXX"]
        }
        mock_send_sqs_message.side_effect = Exception("broken")
        expected_response = {
            'statusCode': 500,
            'body': json.dumps({
                "message": "DLC Request failed with internal error",
                "correlation_id": correlation_id,
                "error": "broken"
            })
        }

        # --- Act ---
        response = add_request_on_throttle_queue(correlation_id, request)

        # --- Assert ---
        self.assertEqual(response, expected_response)
        mock_send_sqs_message.assert_called_once_with(OVERRIDE_THROTTLING_QUEUE, request)
        mock_datetime.now.assert_called_once_with(tz=timezone.utc)
        mock_report_errors.assert_called_once_with(correlation_id, current_time, "broken")
        mock_send_message_to_support.assert_called_once_with(
            SupportMessage(reason="DLC Request failed with internal error",
                           subject="Load Control Override has Failed - Internal Error",
                           tags="MSI, Load Control"),
            correlation_id=correlation_id)

    @mock.patch(f"{FILE_PREFIX}.datetime")
    def test_non_ok_status_code_returned(self, mock_datetime: mock.Mock,
                                         mock_send_sqs_message: mock.Mock,
                                         mock_send_message_to_support: mock.Mock,
                                         mock_report_errors: mock.Mock):
        # --- Arrange ---
        current_time = datetime.now(tz=timezone.utc)
        mock_datetime.now.return_value = current_time
        correlation_id = "some_random_correlation_id"
        error_message = "Sending DLC request to throttling queue resulted in 407 status code"
        request = {
            "site": "XXXXX",
            "status": "ON",
            "switch_addresses": ["LG123XXX"]
        }
        mock_send_sqs_message.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 407}
        }
        expected_response = {
            'statusCode': 500,
            'body': json.dumps({
                "message": "DLC request failed",
                "correlation_id": correlation_id,
                "error": error_message
            })
        }

        # --- Act ---
        response = add_request_on_throttle_queue(correlation_id, request)

        # --- Assert ---
        self.assertEqual(response, expected_response)
        mock_send_sqs_message.assert_called_once_with(OVERRIDE_THROTTLING_QUEUE, request)
        mock_datetime.now.assert_called_once_with(tz=timezone.utc)
        mock_report_errors.assert_called_once_with(correlation_id, current_time, error_message)
        mock_send_message_to_support.assert_called_once_with(
            SupportMessage(reason=error_message,
                           subject="Load Control Override has Failed - Failed Request",
                           tags="MSI, Load Control"),
            correlation_id=correlation_id)

    def test_ok_status_code_returned(self, mock_send_sqs_message: mock.Mock,
                                     mock_send_message_to_support: mock.Mock,
                                     mock_report_errors: mock.Mock):
        # --- Arrange ---
        correlation_id = "some_random_correlation_id"
        error_message = "Sending DLC request to throttling queue resulted in 407 status code"
        request = {
            "site": "XXXXX",
            "status": "ON",
            "switch_addresses": ["LG123XXX"]
        }
        mock_send_sqs_message.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        expected_response = {
            'statusCode': 200,
            'body': json.dumps({
                "message": "DLC request accepted",
                "correlation_id": correlation_id,
            })
        }

        # --- Act ---
        response = add_request_on_throttle_queue(correlation_id, request)

        # --- Assert ---
        self.assertEqual(response, expected_response)
        mock_send_sqs_message.assert_called_once_with(OVERRIDE_THROTTLING_QUEUE, request)
        mock_send_message_to_support.assert_not_called()
        mock_report_errors.assert_not_called()


@mock.patch(f"{FILE_PREFIX}.job_entry")
class TestLambdaHandler(TestCase):

    def test_raises_exception_on_failure(self, mock_job_entry: mock.Mock):
        # --- Arrange ---
        event = {}
        context = {}
        mock_job_entry.side_effect = Exception()

        # --- Act, Assert ---
        self.assertRaises(Exception, lambda_handler, event, context)
        mock_job_entry.assert_called_once_with(event)

    def test_success(self, mock_job_entry: mock.Mock):
        # --- Arrange ---
        event = {}
        context = {}
        expected_response = {
            "statusCode": 200,
            "body": "{}"
        }
        mock_job_entry.return_value = expected_response

        # --- Act, Assert ---
        response = lambda_handler(event, context)
        self.assertEqual(response, expected_response)
        mock_job_entry.assert_called_once_with(event)
