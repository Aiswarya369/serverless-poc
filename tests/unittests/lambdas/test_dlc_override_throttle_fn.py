import datetime
import json
from unittest import mock, TestCase
from datetime import datetime, timezone, timedelta

from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from cresconet_aws.support import SupportMessage
from msi_common import Stage
from src.model.LCMeterEvent import LCMeterEvent
from src.lambdas.dlc_override_throttle_fn import report_error_to_client, \
    KINESIS_DATA_STREAM_NAME, LOAD_CONTROL_ALERT_FORMAT, report_error_to_support, \
    DEFAULT_OVERRIDE_DURATION_MINUTES, update_start_end_times_on_request, \
    initiate_step_function, record_handler, lambda_handler, processor

# Constants
FILE_PREFIX = "src.lambdas.dlc_override_throttle_fn"


class ExecutionAlreadyExists(Exception):
    pass


@mock.patch(f"{FILE_PREFIX}.datetime")
@mock.patch(f"{FILE_PREFIX}.update_tracker")
@mock.patch(f"{FILE_PREFIX}.assemble_event_payload")
@mock.patch(f"{FILE_PREFIX}.deliver_to_kinesis")
class TestReportErrorToClient(TestCase):
    def test_success_with_required_fields(self, mock_deliver_to_kinesis: mock.Mock,
                                          mock_assemble_event_payload: mock.Mock,
                                          mock_update_tracker: mock.Mock, mock_datetime: mock.Mock):
        # --- Arrange ---
        correlation_id = "some_random_id"
        message = "some error"
        current_time = datetime.now(timezone.utc)
        mock_datetime.now.return_value = current_time
        mock_update_tracker.return_value = None
        event_payload = LCMeterEvent(
            subscription_id="subscription_id",
            correlation_id=correlation_id,
            site="CNTEST0001",
            meter_serial_number="LG1234XXX",
            event_description=message,
            milestone=Stage.DECLINED,
            event_datetime_str=current_time.isoformat(timespec='seconds')
        ).as_camelcase_dict()
        mock_assemble_event_payload.return_value = event_payload
        mock_deliver_to_kinesis.return_value = None

        # --- Act ---
        response = report_error_to_client(correlation_id=correlation_id, message=message)

        # --- Assert ---
        self.assertIsNone(response)
        mock_update_tracker.assert_called_once_with(
            correlation_id=correlation_id,
            stage=Stage.DECLINED,
            event_datetime=current_time,
            message=message,
            request_start_date=None,
            request_end_date=None)
        mock_assemble_event_payload.assert_called_once_with(correlation_id, Stage.DECLINED, current_time, message)
        mock_deliver_to_kinesis.assert_called_once_with(event_payload, KINESIS_DATA_STREAM_NAME)

    def test_success_with_required_fields_and_optional_fields(self, mock_deliver_to_kinesis: mock.Mock,
                                                              mock_assemble_event_payload: mock.Mock,
                                                              mock_update_tracker: mock.Mock, mock_datetime: mock.Mock):
        # --- Arrange ---
        correlation_id = "some_random_id"
        message = "some error"
        current_time = datetime.now(timezone.utc)
        mock_datetime.now.return_value = current_time
        mock_update_tracker.return_value = None
        request_start_date = datetime.now()
        event_payload = LCMeterEvent(
            subscription_id="subscription_id",
            correlation_id=correlation_id,
            site="CNTEST0001",
            meter_serial_number="LG1234XXX",
            event_description=message,
            milestone=Stage.DECLINED,
            event_datetime_str=current_time.isoformat(timespec='seconds')
        ).as_camelcase_dict()
        request_end_date = datetime.now()
        mock_assemble_event_payload.return_value = event_payload
        mock_deliver_to_kinesis.return_value = None

        # --- Act ---
        response = report_error_to_client(correlation_id=correlation_id, message=message,
                                          request_start_date=request_start_date, request_end_date=request_end_date)

        # --- Assert ---
        self.assertIsNone(response)
        mock_update_tracker.assert_called_once_with(
            correlation_id=correlation_id,
            stage=Stage.DECLINED,
            event_datetime=current_time,
            message=message,
            request_start_date=request_start_date,
            request_end_date=request_end_date)
        mock_assemble_event_payload.assert_called_once_with(correlation_id, Stage.DECLINED, current_time, message)
        mock_deliver_to_kinesis.assert_called_once_with(event_payload, KINESIS_DATA_STREAM_NAME)


class TestReportErrorToSupport(TestCase):

    @mock.patch(f"{FILE_PREFIX}.send_message_to_support")
    def test_success(self, mock_send_message_to_support: mock.Mock):
        # --- Arrange ---
        correlation_id = "some_random_id"
        subject_hint = "something went wrong!"
        reason = "some random reason"
        mock_send_message_to_support.return_value = None
        support_message = SupportMessage(reason=reason, subject=LOAD_CONTROL_ALERT_FORMAT.format(hint=subject_hint),
                                         tags="MSI, Load Control")

        # --- Act ---
        response = report_error_to_support(correlation_id, reason, subject_hint)

        # --- Assert ---
        self.assertIsNone(response)
        mock_send_message_to_support.assert_called_once_with(support_message, correlation_id=correlation_id)


@mock.patch(f"{FILE_PREFIX}.datetime")
class TestUpdateStartEndTimesOnRequest(TestCase):

    def test_start_and_end_time_not_specified_in_request(self, mock_datetime: mock.Mock):
        # --- Arrange ---
        current_time = datetime.now(tz=timezone.utc)
        expected_start_time = current_time
        expected_end_time = current_time + timedelta(minutes=DEFAULT_OVERRIDE_DURATION_MINUTES)
        mock_datetime.now.return_value = current_time
        mock_datetime.isoformat = datetime.isoformat
        mock_datetime.fromisoformat = datetime.fromisoformat
        request = {}
        expected_new_request = {
            'end_datetime': expected_end_time.isoformat(timespec="seconds"),
            'start_datetime': expected_start_time.isoformat(timespec="seconds")
        }

        # --- Act ---
        response = update_start_end_times_on_request(request)

        # --- Assert ---
        self.assertEqual(len(response), 2)
        start, end = response
        self.assertEqual(start, expected_start_time)
        self.assertEqual(end, expected_end_time)
        self.assertEqual(request, expected_new_request)

    def test_start_time_is_specified_and_end_time_not_specified_in_request(self, mock_datetime: mock.Mock):
        # --- Arrange ---
        current_time = datetime.now(tz=timezone.utc)
        expected_start_time = current_time.replace(microsecond=0)
        expected_end_time = expected_start_time + timedelta(minutes=DEFAULT_OVERRIDE_DURATION_MINUTES)
        mock_datetime.now.return_value = current_time
        mock_datetime.isoformat = datetime.isoformat
        mock_datetime.fromisoformat = datetime.fromisoformat
        request = {
            "start_datetime": current_time.isoformat(timespec="seconds")
        }
        expected_new_request = {
            'end_datetime': expected_end_time.isoformat(timespec="seconds"),
            'start_datetime': expected_start_time.isoformat(timespec="seconds")
        }

        # --- Act ---
        response = update_start_end_times_on_request(request)

        # --- Assert ---
        self.assertEqual(len(response), 2)
        start, end = response
        self.assertEqual(start, expected_start_time)
        self.assertEqual(end, expected_end_time)
        self.assertEqual(request, expected_new_request)

    def test_end_time_is_specified_and_start_time_not_specified_in_request(self, mock_datetime: mock.Mock):
        # --- Arrange ---
        current_time = datetime.now(tz=timezone.utc)
        expected_start_time = current_time
        request_end_time = current_time + timedelta(minutes=38)
        expected_end_time = request_end_time.replace(microsecond=0)
        mock_datetime.now.return_value = current_time
        mock_datetime.isoformat = datetime.isoformat
        mock_datetime.fromisoformat = datetime.fromisoformat
        request = {
            "end_datetime": request_end_time.isoformat(timespec="seconds")
        }
        expected_new_request = {
            'end_datetime': expected_end_time.isoformat(timespec="seconds"),
            'start_datetime': expected_start_time.isoformat(timespec="seconds")
        }

        # --- Act ---
        response = update_start_end_times_on_request(request)

        # --- Assert ---
        self.assertEqual(len(response), 2)
        start, end = response
        self.assertEqual(start, expected_start_time)
        self.assertEqual(end, expected_end_time)
        self.assertEqual(request, expected_new_request)

    def test_end_time_and_start_time_is_specified_in_request(self, mock_datetime: mock.Mock):
        # --- Arrange ---
        current_time = datetime.now(tz=timezone.utc)
        mock_datetime.now.return_value = current_time
        mock_datetime.isoformat = datetime.isoformat
        mock_datetime.fromisoformat = datetime.fromisoformat
        request_start_time = current_time + timedelta(minutes=13)
        request_end_time = request_start_time + timedelta(minutes=37)
        request = {
            "start_datetime": request_start_time.isoformat(timespec="seconds"),
            "end_datetime": request_end_time.isoformat(timespec="seconds")
        }
        expected_new_request = {
            "start_datetime": request_start_time.isoformat(timespec="seconds"),
            "end_datetime": request_end_time.isoformat(timespec="seconds")
        }

        # --- Act ---
        response = update_start_end_times_on_request(request)

        # --- Assert ---
        self.assertEqual(len(response), 2)
        start, end = response
        self.assertEqual(start, request_start_time.replace(microsecond=0))
        self.assertEqual(end, request_end_time.replace(microsecond=0))
        self.assertEqual(request, expected_new_request)


@mock.patch(f"{FILE_PREFIX}.get_step_function_client")
@mock.patch(f"{FILE_PREFIX}.StateMachineHandler.initiate")
class TestInitiateStepFunction(TestCase):

    @mock.patch(f"{FILE_PREFIX}.report_error_to_client")
    @mock.patch(f"{FILE_PREFIX}.report_error_to_support")
    def test_an_error_is_raised(self, mock_report_error_to_support: mock.Mock, mock_report_error_to_client: mock.Mock,
                                mock_sm_handler: mock.Mock, mock_get_step_function_client: mock.Mock):
        # --- Arrange ---
        step_function_client = mock.MagicMock()
        step_function_client.exceptions.ExecutionAlreadyExists = ExecutionAlreadyExists
        correlation_id = "some_random_correlation_id"
        start_datetime = datetime(2023, 5, 12, 10, 5, 0, tzinfo=timezone.utc)
        end_datetime = datetime(2023, 5, 12, 10, 5, 30, tzinfo=timezone.utc)
        request_payload = {
            "start_datetime": start_datetime.isoformat(timespec="seconds"),
            "end_datetime": end_datetime.isoformat(timespec="seconds"),
            "site": "4103861111",
            "switch_addresses": ["TM022014316"],
            "status": "OFF",
            "correlation_id": correlation_id,
        }
        error_message = "OOPS BROKEN"
        mock_get_step_function_client.return_value = step_function_client
        mock_sm_handler.side_effect = Exception(error_message)

        # --- Act ---
        response = initiate_step_function(correlation_id, start_datetime, end_datetime, request_payload)

        # --- Assert ---
        self.assertIsNone(response)
        mock_report_error_to_client.assert_called_once_with(correlation_id=correlation_id, message=error_message)
        mock_report_error_to_support.assert_called_once_with(correlation_id=correlation_id,
                                                             reason="DLC Request failed with internal error",
                                                             subject_hint="Internal Error")

    @mock.patch(f"{FILE_PREFIX}.report_error_to_client")
    @mock.patch(f"{FILE_PREFIX}.report_error_to_support")
    def test_an_execution_already_exists_exception_is_raised(self, mock_report_error_to_support: mock.Mock,
                                                             mock_report_error_to_client: mock.Mock,
                                                             mock_sm_handler: mock.Mock,
                                                             mock_get_step_function_client: mock.Mock):
        # --- Arrange ---
        step_function_client = mock.MagicMock()
        step_function_client.exceptions.ExecutionAlreadyExists = ExecutionAlreadyExists
        correlation_id = "some_random_correlation_id"
        start_datetime = datetime(2023, 5, 12, 10, 5, 0, tzinfo=timezone.utc)
        end_datetime = datetime(2023, 5, 12, 10, 5, 30, tzinfo=timezone.utc)
        request_payload = {
            "start_datetime": start_datetime.isoformat(timespec="seconds"),
            "end_datetime": end_datetime.isoformat(timespec="seconds"),
            "site": "4103861111",
            "switch_addresses": ["TM022014316"],
            "status": "OFF",
            "correlation_id": correlation_id,
        }
        mock_get_step_function_client.return_value = step_function_client
        mock_sm_handler.side_effect = ExecutionAlreadyExists()

        # --- Act ---
        response = initiate_step_function(correlation_id, start_datetime, end_datetime, request_payload)

        # --- Assert ---
        self.assertIsNone(response)
        mock_report_error_to_client.assert_not_called()
        mock_report_error_to_support.assert_not_called()

    @mock.patch(f"{FILE_PREFIX}.report_error_to_client")
    @mock.patch(f"{FILE_PREFIX}.report_error_to_support")
    def test_non_ok_http_response(self, mock_report_error_to_support: mock.Mock,
                                  mock_report_error_to_client: mock.Mock,
                                  mock_sm_handler: mock.Mock,
                                  mock_get_step_function_client: mock.Mock):
        # --- Arrange ---
        step_function_client = mock.MagicMock()
        step_function_client.exceptions.ExecutionAlreadyExists = ExecutionAlreadyExists
        correlation_id = "some_random_correlation_id"
        start_datetime = datetime(2023, 5, 12, 10, 5, 0, tzinfo=timezone.utc)
        end_datetime = datetime(2023, 5, 12, 10, 5, 30, tzinfo=timezone.utc)
        request_payload = {
            "start_datetime": start_datetime.isoformat(timespec="seconds"),
            "end_datetime": end_datetime.isoformat(timespec="seconds"),
            "site": "4103861111",
            "switch_addresses": ["TM022014316"],
            "status": "OFF",
            "correlation_id": correlation_id,
        }
        error_message = "Launching DLC request resulted in 500 status code"
        mock_get_step_function_client.return_value = step_function_client
        mock_sm_handler.return_value = {"ResponseMetadata": {"HTTPStatusCode": 500}}

        # --- Act ---
        response = initiate_step_function(correlation_id, start_datetime, end_datetime, request_payload)

        # --- Assert ---
        self.assertIsNone(response)
        mock_report_error_to_client.assert_called_once_with(correlation_id=correlation_id, message=error_message,
                                                            request_start_date=start_datetime,
                                                            request_end_date=end_datetime)
        mock_report_error_to_support.assert_called_once_with(correlation_id=correlation_id,
                                                             reason="DLC request failed",
                                                             subject_hint="Failed Request")

    @mock.patch(f"{FILE_PREFIX}.update_tracker")
    @mock.patch(f"{FILE_PREFIX}.assemble_event_payload")
    @mock.patch(f"{FILE_PREFIX}.deliver_to_kinesis")
    def test_success(self, mock_deliver_to_kinesis: mock.Mock,
                     mock_assemble_event_payload: mock.Mock,
                     mock_update_tracker: mock.Mock,
                     mock_sm_handler: mock.Mock,
                     mock_get_step_function_client: mock.Mock):
        # --- Arrange ---
        step_function_client = mock.MagicMock()
        step_function_client.exceptions.ExecutionAlreadyExists = ExecutionAlreadyExists
        correlation_id = "some_random_correlation_id"
        start_datetime = datetime(2023, 5, 12, 10, 5, 0, tzinfo=timezone.utc)
        end_datetime = datetime(2023, 5, 12, 10, 5, 30, tzinfo=timezone.utc)
        sm_start_date = datetime(2023, 5, 12, 10, 5, 1, tzinfo=timezone.utc)
        message = "success!"
        request_payload = {
            "start_datetime": start_datetime.isoformat(timespec="seconds"),
            "end_datetime": end_datetime.isoformat(timespec="seconds"),
            "site": "4103861111",
            "switch_addresses": ["TM022014316"],
            "status": "OFF",
            "correlation_id": correlation_id,
        }
        event_payload = LCMeterEvent(
            subscription_id="subscription_id",
            correlation_id=correlation_id,
            site="CNTEST0001",
            meter_serial_number="LG1234XXX",
            event_description=message,
            milestone=Stage.DECLINED,
            event_datetime_str=sm_start_date.isoformat(timespec='seconds')
        ).as_camelcase_dict()
        mock_assemble_event_payload.return_value = event_payload
        mock_get_step_function_client.return_value = step_function_client
        mock_sm_handler.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}, "startDate": sm_start_date,
                                        "executionArn": "some_arn"}

        # --- Act ---
        response = initiate_step_function(correlation_id, start_datetime, end_datetime, request_payload)

        # --- Assert ---
        self.assertIsNone(response)
        mock_update_tracker.assert_called_once_with(
            correlation_id=correlation_id,
            stage=Stage.QUEUED,
            event_datetime=sm_start_date,
            request_start_date=start_datetime,
            request_end_date=end_datetime)
        mock_assemble_event_payload.assert_called_once_with(correlation_id, Stage.QUEUED, sm_start_date)
        mock_deliver_to_kinesis.assert_called_once_with(event_payload, KINESIS_DATA_STREAM_NAME)


@mock.patch(f"{FILE_PREFIX}.is_request_pending_state_machine")
@mock.patch(f"{FILE_PREFIX}.update_start_end_times_on_request")
@mock.patch(f"{FILE_PREFIX}.initiate_step_function")
class TestRecordHandler(TestCase):
    def test_non_pending_request(self, mock_initiate_step_function: mock.Mock,
                                 mock_update_start_end_times_on_request: mock.Mock,
                                 mock_is_request_pending_state_machine: mock.Mock):
        # --- Arrange ---
        mock_is_request_pending_state_machine.return_value = False
        correlation_id = "some_random_correlation_id"
        start_datetime = datetime(2023, 5, 12, 10, 5, 0, tzinfo=timezone.utc)
        end_datetime = datetime(2023, 5, 12, 10, 5, 30, tzinfo=timezone.utc)
        record = SQSRecord({
            "body": json.dumps({
                "start_datetime": start_datetime.isoformat(timespec="seconds"),
                "end_datetime": end_datetime.isoformat(timespec="seconds"),
                "site": "4103861111",
                "switch_addresses": ["TM022014316"],
                "status": "OFF",
                "correlation_id": correlation_id,
            })
        })

        # --- Act ---
        record_handler(record)

        # --- Assert ---
        mock_is_request_pending_state_machine.assert_called_once_with(correlation_id)
        mock_update_start_end_times_on_request.assert_not_called()
        mock_initiate_step_function.assert_not_called()

    @mock.patch(f"{FILE_PREFIX}.datetime")
    @mock.patch(f"{FILE_PREFIX}.report_error_to_client")
    def test_request_with_a_past_end_date(self, mock_report_error_to_client: mock.Mock,
                                          mock_datetime: mock.Mock,
                                          mock_initiate_step_function: mock.Mock,
                                          mock_update_start_end_times_on_request: mock.Mock,
                                          mock_is_request_pending_state_machine: mock.Mock):
        # --- Arrange ---
        mock_datetime.now.return_value = datetime(2023, 5, 12, 11, 5, 30, tzinfo=timezone.utc)
        mock_is_request_pending_state_machine.return_value = True
        correlation_id = "some_random_correlation_id"
        start_datetime = datetime(2023, 5, 12, 10, 5, 0, tzinfo=timezone.utc)
        end_datetime = datetime(2023, 5, 12, 10, 5, 30, tzinfo=timezone.utc)
        mock_update_start_end_times_on_request.return_value = (start_datetime, end_datetime)
        request = {
            "start_datetime": start_datetime.isoformat(timespec="seconds"),
            "end_datetime": end_datetime.isoformat(timespec="seconds"),
            "site": "4103861111",
            "switch_addresses": ["TM022014316"],
            "status": "OFF",
            "correlation_id": correlation_id,
        }
        record = SQSRecord({
            "body": json.dumps(request)
        })

        # --- Act ---
        record_handler(record)

        # --- Assert ---
        mock_is_request_pending_state_machine.assert_called_once_with(correlation_id)
        mock_update_start_end_times_on_request.assert_called_once_with(request)
        mock_report_error_to_client.assert_called_once_with(correlation_id,
                                                            message="Request is rejected as it has a end datetime in the past.")
        mock_initiate_step_function.assert_not_called()

    @mock.patch(f"{FILE_PREFIX}.datetime")
    def test_success(self, mock_datetime: mock.Mock,
                     mock_initiate_step_function: mock.Mock,
                     mock_update_start_end_times_on_request: mock.Mock,
                     mock_is_request_pending_state_machine: mock.Mock):
        # --- Arrange ---
        mock_datetime.now.return_value = datetime(2023, 5, 12, 9, 5, 30, tzinfo=timezone.utc)
        mock_is_request_pending_state_machine.return_value = True
        correlation_id = "some_random_correlation_id"
        start_datetime = datetime(2023, 5, 12, 10, 5, 0, tzinfo=timezone.utc)
        end_datetime = datetime(2023, 5, 12, 10, 5, 30, tzinfo=timezone.utc)
        mock_update_start_end_times_on_request.return_value = (start_datetime, end_datetime)
        request = {
            "start_datetime": start_datetime.isoformat(timespec="seconds"),
            "end_datetime": end_datetime.isoformat(timespec="seconds"),
            "site": "4103861111",
            "switch_addresses": ["TM022014316"],
            "status": "OFF",
            "correlation_id": correlation_id,
        }
        record = SQSRecord({
            "body": json.dumps(request)
        })

        # --- Act ---
        record_handler(record)

        # --- Assert ---
        mock_is_request_pending_state_machine.assert_called_once_with(correlation_id)
        mock_update_start_end_times_on_request.assert_called_once_with(request)
        mock_initiate_step_function.assert_called_once_with(correlation_id=correlation_id, start=start_datetime,
                                                            end=end_datetime, request=request)


@mock.patch(f"{FILE_PREFIX}.process_partial_response")
@mock.patch(f"{FILE_PREFIX}.time")
class TestLambdaHandler(TestCase):

    def test_raises_error_on_exception(self, _mock_time: mock.Mock, mock_process_partial_response: mock.Mock):
        # --- Arrange ---
        record_count = 50
        event = {"Records": [None] * record_count}
        context = LambdaContext()
        mock_process_partial_response.side_effect = Exception()

        # --- Act, Assert ---
        self.assertRaises(Exception, lambda_handler, event, context)
        mock_process_partial_response.assert_called_once_with(event=event, record_handler=record_handler,
                                                              context=context, processor=processor)

    def test_throttling_occurs_if_records_are_processed_too_quickly(self, mock_time: mock.Mock,
                                                                    mock_process_partial_response: mock.Mock):
        # --- Arrange ---
        record_count = 50
        rate_limit_calls, rate_limit_period = 50, 50
        mock_time.time.side_effect = [0, 1]
        event = {"Records": [None] * record_count}
        context = LambdaContext()
        expected_response = {"FailedBatchItems": 5}
        mock_process_partial_response.return_value = expected_response

        # --- Act, Assert ---
        with mock.patch(f"{FILE_PREFIX}.RATE_LIMIT_CALLS", rate_limit_calls):
            with mock.patch(f"{FILE_PREFIX}.RATE_LIMIT_PERIOD_SEC", rate_limit_period):
                response = lambda_handler(event, context)
        self.assertEqual(response, expected_response)
        mock_time.sleep.assert_called_once_with(49)
        mock_process_partial_response.assert_called_once_with(event=event, record_handler=record_handler,
                                                              context=context, processor=processor)

    def test_does_not_throttle_if_requests_take_longer_then_expected(self, mock_time: mock.Mock,
                                                                     mock_process_partial_response: mock.Mock):
        # --- Arrange ---
        record_count = 50
        rate_limit_calls, rate_limit_period = 50, 50
        mock_time.time.side_effect = [0, 60]
        event = {"Records": [None] * record_count}
        context = LambdaContext()
        expected_response = {"FailedBatchItems": 5}
        mock_process_partial_response.return_value = expected_response

        # --- Act, Assert ---
        with mock.patch(f"{FILE_PREFIX}.RATE_LIMIT_CALLS", rate_limit_calls):
            with mock.patch(f"{FILE_PREFIX}.RATE_LIMIT_PERIOD_SEC", rate_limit_period):
                response = lambda_handler(event, context)
        self.assertEqual(response, expected_response)
        mock_time.sleep.assert_not_called()
        mock_process_partial_response.assert_called_once_with(event=event, record_handler=record_handler,
                                                              context=context, processor=processor)
