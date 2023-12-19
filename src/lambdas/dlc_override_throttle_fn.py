import time
import boto3
import json
import os
import logging
from http import HTTPStatus
from typing import Any, Dict, Tuple, Optional
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from functools import lru_cache, partial
from msi_common import Stage
from ratelimiter import RateLimiter
from botocore.client import BaseClient
from src.config.config import AppConfig
from aws_lambda_powertools import Tracer
from cresconet_aws.support import (
    SupportMessage,
    send_message_to_support,
    alert_on_exception,
)
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    process_partial_response,
    EventType,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from src.lambdas.dlc_event_helper import assemble_event_payload
from src.statemachine.state_machine_handler import StateMachineHandler
from src.utils.kinesis_utils import deliver_to_kinesis
from src.utils.tracker_utils import (
    update_tracker,
    is_request_pending_state_machine,
    bulk_update_records,
)

# Environmental variables
KINESIS_DATA_STREAM_NAME: str = os.environ.get("KINESIS_DATA_STREAM_NAME")
LOG_LEVEL: str = os.environ.get("LOG_LEVEL", logging.INFO)
REGION: str = os.environ.get("REGION", "ap-south-1")
REQUEST_TRACKER_TABLE_NAME: str = os.environ.get("REQUEST_TRACKER_TABLE_NAME")
RATE_LIMIT_CALLS = int(os.environ.get("RATE_LIMIT_CALLS", 1000))
RATE_LIMIT_PERIOD_SEC = int(os.environ.get("RATE_LIMIT_PERIOD", 60))
DEFAULT_OVERRIDE_DURATION_MINUTES: int = int(
    os.environ.get("DEFAULT_OVERRIDE_DURATION_MINUTES", 30)
)

# Constants
LOAD_CONTROL_TRACER_NAME = "dlc"
LOAD_CONTROL_ALERT_SOURCE = "dlc-override-throttle-fn"
LOAD_CONTROL_ALERT_FORMAT = "Load Control Override has Failed - {hint}"

# Global variables
processor = BatchProcessor(event_type=EventType.SQS)
tracer = Tracer(service=LOAD_CONTROL_TRACER_NAME)

# Logging Setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(name="LoadControlFunctions")
logger.setLevel(LOG_LEVEL)


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)

        return json.JSONEncoder.default(self, obj)


@lru_cache(maxsize=1)
def get_step_function_client() -> BaseClient:
    """
    Retrieves a step function client for interaction with the step function AWS API.

    :returns: The step function client.
    """
    return boto3.client("stepfunctions", region_name=REGION)


# def report_error_to_client(
#     correlation_id: str,
#     message: str,
#     request_start_date: Optional[datetime] = None,
#     request_end_date: Optional[datetime] = None,
# ):
#     """
#     Updated the request tracker with a failure status and reports the failure to the client via the Kinesis stream.

#     :param correlation_id: The request correlation_id.
#     :param message: The error message for the failure.
#     :param request_start_date: An optional start date for the request.
#     :param request_end_date: An optional end date for the request.
#     """
#     error_datetime = datetime.now(timezone.utc)
#     update_tracker(
#         correlation_id=correlation_id,
#         stage=Stage.DECLINED,
#         event_datetime=error_datetime,
#         message=message,
#         request_start_date=request_start_date,
#         request_end_date=request_end_date,
#     )
#     payload = assemble_event_payload(
#         correlation_id, Stage.DECLINED, error_datetime, message
#     )
#     deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)


def report_error_to_client(records, message):
    """
    Updated the request tracker with a failure status and reports the failure to the client via the Kinesis stream.

    :param correlation_id: The request correlation_id.
    :param message: The error message for the failure.
    :param request_start_date: An optional start date for the request.
    :param request_end_date: An optional end date for the request.
    """
    error_datetime = datetime.now(timezone.utc)
    if "site_switch_crl_id" in records:
        bulk_update_records(records, Stage.DECLINED, error_datetime, message=message)
        return
    update_tracker(
        correlation_id=records["correlation_id"],
        stage=Stage.DECLINED,
        event_datetime=error_datetime,
        message=message,
        request_start_date=records["request_start_date"],
        request_end_date=records["request_end_date"],
    )
    payload = assemble_event_payload(
        records["correlation_id"], Stage.DECLINED, error_datetime, message
    )
    deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)


def report_error_to_support(correlation_id: str, reason: str, subject_hint: str = ""):
    """
    Reports an error to support for a given correlation_id.

    :param correlation_id: The correlation ID for the request that failed.
    :param reason: The reason why it failed.
    :param subject_hint: The subject hint that will appear in OpsGenie.
    """
    subject = LOAD_CONTROL_ALERT_FORMAT.format(hint=subject_hint)
    support_message = SupportMessage(
        reason=reason, subject=subject, tags=AppConfig.LOAD_CONTROL_TAGS
    )
    send_message_to_support(support_message, correlation_id=correlation_id)


def update_start_end_times_on_request(
    request: Dict[str, Any]
) -> Tuple[datetime, datetime]:
    """
    Mutates the start and end times on the request so that it matches the expected format
    and instantiates the values if they are missing.

    :param request: The request dictionary (mutable).
    :returns: (start datetime, end datetime) of the request in UTC.
    """
    start = datetime.now(tz=timezone.utc)
    if "start_datetime" in request:
        start = datetime.fromisoformat(request["start_datetime"])
        start = start.astimezone(tz=timezone.utc)
    request["start_datetime"] = start.isoformat(timespec="seconds")

    end = start + timedelta(minutes=DEFAULT_OVERRIDE_DURATION_MINUTES)
    if "end_datetime" in request:
        end = datetime.fromisoformat(request["end_datetime"])
        end = end.astimezone(tz=timezone.utc)
    request["end_datetime"] = end.isoformat(timespec="seconds")
    return start, end


def report_error(correlation_id, reason, subject_hint=None):
    if not isinstance(correlation_id, str):
        partial_report_error_to_support = partial(
            report_error_to_support, reason=reason, subject_hint=subject_hint
        )
        list(map(partial_report_error_to_support, correlation_id))
    else:
        report_error_to_support(
            correlation_id=correlation_id, reason=reason, subject_hint=subject_hint
        )


def update_status(response, request):
    start_date: datetime = response["startDate"]
    logger.info(
        "SM invoked for DLC request. ARN: %s, StartDateTime: %s",
        response["executionArn"],
        start_date,
    )

    request_start_date = datetime.fromisoformat(request["start_datetime"])
    request_end_date = datetime.fromisoformat(request["end_datetime"])
    if "site_switch_crl_id" in request:
        bulk_update_records(
            request,
            Stage.QUEUED,
            start_date,
            request_start_date=request_start_date,
            request_end_date=request_end_date,
            original_start_datetime=request_start_date,
        )

        return
    update_tracker(
        correlation_id=request["correlation_id"],
        stage=Stage.QUEUED,
        event_datetime=start_date,
        request_start_date=request_start_date,
        request_end_date=request_end_date,
        original_start_datetime=request_start_date,
    )
    payload = assemble_event_payload(
        request["correlation_id"], Stage.QUEUED, start_date
    )
    deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)


def initiate_step_function(correlation_id, request: Dict[str, Any]):
    """
    Initiates the step function that will process override DLC request.

    :param correlation_id: The correlation id for the request.
    :param start: The start of the DLC request.
    :param end: The end of the DLC request.
    :param request: The DLC request.
    """
    step_function_client = get_step_function_client()
    try:
        logger.info("Starting step function execution id: %s", correlation_id)
        sm_handler = StateMachineHandler(
            step_function_client, AppConfig.DLC_OVERRIDE_SM_ARN
        )
        step_function_id = correlation_id
        if not isinstance(correlation_id, str):
            step_function_id = "GRP-" + correlation_id[0]
        request["action"] = "groupLCRequests"
        response = sm_handler.initiate(
            step_function_id, json.dumps(request, cls=JSONEncoder)
        )
        status_code: int = response["ResponseMetadata"]["HTTPStatusCode"]

        if status_code == HTTPStatus.OK:
            update_status(response, request)
            return

        error_message = f"Launching DLC request resulted in {status_code} status code"
        report_error_to_client(request, message=error_message)
        report_error(
            correlation_id=correlation_id,
            reason="DLC request failed",
            subject_hint="Failed Request",
        )
    except step_function_client.exceptions.ExecutionAlreadyExists:
        logger.info(
            "SM already is already active for correlation_id: %s", correlation_id
        )
    except Exception as e:
        logger.exception(
            "Exception while attempting to initiate state machine: %s",
            repr(e),
            exc_info=e,
        )
        report_error_to_client(request, message=str(e))
        report_error(
            correlation_id=correlation_id,
            reason="DLC Request failed with internal error",
            subject_hint="Internal Error",
        )


def remove_processed_records(obj):
    if is_request_pending_state_machine(obj["correlation_id"]):
        return obj, obj["site"], obj["switch_addresses"], obj["correlation_id"]
    else:
        logger.info(
            "Request with matching correlation id: %s, has already been processed.",
            obj["correlation_id"],
        )


@RateLimiter(max_calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD_SEC)
def record_handler(record):
    """
    Process a single record. This function is also rate limited.
    The `@RateLimiter` is used to rate limit events that contain more records than the rate limit. In which case it
    will force a wait time between rate limited batches.
    e.g. if we have 1000 records, and we are limited to 500 per 30 seconds. This will enforce that the next batch
    won't be processed until the period is complete.
    libray: https://pypi.org/project/ratelimiter

    :param record: The SQS record.
    """
    if not isinstance(record["correlation_id"], str):
        (
            record["site_switch_crl_id"],
            record["site"],
            record["switch_addresses"],
            record["correlation_id"],
        ) = zip(*map(remove_processed_records, record["site_switch_crl_id"]))
        site_switch_crl_id = record["site_switch_crl_id"]
        if not site_switch_crl_id:
            return
    else:
        if not is_request_pending_state_machine(record["correlation_id"]):
            return

    request_start, request_end = update_start_end_times_on_request(record)
    if request_end <= datetime.now(tz=timezone.utc):
        logger.error(
            "Request with correlation_id '%s', has been throttled for too long",
            str(record["correlation_id"]),
        )
        report_error_to_client(
            record, message="Request is rejected as it has a end datetime in the past"
        )
        return

    logger.info(
        "Starting to process DLC request with correlation_id: %s",
        str(record["correlation_id"]),
    )
    initiate_step_function(correlation_id=record["correlation_id"], records=record)


def group_records(event):
    """
    This function accepts the individual requests from the sqs.
    Individual requests if group_id present, are then grouped based on group_id, status, start_datetime and end_datetime.
    Individual requests if no group_id will be considered separately.
    A list with the grouped requests and the individual requests will be returned.

    :param event: The sqs events.
    """
    data = [ast.literal_eval(i["body"]) for i in event["Records"]]
    nan_records = []

    grouped_records = {}

    for record in data:
        group_id = record.get("group_id")

        if group_id is None:  # Requests with no group_id
            nan_records.append(record)
        else:  # Requests with group_id
            key = (
                group_id,
                record.get("status"),
                record.get("start_datetime"),
                record.get("end_datetime"),
            )

            if key not in grouped_records:
                grouped_records[key] = {
                    "group_id": group_id,
                    "status": record.get("status"),
                    "start_datetime": record.get("start_datetime"),
                    "end_datetime": record.get("end_datetime"),
                    "switch_addresses": [],
                    "site": [],
                    "correlation_id": [],
                    "site_switch_crl_id": [],
                }

            switch_addresses = record.get("switch_addresses")
            site = record.get("site")
            correlation_id = record.get("correlation_id")
            grouped_records[key]["switch_addresses"].append(
                record.get("switch_addresses")
            )
            grouped_records[key]["site"].append(record.get("site"))
            grouped_records[key]["correlation_id"].append(record.get("correlation_id"))
            grouped_records[key]["site_switch_crl_id"].append(
                {
                    "site": site,
                    "switch_addresses": switch_addresses,
                    "correlation_id": correlation_id,
                    "sub_id": record.get("sub_id"),
                }
            )

    grouped_records_list = list(grouped_records.values())

    return grouped_records_list + nan_records


@alert_on_exception(
    tags=AppConfig.LOAD_CONTROL_TAGS, service_name=LOAD_CONTROL_ALERT_SOURCE
)
@tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: LambdaContext):
    """
    The lambda entry point.

    :param event: The raw event.
    :param context: The lambda context.
    """
    try:
        start_time = time.time()
        result = list(map(record_handler, group_records(event)))
        # result = process_partial_response(event=event, record_handler=record_handler, processor=processor,
        #                                   context=context)
        end_time = time.time()

        expected_runtime = int(
            round(len(event["Records"]) / RATE_LIMIT_CALLS * RATE_LIMIT_PERIOD_SEC)
        )
        processing_time = int(round(end_time - start_time))
        if processing_time < expected_runtime:
            time.sleep(RATE_LIMIT_PERIOD_SEC - processing_time)
        return result
    except Exception as e:
        logger.exception(
            "Failed to process events. Error: %s, Event: %s", repr(e), event, exc_info=e
        )
        raise e
