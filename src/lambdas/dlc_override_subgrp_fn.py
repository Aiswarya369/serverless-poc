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
from src.model.enums import Stage
from ratelimiter import RateLimiter
from botocore.client import BaseClient
from src.config.config import AppConfig
from aws_lambda_powertools import Tracer
import ast
import pandas as pd
from src.utils.tracker_utils import (
    new_get_contiguous_request,
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
from src.utils.tracker_utils import update_tracker, is_request_pending_state_machine
from src.utils.request_validator import RequestValidator

# Environmental variables
# KINESIS_DATA_STREAM_NAME: str = os.environ.get("KINESIS_DATA_STREAM_NAME")
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


def report_error_to_client(
    correlation_id: str,
    message: str,
    request_start_date: Optional[datetime] = None,
    request_end_date: Optional[datetime] = None,
):
    """
    Updated the request tracker with a failure status and reports the failure to the client via the Kinesis stream.

    :param correlation_id: The request correlation_id.
    :param message: The error message for the failure.
    :param request_start_date: An optional start date for the request.
    :param request_end_date: An optional end date for the request.
    """
    error_datetime = datetime.now(timezone.utc)
    update_tracker(
        correlation_id=correlation_id,
        stage=Stage.DECLINED,
        event_datetime=error_datetime,
        message=message,
        request_start_date=request_start_date,
        request_end_date=request_end_date,
    )
    # payload = assemble_event_payload(correlation_id, Stage.DECLINED, error_datetime, message)
    # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)


def report_error_to_support(correlation_id: str, reason: str, subject_hint: str = ""):
    """
    Reports an error to support for a given correlation_id.

    :param correlation_id: The correlation ID for the request that failed.
    :param reason: The reason why it failed.
    :param subject_hint: The subject hint that will appear in OpsGenie.
    """
    # subject = LOAD_CONTROL_ALERT_FORMAT.format(hint=subject_hint)
    # support_message = SupportMessage(reason=reason, subject=subject, tags=AppConfig.LOAD_CONTROL_TAGS)
    # send_message_to_support(support_message, correlation_id=correlation_id)


def reject_request(
    correlation_id,
    message: str,
    request_start_date: Optional[datetime] = None,
    request_end_date: Optional[datetime] = None,
):
    if correlation_id and (not isinstance(correlation_id, str)):
        partial_report_error_to_client = partial(
            report_error_to_client,
            message=message,
            request_start_date=request_start_date,
            request_end_date=request_end_date,
        )
        list(map(partial_report_error_to_client, correlation_id))
    else:
        report_error_to_client(
            correlation_id,
            message=message,
            request_start_date=request_start_date,
            request_end_date=request_end_date,
        )


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


def update_status(response, start, end, correlation_id):
    start_date: datetime = response["startDate"]
    logger.info(
        "SM invoked for DLC request. ARN: %s, StartDateTime: %s",
        response["executionArn"],
        start_date,
    )
    update_tracker(
        correlation_id=correlation_id,
        stage=Stage.QUEUED,
        event_datetime=start_date,
        request_start_date=start,
        request_end_date=end,
    )
    # payload = assemble_event_payload(correlation_id, Stage.QUEUED,
    #                                  start_date)
    # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)


def initiate_step_function(
    correlation_id: str, start: datetime, end: datetime, request: Dict[str, Any]
):
    """
    Process a single SQS record. This function is also rate limited.
    The `@RateLimiter` is used to rate limit events that contain more records than the rate limit. In which case it
    will force a wait time between rate limited batches.
    e.g. if we have 1000 records, and we are limited to 500 per 30 seconds. This will enforce that the next batch
    won't be processed until the period is complete.
    libray: https://pypi.org/project/ratelimiter

    :param record: The SQS record.
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

        response = sm_handler.initiate(
            step_function_id, json.dumps(request, cls=JSONEncoder)
        )
        status_code: int = response["ResponseMetadata"]["HTTPStatusCode"]

        if status_code == HTTPStatus.OK:
            if not isinstance(correlation_id, str):
                partial_update_status = partial(update_status, response, start, end)
                list(map(partial_update_status, correlation_id))
                return
            update_status(response, start, end, correlation_id)
            # payload = assemble_event_payload(correlation_id, Stage.QUEUED, start_date)
            # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)
            return

        error_message = f"Launching DLC request resulted in {status_code} status code"
        reject_request(
            correlation_id=correlation_id,
            message=error_message,
            request_start_date=start,
            request_end_date=end,
        )
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
        reject_request(correlation_id=correlation_id, message=str(e))
        report_error(
            correlation_id=correlation_id,
            reason="DLC Request failed with internal error",
            subject_hint="Internal Error",
        )


def record_handler(record):
    logger.info("Starting to process DLC request: %s", record["request"])
    correlation_id = record["request"]["correlation_id"]

    start = datetime.fromisoformat(record["request"]["start_datetime"])
    start = start.astimezone(tz=timezone.utc)
    record["request"]["start_datetime"] = start.isoformat(timespec="seconds")

    end = datetime.fromisoformat(record["request"]["end_datetime"])
    end = end.astimezone(tz=timezone.utc)
    record["request"]["end_datetime"] = end.isoformat(timespec="seconds")

    initiate_step_function(
        correlation_id=correlation_id,
        start=start,
        end=end,
        request=record["request"],
    )


def group_records(request):
    """
    Process records in event and get the all 'body'.
    Split the records that not have the group_id in 'body'.
    Group records that have group_id and add new field 'site_and_switch'.
    Finally, merge the both records and return.
    """

    logger.info("group input:\n%s", request)
    response = {}
    responses = []

    # Get request(s) that are previously contiguous to this one.
    contiguous_requests = new_get_contiguous_request(request)

    # Contiguous with the same switch direction
    for contiguous_request in contiguous_requests:
        # Contiguous with the same switch direction
        if request["status"] == contiguous_request["overrdValue"]:
            logger.info("Contiguous request: %s", contiguous_request)
            # logger.debug("Terminal request: %s", terminal_request)
            contiguous_request["flag"] = "cont-extend"

        # Contiguous with the opposite switch direction
        else:
            contiguous_request["flag"] = "cont-create"

        response["request"] = contiguous_request
        responses.append(response)
    # else:
    #     # No contiguous request - create a new stand-alone policy and deploy straight away.
    if request["switch_addresses"]:
        response["request"] = request
        responses.append(response)

    logger.info("group output:\n%s", responses)
    list(map(record_handler, responses))


# @alert_on_exception(tags=AppConfig.LOAD_CONTROL_TAGS, service_name=LOAD_CONTROL_ALERT_SOURCE)
# @tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: LambdaContext):
    """
    The lambda entry point.

    :param event: The raw event.
    :param context: The lambda context.
    """

    try:
        logger.info("Events from Sub Group SQS")
        logger.info(event)
        start_time = time.time()
        start_time = time.time()
        request = [ast.literal_eval(i["body"]) for i in event["Records"]]
        result = list(map(group_records, request))

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
