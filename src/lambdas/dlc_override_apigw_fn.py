import datetime
import json

import logging
import os
import uuid
import boto3
from http import HTTPStatus
from datetime import timedelta, datetime, timezone
from decimal import Decimal

from logging import Logger
from typing import Union, Dict, Any, List
from aws_lambda_powertools import Tracer
from botocore.client import BaseClient
from src.model.enums import Stage

# from cresconet_aws.support import alert_on_exception
from src.utils.aws_utils import send_sqs_message
from src.config.config import AppConfig
from src.lambdas.dlc_event_helper import assemble_error_message, assemble_event_payload
from src.utils.kinesis_utils import deliver_to_kinesis
from src.utils.request_validator import RequestValidator, ValidationError
from src.utils.tracker_utils import create_tracker, update_tracker

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: Logger = logging.getLogger(name=__name__)
# Environment lookup; if null, set to INFO level.
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

# Environmental variables.
REGION: str = os.environ.get("REGION", "ap-south-1")
# KINESIS_DATA_STREAM_NAME: str = os.environ.get("KINESIS_DATA_STREAM_NAME", "msi-dev-subscriptions-kds")
DEFAULT_OVERRIDE_DURATION_MINUTES: int = int(
    os.environ.get("DEFAULT_OVERRIDE_DURATION_MINUTES", 30)
)
OVERRIDE_THROTTLING_QUEUE = os.environ.get(
    "OVERRIDE_THROTTLING_QUEUE", "load-control-throttle-queue"
)

# X-ray tracer.
tracer: Tracer = Tracer(service="dlc")

# Boto3 resources.
STEP_FUNCTION_CLIENT: BaseClient = boto3.client("stepfunctions", region_name=REGION)

# Constants.
LOAD_CONTROL_SERVICE_NAME = "load_control"
LOAD_CONTROL_ALERT_SOURCE = "dlc-override-apigw-fn"
LOAD_CONTROL_ALERT_FORMAT = "Load Control Override has Failed - {hint}"
UTC_OFFSET: int = 10
INSTANCE_DATE_FORMAT: str = "%Y-%m-%dT%H%M%S"


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)

        return json.JSONEncoder.default(self, obj)


def format_response(
    status_code: HTTPStatus, body: Union[Dict[str, Any], str]
) -> Dict[str, Any]:
    """
    Formats a response object/string to a valid HTTP response.

    :param status_code: The response status code.
    :param body: The response object/string.
    :returns: Formatted HTTP response.
    """
    body_text: str = json.dumps(body) if isinstance(body, dict) else body

    return {"statusCode": status_code.value, "body": body_text}


def add_request_on_throttle_queue(
    correlation_id: str, request: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Adds a request to the throttling queue.

    :param correlation_id: The request correlation ID.
    :param request: The request that is being sent onto the queue.
    :returns: The request response.
    """
    try:
        response = send_sqs_message(OVERRIDE_THROTTLING_QUEUE, request)
        status_code: int = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == HTTPStatus.OK:
            logger.info("Successfully queued DLC request on throttling queue.")
            return format_response(
                HTTPStatus.OK,
                {"message": "DLC request accepted", "correlation_id": correlation_id},
            )

        error_message = f"Sending DLC request to throttling queue resulted in {status_code} status code"
        report_errors(correlation_id, datetime.now(tz=timezone.utc), error_message)
        # subject = LOAD_CONTROL_ALERT_FORMAT.format(hint="Failed Request")
        # support_message = SupportMessage(reason=error_message, subject=subject, tags=AppConfig.LOAD_CONTROL_TAGS)
        # send_message_to_support(support_message, correlation_id=correlation_id)
        return format_response(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            {
                "message": "DLC request failed",
                "correlation_id": correlation_id,
                "error": error_message,
            },
        )
    except Exception as e:
        # logger.exception("Exception while attempting to place request on throttle queue. %s", repr(e))
        logger.exception(
            "Exception while attempting to place request on throttle queue. %s", repr(e)
        )
        reason = "DLC Request failed with internal error"
        report_errors(correlation_id, datetime.now(tz=timezone.utc), str(e))
        # subject = LOAD_CONTROL_ALERT_FORMAT.format(hint="Internal Error")
        # support_message = SupportMessage(reason=reason, subject=subject, tags=AppConfig.LOAD_CONTROL_TAGS)
        # send_message_to_support(support_message, correlation_id=correlation_id)
        return format_response(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            {"message": reason, "correlation_id": correlation_id, "error": str(e)},
        )


def report_errors(
    correlation_id: str,
    error_datetime: datetime,
    error_message: Union[str, List[ValidationError]],
):
    """
    Reports errors back to the customer and updates the request tracker.

    :param correlation_id: The correlation ID of the request.
    :param error_datetime: The time of failure.
    :param error_message: A list of validation errors to report or a single error message.
    """
    message = error_message
    if isinstance(error_message, list):
        message = assemble_error_message(error_message)

    # Update request tracker.
    update_tracker(
        correlation_id=correlation_id,
        stage=Stage.DECLINED,
        event_datetime=error_datetime,
        message=message,
    )

    # Create event and send to Kinesis.
    # payload = assemble_event_payload(correlation_id, Stage.DECLINED, error_datetime, message)
    # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)


def job_entry(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processes the DLC override request and ensures it is valid before placing it on the
    override throttling queue.

    :param event: The HTTP event.
    :returns: The HTTP response.
    """
    if "body" not in event:
        raise RuntimeError("Body field not found in the request")

    # Get stuff from incoming request.
    request: dict = json.loads(event["body"])
    subscription_id: str = event["pathParameters"]["subscription_id"]  # Always exists.

    # Validate the request.
    errors = RequestValidator.validate_dlc_override_request(
        request, DEFAULT_OVERRIDE_DURATION_MINUTES
    )
    if errors:
        # Validation errors - decline the request immediately (before we create a correlation id and store
        # it in the tracker) as we cannot proceed any further with it.
        logger.debug("Request errors: %s", errors)

        error_details: dict = {
            "correlation_id": None,
            "message": f"Invalid request: found {len(errors)} error(s)",
            "errorDetails": [e.error for e in errors],
        }

        return format_response(HTTPStatus.BAD_REQUEST, error_details)

    # We have a valid request.
    #
    # Before we do anything else, we need to create our correlation id and store it in our request.
    now: datetime = datetime.now(tz=timezone.utc)
    site: str = request["site"]
    correlation_id = create_correlation_id(site, now)
    request["correlation_id"] = correlation_id

    # Get additional details from the request now that it has passed validation.
    override_status: str = request["status"]

    # There should only be one meter serial supplied in "switch_addresses", as per validation.
    # There should only be one meter serial supplied in "switch_addresses", as per validation.
    # switch_addresses: Union[str, List[str]] = request["switch_addresses"]
    # meter_serial_number: str = (
    #     switch_addresses[0] if type(switch_addresses) == list else switch_addresses
    # )

    switch_addresses: Union[str, List[str]] = request["switch_addresses"]
    meter_serial_number: str = (
        switch_addresses[0] if type(switch_addresses) == list else switch_addresses
    )
    request["switch_addresses"] = meter_serial_number

    # Create initial request tracker records.
    # We can't add the request start and end dates to the tracker header record here because we need to validate
    # them first prior to parsing to datetime objects, which we do below.
    # They will be added when we initiate the Step Function.
    # Override status can be added, even it is incorrect (the validation will identify if so).
    create_tracker(
        correlation_id=correlation_id,
        sub_id=subscription_id,
        request_site=site,
        serial_no=meter_serial_number,
        override=override_status,
        original_start_datetime=request["start_datetime"],
    )

    # Validate the subscription.
    _, subscription_errors = RequestValidator.validate_subscription(
        subscription_id, LOAD_CONTROL_SERVICE_NAME
    )
    if subscription_errors:
        logger.debug("Subscription errors: %s", subscription_errors)
        report_errors(correlation_id, now, subscription_errors)
        error_details = {
            "correlation_id": correlation_id,
            "message": f"Invalid request: found {len(subscription_errors)} subscription error(s)",
            "errorDetails": [e.error for e in subscription_errors],
        }

        return format_response(HTTPStatus.BAD_REQUEST, error_details)

    # Validate whether the request either overlaps or is a duplicate.
    # This is implemented as a separate function, rather than in validate_dlc_override_request(), because that
    # function gets called twice: once here and again when the Step Function deploys the policy.
    # In order to minimise calls on the tracker database, we just check once for overlaps (i.e. here).
    duration_errors = RequestValidator.validate_request_duration(
        request, DEFAULT_OVERRIDE_DURATION_MINUTES
    )
    if duration_errors:
        # logger.debug("Duration errors: %s", duration_errors)
        report_errors(correlation_id, now, duration_errors)
        error_details = {
            "correlation_id": correlation_id,
            "message": f"Invalid request: found {len(duration_errors)} error(s)",
            "errorDetails": [e.error for e in duration_errors],
        }

        return format_response(HTTPStatus.BAD_REQUEST, error_details)

    logger.info("Request payload for state machine: %s", request)
    return add_request_on_throttle_queue(correlation_id, request)


def create_correlation_id(site: str, now: datetime) -> str:
    """
    Create correlation id for use throughout this process.

    :param site: the request's site to use.
    :param now: the current date/time in UTC.
    :return:
    """
    dt: str = (now + timedelta(hours=UTC_OFFSET)).strftime(INSTANCE_DATE_FORMAT)
    correlation_id: str = f"{site}-{dt}-{str(uuid.uuid4())}"
    logger.info("Correlation id: %s", correlation_id)

    return correlation_id


# @alert_on_exception(tags=AppConfig.LOAD_CONTROL_TAGS, service_name=LOAD_CONTROL_ALERT_SOURCE)
# @tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], _context: Any):
    """
    Lambda entry point for DLC API requests.

    :param event: The HTTP request from the API gateway.
    :param _context: The lambda context, which is ignored.
    :returns: A http response.
    """
    try:
        logger.info("Lambda to perform direct Load Control API override being run now")
        logger.info(f"Event: %s", str(event))
        return job_entry(event)
    except Exception as e:
        logger.exception(
            "Failed to process DLC request event. Event: %s, Error: %s",
            str(event),
            repr(e),
            exc_info=e,
        )
        # todo
        # Currently the API gateway handles and returns a InternalServerError on a exception. And the support topic
        # gets alerted through the decorator. But might be worth seeing if a 'valid' http response is necessary in
        # order to provide more detail.
        raise e


if __name__ == "__main__":
    data: dict = {
        "pathParameters": {"subscription_id": "a25fb76c298447e3996281098c59ed00"},
        "body": '{"site": "3120309956", "switch_addresses": "LG022102555", "status": "ON"}',
    }

    result: dict = job_entry(data)
    logger.info(result)
