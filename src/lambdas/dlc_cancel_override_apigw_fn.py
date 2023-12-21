import json
import logging
import os
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Union, Optional
import boto3 as boto3
from botocore.client import BaseClient
from botocore.errorfactory import ClientError
from msi_common import Stage
from src.config.config import AppConfig
from src.statemachine.state_machine_handler import StateMachineHandler
from src.utils.request_validator import RequestValidator
from src.utils.tracker_utils import get_header_record
from cresconet_aws.support import send_message_to_support, SupportMessage, alert_on_exception

# -----------------------
# Environmental variables
# -----------------------
REGION = os.environ.get("REGION", "ap-southeast-2")
LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

# ---------
# Constants
# ---------
IN_PROGRESS_STAGES = [Stage.RECEIVED.value, Stage.QUEUED.value, Stage.POLICY_CREATED.value,
                      Stage.POLICY_DEPLOYED.value, Stage.POLICY_EXTENDED.value,
                      Stage.EXTENDED_BY.value, Stage.EXTENDS.value, Stage.DLC_OVERRIDE_STARTED.value]
STEP_FUNCTION_IN_PROGRESS_STAGES = [Stage.QUEUED.value, Stage.POLICY_CREATED.value]
LOAD_CONTROL_ALERT_SOURCE = "dlc-cancel-override-apigw-fn"
CANCELLATION_EXECUTION_ID_DATETIME_FORMAT = "%Y%m%d%H%M%S"
LOAD_CONTROL_SERVICE_NAME = "load_control"
CANCEL_OVERRIDE_ALERT_SUBJECT_FORMAT = "Load Control Cancel Override has Failed - {hint}"

# ----------------
# Global variables
# ----------------
STEP_FUNCTION_CLIENT: Optional[BaseClient] = None

# -------
# Logging
# -------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(name="CancelDirectLoadControlRequest")
logger.setLevel(LOG_LEVEL)


def init_step_function_client():
    """
    Create a step function client for STEP_FUNCTION_CLIENT if one does not exist
    """
    global STEP_FUNCTION_CLIENT

    if not STEP_FUNCTION_CLIENT:
        STEP_FUNCTION_CLIENT = boto3.client('stepfunctions', region_name=REGION)


class InvalidRequest(Exception):
    """
    Exception class for scenarios where the request is invalid
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


def initiate_step_function(correlation_id: str, request: dict) -> dict:
    """
    Initiate AWS step function and formulate an HTTP response to send back to the caller.

    :param correlation_id:
    :param request:
    :return:
    """
    timestamp: str = datetime.now(timezone.utc).strftime(CANCELLATION_EXECUTION_ID_DATETIME_FORMAT)
    step_function_execution_id: str = f"{correlation_id}-{timestamp}"

    sm_handler = StateMachineHandler(STEP_FUNCTION_CLIENT, AppConfig.DLC_CANCEL_OVERRIDE_SM_ARN)
    response = sm_handler.initiate(step_function_execution_id, json.dumps(request))

    # Ensure the response indicates everything was OK.
    status_code: int = response["ResponseMetadata"]["HTTPStatusCode"]

    if HTTPStatus.OK == status_code:
        # All good.
        start_date: datetime = response["startDate"]

        logger.info("Invoked SM to process DLC request. ARN: %s, StartDateTime: %s", response["executionArn"],
                    start_date)

        return format_response(status_code, {
            "message": "DLC cancel request in progress",
            "correlation_id": correlation_id
        })
    else:
        raise RuntimeError(f"Launching DLC cancel request step function resulted in {status_code} status code")


def format_response(status_code: int, response: Union[dict, str]) -> dict:
    """
    Format HTTP response.

    :param status_code:
    :param response:
    :return:
    """
    body: str = json.dumps(response) if isinstance(response, dict) else response

    response = {
        "statusCode": status_code,
        "body": body
    }
    logger.info("Response: %s", response)
    return response


def process_request(event: dict) -> dict:
    """
    Process request.

    :param event: cancel override event
    :return: api gateway response
    """
    # The subscription + correlation id will always exist as this request has come via API Gateway.
    provided_subscription_id: str = event["pathParameters"]["subscription_id"]

    correlation_id: str = event["queryStringParameters"]["correlation_id"]
    subscriber: str = event["queryStringParameters"]["subscriber"]

    logger.info("Subscription id: %s, Correlation id: %s, Subscriber: %s", provided_subscription_id, correlation_id,
                subscriber)

    try:
        # Validate subscription active & LC
        subscription_record, subscription_errors = RequestValidator.validate_subscription(provided_subscription_id,
                                                                                          LOAD_CONTROL_SERVICE_NAME)

        if subscription_errors:
            raise InvalidRequest(f"Subscription id {provided_subscription_id} is not valid")

        # Validate subscription for subscriber
        if subscription_record['subName'] != subscriber:
            raise InvalidRequest(
                f"Given subscriber {subscriber} does not own the subscription {provided_subscription_id}")

        # Validate correlation ID exists
        header_record: dict = get_header_record(correlation_id)
        if not header_record:
            raise InvalidRequest(f"Correlation id {correlation_id} not found")
        
        if header_record.get('group_id', None):
            raise InvalidRequest(f"Correlation id {correlation_id} is a part of group dispatch and cannot be canceled")

        request_subscription_id = header_record["subId"]
        if provided_subscription_id != request_subscription_id:
            raise InvalidRequest(
                f"Subscription id {provided_subscription_id} does not match the subscription id of the "
                f"override request to cancel")

        # Validate request is in progress
        current_stage: str = header_record.get("currentStg")
        if current_stage not in IN_PROGRESS_STAGES:
            raise InvalidRequest(f"Load control request in state: {current_stage} - cannot cancel from this state")

        # Get the current datetime in UTC without microseconds to match what is stored in the tracker table
        now: datetime = datetime.now(timezone.utc).replace(microsecond=0)

        # Validate request has not already finished
        if datetime.fromisoformat(header_record.get('rqstEndDt')) < now:
            raise InvalidRequest("Request given has an end date in the past so is already completed")

        policy_id = header_record.get('plcyId')
        if policy_id:
            policy_id = int(policy_id)

        # Package request for the step function
        request = {
            "correlation_id": correlation_id,
            "site": header_record.get("site"),
            "switch_addresses": [header_record.get("mtrSrlNo")],
            "status": header_record.get("overrdValue"),
            "meter_serial_number": header_record.get("mtrSrlNo"),
            "current_stage": header_record.get("currentStg"),
            "start_datetime": header_record.get('rqstStrtDt'),
            "end_datetime": header_record.get('rqstEndDt'),
            "policy_id": policy_id,
            "extended_by": header_record.get('extnddBy'),
            "subscription_id": request_subscription_id
        }
        return initiate_step_function(correlation_id, request)

    except InvalidRequest as e:
        logger.exception(str(e))
        return format_response(HTTPStatus.BAD_REQUEST, {
            "message": str(e),
            "correlation_id": correlation_id
        })
    except ClientError as e:
        logger.exception(str(e))

        # Unable to import exception class botocore.errorfactory.ExecutionDoesNotExist: when an exception
        # is created by botocore.error_factory then it is not possible to directly import it.
        if "ExecutionDoesNotExist" == e.__class__.__name__:
            # Cannot find a workflow for the supplied request id.
            subject = CANCEL_OVERRIDE_ALERT_SUBJECT_FORMAT.format(hint="Correlation ID Not Found")
            reason = f"Correlation id {correlation_id} not found"
        else:
            subject = CANCEL_OVERRIDE_ALERT_SUBJECT_FORMAT.format(hint="Client Internal Error")
            reason = "Direct load control cancel request failed with internal error"

        support_message = SupportMessage(reason=reason, subject=subject, stack_trace=repr(e),
                                         tags=AppConfig.LOAD_CONTROL_TAGS)
        send_message_to_support(support_message, correlation_id=correlation_id)
        return format_response(HTTPStatus.INTERNAL_SERVER_ERROR, {
            "message": reason,
            "correlation_id": correlation_id
        })
    except Exception as e:
        logger.exception(str(e))
        subject = CANCEL_OVERRIDE_ALERT_SUBJECT_FORMAT.format(hint="Internal Error")
        reason = "Direct load control cancel request failed with internal error"
        support_message = SupportMessage(reason=reason, subject=subject, stack_trace=repr(e),
                                         tags=AppConfig.LOAD_CONTROL_TAGS)
        send_message_to_support(support_message, correlation_id=correlation_id)
        return format_response(HTTPStatus.INTERNAL_SERVER_ERROR, {
            "message": reason,
            "correlation_id": correlation_id
        })


@alert_on_exception(tags=AppConfig.LOAD_CONTROL_TAGS, service_name=LOAD_CONTROL_ALERT_SOURCE)
def lambda_handler(event: dict, _):
    """
    Main processing.

    :param event:
    :param _:
    :return:
    """
    logger.info("Running lambda to cancel direct load control override")
    logger.info("Event:\n%s", str(event))
    init_step_function_client()
    return process_request(event)


if __name__ == "__main__":
    data: dict = {
        "pathParameters": {
            "subscription_id": "2d45fe05f3e44be38673b9ffd11fa2db"
        },
        "queryStringParameters": {
            "correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
            "subscriber": "ORIGIN"
        }
    }

    # IN_PROGRESS_STAGES.append(Stage.CANCELLED.value)  # added for dev test purposes
    result: dict = process_request(data)
