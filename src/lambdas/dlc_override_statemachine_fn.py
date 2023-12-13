import datetime
import logging
import os
from datetime import datetime, timezone, timedelta
from logging import Logger
from typing import Optional
import uuid
import boto3
from functools import partial

# import cresconet_aws.secrets_manager as cn_secret_manager
from aws_lambda_powertools import Tracer
from botocore.client import BaseClient

# from cresconet_aws.support import alert_on_exception
from src.model.enums import Stage
from src.utils.client import (
    create_lc_override_schedule_policy,
    deploy_policy,
    replace_lc_override_schedule_policy,
)

from src.config.config import AppConfig
from src.lambdas.dlc_event_helper import assemble_error_message, assemble_event_payload
from src.model.sm_actions import SupportedOverrideSMActions
from src.utils.kinesis_utils import deliver_to_kinesis
from src.utils.request_validator import RequestValidator
from src.utils.tracker_utils import (
    update_tracker,
    get_contiguous_request,
    new_get_contiguous_request,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: Logger = logging.getLogger(name="LoadControlFunctions")

# Environment lookup; if null, set to INFO level.
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

# Environmental variables.
REGION: str = os.environ.get("REGION", "ap-south-1")
KINESIS_DATA_STREAM_NAME: str = os.environ.get(
    "KINESIS_DATA_STREAM_NAME", "msi-dev-subscriptions-kds"
)
DEFAULT_OVERRIDE_DURATION_MINUTES: int = int(
    os.environ.get("DEFAULT_OVERRIDE_DURATION_MINUTES", 30)
)
CONTIGUOUS_START_BUFFER_MINUTES: int = int(
    os.environ.get("CONTIGUOUS_START_BUFFER_MINUTES", 5)
)
OPPOSITE_SWITCH_DIRECTION_BACKOFF = int(
    os.environ.get("OPPOSITE_SWITCH_DIRECTION_BACKOFF", 5)
)
PNET_SESSION_TABLE: str = os.environ.get("PNET_SESSION_TABLE")
PNET_SESSION_LIFETIME_SECONDS: int = int(
    os.environ.get("PNET_SESSION_LIFETIME_SECONDS", 300)
)

# Constants.
HTTP_SUCCESS: int = 200
HTTP_BAD_REQUEST: int = 400
LOAD_CONTROL_ALERT_SOURCE = "dlc-override-statemachine-fn"
POLICY_DEPLOY_DATETIME_FORMAT: str = "%Y-%m-%dT%H:%M:%SZ"

tracer: Tracer = Tracer(service="dlc")

# Boto3 resource for DynamoDB.
DYNAMO_RESOURCE: BaseClient = boto3.resource("dynamodb", region_name=REGION)

# PolicyNet client as a global variable.
# This avoids creating it every time the lambda is being called as it takes time to load up the WSDL.
# PolicyNetClient = None


def report_errors(correlation_id, errors) -> dict:
    """
    Report validation errors.

    :param correlation_id:
    :param errors:
    :return:
    """
    now: datetime = datetime.now(timezone.utc)

    # Update tracker.
    message: str = assemble_error_message(errors)
    update_tracker(
        correlation_id=correlation_id,
        stage=Stage.DECLINED,
        event_datetime=now,
        message=message,
    )

    # Create event.
    # payload: dict = assemble_event_payload(correlation_id, Stage.DECLINED, now, message)
    # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)

    return {
        "statusCode": HTTP_BAD_REQUEST,
        "message": "Invalid request",
        "errorDetails": [e.error for e in errors],
    }


def create_policy_update_tracker(
    data=None, event_datetime=None, response=None, policy_name=None, request=None
):
    status_code: int = response["statusCode"]
    message: str = response["message"]
    policy_id: int = response["policyID"]
    now: datetime = event_datetime
    correlation_id = data["correlation_id"]
    # start_datetime = datetime.fromisoformat(request['start_datetime'])
    # end_datetime = datetime.fromisoformat(request['end_datetime'])
    # start_datetime = request['start_datetime']
    # end_datetime = request['end_datetime']

    if status_code == HTTP_SUCCESS:
        # Policy id may not have been returned in a non-success scenario.

        # Update tracker.
        update_tracker(
            correlation_id=correlation_id,
            stage=Stage.POLICY_CREATED,
            event_datetime=now,
            message=message,
            policy_name=policy_name,
            policy_id=policy_id,
            # request_start_date=start_datetime,
            # request_end_date=end_datetime
        )
    else:
        # Update tracker.
        update_tracker(
            correlation_id=correlation_id,
            stage=Stage.DECLINED,
            event_datetime=now,
            message=message,
            policy_name=policy_name,
            # request_start_date=start_datetime,
            # request_end_date=end_datetime
        )

        # Create event.
        # payload: dict = assemble_event_payload(correlation_id, Stage.DECLINED,
        #                                       now, message)
        # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)


def extend_policy_update_tracker(
    data=None, event_datetime=None, response=None, policy_name=None, request=None
):
    status_code: int = response["statusCode"]
    message: str = response["message"]
    policy_id: int = response["policyID"]
    now: datetime = event_datetime
    correlation_id = data["correlation_id"]
    start_datetime = datetime.fromisoformat(request["start_datetime"])
    end_datetime = datetime.fromisoformat(request["end_datetime"])
    # new_start = request['start_datetime']
    # new_end = request['end_datetime']
    if status_code == HTTP_SUCCESS:
        # Update tracker with the following details:
        #
        # request 1: EXTENDED_BY 2 (also send to Kinesis)
        # request 2: EXTENDS 1 (also send to Kinesis)
        # request 2: Existing (previous) policy has been successfully extended in PolicyNet.
        #
        # 1. Update the contiguous request to indicate it has been extended.
        contiguous_correlation_id: str = data["crrltnId"]
        extend_message: str = f"Request {contiguous_correlation_id} has been extended by request {correlation_id}"

        update_tracker(
            correlation_id=contiguous_correlation_id,
            stage=Stage.EXTENDED_BY,
            event_datetime=now,
            message=extend_message,
            # request_start_date=request['rqstStrtDt'],
            # request_end_date=new_start,
            extended_by=data["correlation_id"],
        )

        # Send to Kinesis.
        # payload: dict = assemble_event_payload(contiguous_correlation_id, Stage.EXTENDED_BY, now, extend_message)
        # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)

        # 2. Update the new request to indicate that it is extending a previous request.
        extend_message = (
            f"Request {correlation_id} extends request {contiguous_correlation_id}"
        )

        update_tracker(
            correlation_id=correlation_id,
            stage=Stage.EXTENDS,
            event_datetime=now,
            message=extend_message,
            request_start_date=start_datetime,
            request_end_date=end_datetime,
            extends=contiguous_correlation_id,
            original_start_datetime=request["original_start_datetime"],
        )

        # Send to Kinesis.
        # payload: dict = assemble_event_payload(correlation_id, Stage.EXTENDS, now, extend_message)
        # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)

        # 3. Update the new request to say the extension has been successfully applied to PolicyNet.
        update_tracker(
            correlation_id=correlation_id,
            stage=Stage.POLICY_EXTENDED,
            event_datetime=now,
            message=message,
            policy_name=policy_name,
            policy_id=policy_id,
        )
    else:
        # Update tracker.
        update_tracker(
            correlation_id=correlation_id,
            stage=Stage.DECLINED,
            event_datetime=now,
            message=message,
            policy_name=policy_name,
        )

        # Send to Kinesis.
        # payload: dict = assemble_event_payload(correlation_id, Stage.DECLINED, now, message)
        # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)


def create_policy(request: dict):
    """
    Create policy in PolicyNet.

    :param correlation_id:
    :param request:
    :return:
    """
    logger.info("Creating policy in PolicyNet")

    meter_serial: str = request["switch_addresses"]
    # New request's start and end dates will be added as default values when we initiated the Step Function execution
    # if they were not explicitly provided.
    start_datetime = datetime.fromisoformat(request["start_datetime"])
    end_datetime = datetime.fromisoformat(request["end_datetime"])
    duration = int((end_datetime - start_datetime).total_seconds() / 60)
    logger.debug("Period is from %s for %s minutes", start_datetime, duration)
    turn_off: bool = False if request["status"] == "ON" else True
    replace: bool = request["replace"]

    # Create policy.
    # The response from PolicyNet doesn't include anything useful, so we'll have to be "as near as"
    # with event datetimes.
    now: datetime = datetime.now(timezone.utc)

    policy_name, response = create_lc_override_schedule_policy(
        meter_serials=meter_serial,
        start_datetime=start_datetime,
        turn_off=turn_off,
        duration=duration,
        replace=replace,
    )

    status_code: int = response["statusCode"]
    message: str = response["message"]

    if "site_switch_crl_id" not in request:
        request["site_switch_crl_id"] = [{"correlation_id": request["correlation_id"]}]
    partial_create_policy_update_tracker = partial(
        create_policy_update_tracker,
        event_datetime=now,
        response=response,
        policy_name=policy_name,
        request=request,
    )
    list(map(partial_create_policy_update_tracker, request["site_switch_crl_id"]))
    logger.info("Create policy response")
    logger.info(response)

    # if status_code == HTTP_SUCCESS:
    #     # Policy id may not have been returned in a non-success scenario.
    #     policy_id: int = response["policyID"]

    #     # Update tracker.
    #     update_tracker(
    #         correlation_id=correlation_id,
    #         stage=Stage.POLICY_CREATED,
    #         event_datetime=now,
    #         message=message,
    #         policy_name=policy_name,
    #         policy_id=policy_id,
    #         request_start_date=start_datetime,
    #         request_end_date=end_datetime,
    #     )
    # else:
    #     # Update tracker.
    #     update_tracker(
    #         correlation_id=correlation_id,
    #         stage=Stage.DECLINED,
    #         event_datetime=now,
    #         message=message,
    #         policy_name=policy_name,
    #         request_start_date=start_datetime,
    #         request_end_date=end_datetime,
    #     )

    #     # Create event.
    #     # payload: dict = assemble_event_payload(correlation_id, Stage.DECLINED, now, message)
    #     # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)

    return response


def extend_policy(request):
    """
    Extend policy in PolicyNet.

    :param correlation_id:
    :param request:
    :param contiguous_request:
    :param terminal_request:
    :return:
    """
    logger.info("Extending existing policy in PolicyNet")

    # terminal_start: datetime = datetime.fromisoformat(request["rqstStrtDt"])
    # terminal_end: datetime = datetime.fromisoformat(request["original_start_datetime"])
    # contiguous_start: datetime = datetime.fromisoformat(
    #     contiguous_request["rqstStrtDt"]
    # )
    # contiguous_end: datetime = datetime.fromisoformat(contiguous_request["rqstEndDt"])

    terminal_start: datetime = datetime.fromisoformat(request["rqstStrtDt"])
    terminal_end: datetime = datetime.fromisoformat(request["original_start_datetime"])

    # New request's start and end dates will be added as default values when we initiated the Step Function execution
    # if they were not explicitly provided.
    new_start: datetime = datetime.fromisoformat(request["start_datetime"])
    new_end = datetime.fromisoformat(request["end_datetime"])

    # Use the new request's end date and existing request's start date
    # to derive an extended period for the existing request.
    duration = int((new_end - terminal_start).total_seconds() / 60)

    # logger.debug("Terminal request was from %s to %s", terminal_start, terminal_end)
    # logger.debug(
    #     "Contiguous request was from %s to %s", contiguous_start, contiguous_end
    # )
    # logger.debug("New request is from %s to %s", new_start, new_end)
    # logger.debug(
    #     "Extended policy will be from %s for %s minutes", terminal_start, duration
    # )

    turn_off: bool = False if request["status"] == "ON" else True

    # Extend existing policy.
    # The response from PolicyNet doesn't include anything useful, so we'll have to be "as near as"
    # with event datetimes.
    now: datetime = datetime.now(timezone.utc)

    policy_name, response = replace_lc_override_schedule_policy(
        meter_serials=request["switch_addresses"],
        start_datetime=terminal_start,
        turn_off=turn_off,
        duration=duration,
    )

    status_code: int = response["statusCode"]
    message: str = response["message"]
    policy_id: int = response["policyID"]

    if "site_switch_crl_id" not in request:
        request["site_switch_crl_id"] = [
            {
                "correlation_id": request["correlation_id"],
                "crrltnId": request["crrltnId"],
            }
        ]
    partial_extend_policy_update_tracker = partial(
        extend_policy_update_tracker,
        event_datetime=now,
        response=response,
        policy_name=policy_name,
        request=request,
    )
    list(map(partial_extend_policy_update_tracker, request["site_switch_crl_id"]))

    return response

    # if status_code == HTTP_SUCCESS:
    #     # Update tracker with the following details:
    #     #
    #     # request 1: EXTENDED_BY 2 (also send to Kinesis)
    #     # request 2: EXTENDS 1 (also send to Kinesis)
    #     # request 2: Existing (previous) policy has been successfully extended in PolicyNet.
    #     #
    #     # 1. Update the contiguous request to indicate it has been extended.
    #     contiguous_correlation_id: str = contiguous_request["crrltnId"]
    #     extend_message: str = f"Request {contiguous_correlation_id} has been extended by request {correlation_id}"

    #     update_tracker(
    #         correlation_id=contiguous_correlation_id,
    #         stage=Stage.EXTENDED_BY,
    #         event_datetime=now,
    #         message=extend_message,
    #         request_start_date=contiguous_start,
    #         request_end_date=contiguous_end,
    #         extended_by=correlation_id,
    #     )

    #     # Send to Kinesis.
    #     # payload: dict = assemble_event_payload(contiguous_correlation_id, Stage.EXTENDED_BY, now, extend_message)
    #     # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)

    #     # 2. Update the new request to indicate that it is extending a previous request.
    #     extend_message = (
    #         f"Request {correlation_id} extends request {contiguous_correlation_id}"
    #     )

    #     update_tracker(
    #         correlation_id=correlation_id,
    #         stage=Stage.EXTENDS,
    #         event_datetime=now,
    #         message=extend_message,
    #         request_start_date=new_start,
    #         request_end_date=new_end,
    #         extends=contiguous_correlation_id,
    #     )

    #     # Send to Kinesis.
    #     # payload: dict = assemble_event_payload(correlation_id, Stage.EXTENDS, now, extend_message)
    #     # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)

    #     # 3. Update the new request to say the extension has been successfully applied to PolicyNet.
    #     update_tracker(
    #         correlation_id=correlation_id,
    #         stage=Stage.POLICY_EXTENDED,
    #         event_datetime=now,
    #         message=message,
    #         policy_name=policy_name,
    #         policy_id=policy_id,
    #     )
    # else:
    #     # Update tracker.
    #     update_tracker(
    #         correlation_id=correlation_id,
    #         stage=Stage.DECLINED,
    #         event_datetime=now,
    #         message=message,
    #         policy_name=policy_name,
    #     )

    #     # Send to Kinesis.
    #     # payload: dict = assemble_event_payload(correlation_id, Stage.DECLINED, now, message)
    #     # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)

    return response


def is_being_enforced(start: datetime, end: datetime, now: datetime):
    """
    Identify whether 'now' is considered to be in the enforcement period - true if "now" between the start and end.

    :param start:
    :param end:
    :param now:
    :return:
    """
    return True if start <= now < end else False


def handle_create_policy(request: dict):
    """
    PolicyNet policy creation.

    Incoming request looks like this:
    {
        'site': '4103861111',
        'switch_addresses': ['TM022014316'],
        'status': 'OFF',
        'start_datetime': '2022-06-23T03:30:00+00:00',
        'end_datetime': '2022-06-23T03:35:00+00:00',
        'correlation_id': '4103861111-2022-06-23T133423-151a7abb-ef95-4ca7-85ae-22a9af668022',
    }

    :param request:
    :return:
    """
    logger.info("handle_create_policy:\n%s", request)

    correlation_id: str = request["correlation_id"]
    errors: list = RequestValidator.validate_dlc_override_request(
        request, DEFAULT_OVERRIDE_DURATION_MINUTES, check=False
    )

    if errors:
        if isinstance(correlation_id, list):
            partial_report_errors = partial(report_errors, errors=errors)
            list(map(partial_report_errors, correlation_id))
        else:
            report_errors(correlation_id, errors)

        return {
            "statusCode": HTTP_BAD_REQUEST,
            "message": "Invalid request",
            "errorDetails": [e.error for e in errors],
        }
    else:
        now: datetime = datetime.now(timezone.utc)
        deploy_start_datetime: str = now.strftime(POLICY_DEPLOY_DATETIME_FORMAT)
        if not isinstance(correlation_id, list):
            # Get request(s) that are previously contiguous to this one.
            contiguous_requests = new_get_contiguous_request(request)
            # logger.debug("Default policy deployment start datetime: %s", deploy_start_datetime)

            # Contiguous with the same switch direction
            for contiguous_request in contiguous_requests:
                # Contiguous with the same switch direction
                if request["status"] == contiguous_request["overrdValue"]:
                    logger.debug("Contiguous request: %s", contiguous_request)
                    # logger.debug("Terminal request: %s", terminal_request)

                    # Extend policy in PolicyNet.
                    response: dict = extend_policy(contiguous_request)

                    if response["statusCode"] == HTTP_SUCCESS:
                        # Check to see when we can deploy this policy - if the contiguous policy is currently
                        # being enforced ("now" between start and end date), we can deploy immediately.
                        start: datetime = datetime.fromisoformat(
                            contiguous_request["rqstStrtDt"]
                        )
                        end: datetime = datetime.fromisoformat(
                            contiguous_request["rqstEndDt"]
                        )

                        if not is_being_enforced(start, end, now):
                            # Policy currently NOT being enforced.
                            # We need to pause deployment until after the contiguous policy is being enforced.
                            deploy_start: datetime = start + timedelta(
                                minutes=CONTIGUOUS_START_BUFFER_MINUTES
                            )
                            deploy_start_datetime: str = deploy_start.strftime(
                                POLICY_DEPLOY_DATETIME_FORMAT
                            )
                            logger.info(
                                "Setting policy deployment start datetime to %s",
                                deploy_start_datetime,
                            )

                # Contiguous with the opposite switch direction
                else:
                    # When there is a contiguous request of the opposite switch direction we need to push back the start
                    # datetime so the contiguous request has time to finish as it doesn't finish exactly at the end time in
                    # PolicyNet.
                    start_datetime: datetime = datetime.fromisoformat(
                        request["start_datetime"]
                    )
                    start_datetime += timedelta(
                        minutes=OPPOSITE_SWITCH_DIRECTION_BACKOFF
                    )
                    start_datetime_str: str = start_datetime.isoformat()

                    request["start_datetime"] = start_datetime_str
                    request["replace"] = True
                    response: dict = create_policy(request)
                response["deploy_start_datetime"] = deploy_start_datetime
                response["request"] = contiguous_request
                # responses.append(response)
            # else:
            #     # No contiguous request - create a new stand-alone policy and deploy straight away.
            if request["switch_addresses"]:
                request["replace"] = False
                response: dict = create_policy(request)
                response["deploy_start_datetime"] = deploy_start_datetime
                response["request"] = request
                # responses.append(response)
        else:
            if "flag" in request:
                if request["flag"] == "cont-extend":
                    response: dict = extend_policy(request)

                    if response["statusCode"] == HTTP_SUCCESS:
                        # Check to see when we can deploy this policy - if the contiguous policy is currently
                        # being enforced ("now" between start and end date), we can deploy immediately.
                        start: datetime = datetime.fromisoformat(request["rqstStrtDt"])
                        end: datetime = datetime.fromisoformat(request["rqstEndDt"])

                        if not is_being_enforced(start, end, now):
                            # Policy currently NOT being enforced.
                            # We need to pause deployment until after the contiguous policy is being enforced.
                            deploy_start: datetime = start + timedelta(
                                minutes=CONTIGUOUS_START_BUFFER_MINUTES
                            )
                            deploy_start_datetime: str = deploy_start.strftime(
                                POLICY_DEPLOY_DATETIME_FORMAT
                            )
                            logger.info(
                                "Setting policy deployment start datetime to %s",
                                deploy_start_datetime,
                            )

                elif request["flag"] and request["flag"] == "cont-create":
                    start_datetime: datetime = datetime.fromisoformat(
                        request["start_datetime"]
                    )
                    start_datetime += timedelta(
                        minutes=OPPOSITE_SWITCH_DIRECTION_BACKOFF
                    )
                    start_datetime_str: str = start_datetime.isoformat()

                    request["start_datetime"] = start_datetime_str
                    request["replace"] = True
                    response: dict = create_policy(request)
            else:
                request["replace"] = False
                response: dict = create_policy(request)
                response["deploy_start_datetime"] = deploy_start_datetime
                response["request"] = request
                # responses.append(response)

        # Add the policy deployment start datetime.
        logger.info("Create policy Respone : %s", response)
        return response


def handle_deploy_policy(event: dict):
    """
    PolicyNet policy deployment.

    :param event:
    :return:
    """
    logger.debug("handle_deploy_policy: %s", event)
    logger.info("Deploying policy in PolicyNet")
    correlation_ids = event["request"]["correlation_id"]
    if not isinstance(correlation_ids, list):
        correlation_ids = [correlation_ids]

    if "policyID" not in event:
        now: datetime = datetime.now(timezone.utc)
        message: str = "Invalid request: policy deployment needs policy id"

        # Update tracker.
        for correlation_id in correlation_ids:
            update_tracker(
                correlation_id=correlation_id,
                stage=Stage.DECLINED,
                event_datetime=now,
                message=message,
            )

        # Create event.
        # payload: dict = assemble_event_payload(correlation_id, Stage.DECLINED, now, message)
        # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)

        return {"statusCode": HTTP_BAD_REQUEST, "message": message}

    # Deploy policy.
    # The response from PolicyNet doesn't include anything useful, so we'll have to be "as near as"
    # with event datetimes.

    now: datetime = datetime.now(timezone.utc)
    policy_id: int = event["policyID"]

    response: dict = deploy_policy(policy_id)

    status_code: int = response["statusCode"]
    message: str = response["message"]

    for correlation_id in correlation_ids:
        if status_code == HTTP_SUCCESS:
            # Update tracker.
            update_tracker(
                correlation_id=correlation_id,
                stage=Stage.POLICY_DEPLOYED,
                event_datetime=now,
                message=message,
                policy_id=policy_id,
            )
        else:
            # Update tracker.
            update_tracker(
                correlation_id=correlation_id,
                stage=Stage.DECLINED,
                event_datetime=now,
                message=message,
            )

        # Create event.
        # payload: dict = assemble_event_payload(correlation_id, Stage.DECLINED, now, message)
        # deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)

    return response


# @alert_on_exception(tags=AppConfig.LOAD_CONTROL_TAGS, service_name=LOAD_CONTROL_ALERT_SOURCE)
# @tracer.capture_lambda_handler
def lambda_handler(event: dict, _):
    """
    Lambda handler for State Machine (Step Function).

    :param event:
    :param _: Lambda context (not used)
    :return:
    """
    global policynet_client

    logger.info("----------------")
    logger.info("Direct Load Control State Machine override running...")
    logger.info("Event: %s", str(event))
    logger.info("----------------")

    if "action" not in event:
        return {
            "statusCode": HTTP_BAD_REQUEST,
            "message": "Invalid request: no action supplied.",
        }

    action: str = event["action"]
    logger.debug("Action supplied: %s", action)

    request: dict = event["request"]

    # Get PolicyNet credentials; set in our PolicyNet client object.
    # pnet_auth_details: dict = cn_secret_manager.get_secret_value_dict(AppConfig.PNET_AUTH_DETAILS_SECRET_ID)

    # if not policynet_client:
    #     url = pnet_auth_details['pnet_url']
    #     policynet_client = PolicyNetClient(f"{url}/PolicyNet.wsdl", url, DYNAMO_RESOURCE,
    #                                       session_table=PNET_SESSION_TABLE,
    #                                       session_lifetime=PNET_SESSION_LIFETIME_SECONDS)
    #     policynet_client.set_credentials(pnet_auth_details['pnet_username'], pnet_auth_details['pnet_password'])
    #     logger.debug("Set credentials in PolicyNet client")

    if action == SupportedOverrideSMActions.CREATE_DLC_POLICY.value:
        response = handle_create_policy(request)
        # return response if type(response) == list else [response]
    elif action == SupportedOverrideSMActions.DEPLOY_DLC_POLICY.value:
        response = handle_deploy_policy(event)
        # elif action == SupportedOverrideSMActions.LOGOUT_PNET.value:
        #     policynet_client.logout()

        response: dict = {
            "statusCode": HTTP_SUCCESS,
            "message": "Logged out successfully",
        }
    else:
        response = {
            "statusCode": HTTP_BAD_REQUEST,
            "message": f"Invalid request: unsupported action '{action}'",
        }

    return response


if __name__ == "__main__":
    data = {
        "action": "createDLCPolicy",
        "request": {
            "site": "4103861111",
            "switch_addresses": ["TM022014316"],
            "status": "OFF",
            "start_datetime": "2022-06-28T03:30:00+00:00",
            "end_datetime": "2022-06-28T03:40:00+00:00",
            "correlation_id": "4103861111-2022-06-23T133502-6d88dc36-c87b-4900-8690-aa6d6ebd9441",
        },
    }
    result = lambda_handler(data, None)
    logger.info(result)
