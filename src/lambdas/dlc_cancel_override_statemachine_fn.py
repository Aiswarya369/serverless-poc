import logging
import os
from datetime import datetime, timezone
from logging import Logger
from typing import Optional

import boto3
import cresconet_aws.secrets_manager as cn_secret_manager
from aws_lambda_powertools import Tracer
from botocore.client import BaseClient
from cresconet_aws.support import alert_on_exception
from msi_common import Stage
from msi_policynet_client import PolicyNetClient

from src.config.config import AppConfig
from src.lambdas.dlc_event_helper import assemble_event_payload
from src.model.sm_actions import SupportedCancelOverrideSMActions
from src.utils.kinesis_utils import deliver_to_kinesis
from src.utils.tracker_utils import (
    update_tracker,
    get_header_record,
    get_contiguous_request,
)

# -----------------------
# Environmental variables
# -----------------------
KINESIS_DATA_STREAM_NAME: str = os.environ.get("KINESIS_DATA_STREAM_NAME")
LOG_LEVEL: str = os.environ.get("LOG_LEVEL", logging.INFO)
REGION: str = os.environ.get("REGION", "ap-southeast-2")
REQUEST_TRACKER_TABLE_NAME: str = os.environ.get("REQUEST_TRACKER_TABLE_NAME")
PNET_SESSION_TABLE: str = os.environ.get("PNET_SESSION_TABLE")
PNET_SESSION_LIFETIME_SECONDS: int = int(
    os.environ.get("PNET_SESSION_LIFETIME_SECONDS", 300)
)

# -------
# Logging
# -------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: Logger = logging.getLogger(name="LoadControlFunctions")
logger.setLevel(LOG_LEVEL)

# ----------------
# Global variables
# ----------------
CANCELLATION_REASON: str = "User-initiated cancellation of Direct load control request"
HTTP_SUCCESS: int = 200
STEP_FUNCTION_IN_PROGRESS_STAGES = [
    Stage.QUEUED.value,
    Stage.POLICY_CREATED.value,
    Stage.POLICY_EXTENDED.value,
]
POLICY_DEPLOYED_STATES = [
    Stage.POLICY_DEPLOYED.value,
    Stage.DLC_OVERRIDE_STARTED.value,
    Stage.EXTENDED_BY.value,
]
POLICY_DEPLOY_DATETIME_FORMAT: str = "%Y-%m-%dT%H:%M:%SZ"
# used to specify which cancellation replace scenario to take; matches what is found in the dlc-cancel-override-sm.json
# definition
REPLACE_FIRST_REQUEST = "replace_first_request"
REPLACE_SECOND_REQUEST = "replace_second_request"
LOAD_CONTROL_ALERT_SOURCE = "dlc-cancel-override-statemachine-fn"

# -------
# Clients
# -------
policynet_client: Optional[PolicyNetClient] = None
step_function_client: Optional[BaseClient] = None
dynamo_resource: Optional[BaseClient] = None

tracer: Tracer = Tracer(service="dlc")


def init_clients():
    """
    Create a clients where they don't exist, using global variables to avoid cold starts
    """
    global step_function_client, dynamo_resource, policynet_client

    if not step_function_client:
        step_function_client = boto3.client("stepfunctions", region_name=REGION)

    if not dynamo_resource:
        dynamo_resource = boto3.resource("dynamodb", region_name=REGION)

    if not policynet_client:
        # Get PolicyNet credentials; set in our PolicyNet client object.
        pnet_auth_details: dict = cn_secret_manager.get_secret_value_dict(
            AppConfig.PNET_AUTH_DETAILS_SECRET_ID
        )

        url = pnet_auth_details["pnet_url"]
        policynet_client = PolicyNetClient(
            f"{url}/PolicyNet.wsdl",
            url,
            dynamo_resource,
            session_table=PNET_SESSION_TABLE,
            session_lifetime=PNET_SESSION_LIFETIME_SECONDS,
        )
        policynet_client.set_credentials(
            pnet_auth_details["pnet_username"], pnet_auth_details["pnet_password"]
        )


def evaluate_request(event: dict) -> dict:
    """
    Figures out whether a policy needs to replace an existing one in order to cancel the request
    Returns a dictionary containing a "replace" boolean, true if the policy in PolicyNet required replacing, false if
    not.

    @param event:
    @return:
    """
    request: dict = event["request"]
    logger.info("Initialising evaluate request with request: %s", request)

    payload = {
        "cancelled_correlation_id": request["correlation_id"],
        "policy_id": request["policy_id"],
        "replace": False,
    }

    # This checks if the cancellation scenario is that where we have two contiguous requests and we are cancelling the
    # first one e.g.
    #   request 1
    #   1000-1030
    #
    #   request 2
    #   1030-1100
    #
    #   Cancel request 1 at 1015
    #
    #   In this case we want to stop the override now (1015) and then have it start again at 1030-1100, so we will
    #   undeploy the current policies and deploy a new one for 1030-1100.
    if request["current_stage"] == Stage.EXTENDED_BY.value:
        extends_record: dict = get_header_record(request["extended_by"])
        payload = {
            "cancelled_correlation_id": request["correlation_id"],
            "replaced_correlation_id": extends_record["crrltnId"],
            "replacement_start_datetime": extends_record["rqstStrtDt"],
            "replacement_end_datetime": extends_record["rqstEndDt"],
            "extended_by_policy_id": extends_record["plcyId"],
            "extended_by_stage": extends_record["currentStg"],
            "status": request["status"],
            "meter_serial_number": request["meter_serial_number"],
            "replace": REPLACE_SECOND_REQUEST,
        }

    # This checks if the cancellation scenario is that where we have two contiguous requests and we are cancelling the
    # second one when the first one is still in enforcing (start date < now < end date) e.g.
    #   request 1
    #   1000-1030
    #
    #   request 2
    #   1030-1100
    #
    #   Cancel request 2 at 1015
    #
    #   In this case we want the override to action from 1000-1030, so we create a replacement policy for this time to
    #   overwrite the currently deployed policy of 1000-1100.
    elif request["current_stage"] in [
        Stage.POLICY_EXTENDED.value,
        Stage.POLICY_DEPLOYED.value,
        Stage.DLC_OVERRIDE_STARTED.value,
    ]:
        contiguous_request = get_contiguous_request(request, cancel_req=True)

        # Contiguous with the same switch direction
        if (
            contiguous_request
            and request["status"] == contiguous_request["overrdValue"]
        ):
            contiguous_request_start_date = datetime.fromisoformat(
                contiguous_request["rqstStrtDt"]
            )
            contiguous_request_end_date = datetime.fromisoformat(
                contiguous_request["rqstEndDt"]
            )
            # Get the current datetime in UTC without microseconds to match what is stored in the tracker table
            now: datetime = datetime.now(timezone.utc).replace(microsecond=0)

            logger.info(
                "Contiguous request start date %s\nend date %s\nNow %s",
                contiguous_request_start_date.isoformat(),
                contiguous_request_end_date.isoformat(),
                now.isoformat(),
            )
            if contiguous_request_end_date > now > contiguous_request_start_date:
                payload = {
                    "cancelled_correlation_id": request["correlation_id"],
                    "replaced_correlation_id": contiguous_request["crrltnId"],
                    "replacement_start_datetime": contiguous_request["rqstStrtDt"],
                    "replacement_end_datetime": contiguous_request["rqstEndDt"],
                    "status": request["status"],
                    "meter_serial_number": request["meter_serial_number"],
                    "replace": REPLACE_FIRST_REQUEST,
                }
            # In this case the contiguous request has not started yet so we can just delete the request afterwards as it
            # will not be deployed until after the contiguous request has started.
            elif now < contiguous_request_start_date:
                update_tracker(
                    correlation_id=contiguous_request["crrltnId"],
                    stage=Stage.POLICY_DEPLOYED,
                    event_datetime=now,
                    message="Request that extended this one was cancelled so reinstating this one",
                    request_start_date=datetime.fromisoformat(
                        contiguous_request["rqstStrtDt"]
                    ),
                    request_end_date=datetime.fromisoformat(
                        contiguous_request["rqstEndDt"]
                    ),
                )

    logger.info("Returning a payload: %s", payload)
    return payload


def create_policy(payload: dict) -> dict:
    """
    Creates a policy in PolicyNet
    Also updates the DLC override tracker to reflect this change and returns the policy ID of the new policy.

    @param payload:
    @return:
    """
    # Extract variables
    evaluated_request: dict = payload["evaluated_request"]["payload"]

    replaced_correlation_id: str = evaluated_request["replaced_correlation_id"]
    meter_serial: str = evaluated_request["meter_serial_number"]
    turn_off: bool = False if evaluated_request["status"] == "ON" else True
    start_datetime: datetime = datetime.fromisoformat(
        evaluated_request["replacement_start_datetime"]
    )
    end_datetime: datetime = datetime.fromisoformat(
        evaluated_request["replacement_end_datetime"]
    )
    duration: int = int((end_datetime - start_datetime).total_seconds() / 60)
    replace: bool = False if evaluated_request["replace"] is False else True

    request = payload["request"]
    current_stage: str = request["current_stage"]

    # Stop step creation step function if it exists.
    if current_stage in STEP_FUNCTION_IN_PROGRESS_STAGES:
        execution_arn: str = (
            f"{AppConfig.DLC_OVERRIDE_SM_EXECUTION_ARN}:{request['correlation_id']}"
        )
        logger.info("Stopping step function with ARN %s", execution_arn)
        response: dict = step_function_client.stop_execution(
            executionArn=execution_arn, cause=CANCELLATION_REASON
        )
        logger.info("Step function response: %s", response)

    logger.info(
        "Creating a PolicyNet policy with meter serial %s, status %s, start datetime %s, duration %s and replace %s",
        meter_serial,
        turn_off,
        start_datetime,
        duration,
        replace,
    )

    # Create policy
    now: datetime = datetime.now(timezone.utc)

    policy_name, response = policynet_client.create_lc_override_schedule_policy(
        meter_serial, turn_off, start_datetime, duration, replace
    )

    status_code: int = response["statusCode"]
    message: str = response["message"]
    policy_id: int = response["policyID"]

    if status_code != HTTP_SUCCESS:
        logger.error("Error creating policy; PolicyNet returned response: %s", response)
        raise RuntimeError(
            "Error creating policy; PolicyNet returned response: %s", response
        )
    else:
        # update tracker
        update_tracker(
            correlation_id=replaced_correlation_id,
            stage=Stage.POLICY_CREATED,
            event_datetime=now,
            message=message,
            policy_name=policy_name,
            policy_id=policy_id,
            request_start_date=start_datetime,
            request_end_date=end_datetime,
        )
        payload["policy_id"] = response["policyID"]
        return payload


def deploy_replacement_policy(payload: dict) -> str:
    """
    Given a payload dictionary containing a policy_id, deploy the policy in PolicyNet and update the DLC override
    tracker to reflect the change in status.

    example payload:
            {'request':
                {'correlation_id': '4407114520-2022-07-15T085749-e1da06a6-6b6d-4920-a54d-2c6d40c6831a', 'site': '4407114520', 'switch_addresses': ['LG022102554'],
                'status': 'ON', 'meter_serial_number': 'LG022102554', 'current_stage': 'EXTENDED_BY', 'start_datetime': '2022-07-14T23:00:00+00:00',
                'end_datetime': '2022-07-14T23:05:00+00:00', 'policy_id': 8836, 'extended_by': '4407114520-2022-07-15T085757-9b6af73f-64ab-43a7-9c1e-73be2203af59',
                'subscription_id': '07922f44ce874a4d8d658f8091edf7f5'},
            'evaluated_request':
                {'payload':
                    {'cancelled_correlation_id': '4407114520-2022-07-15T085749-e1da06a6-6b6d-4920-a54d-2c6d40c6831a',
                    'replaced_correlation_id': '4407114520-2022-07-15T085757-9b6af73f-64ab-43a7-9c1e-73be2203af59',
                    'replacement_start_datetime': '2022-07-14T23:00:00+00:00', 'replacement_end_datetime': '2022-07-14T23:10:00+00:00',
                    'status': 'ON', 'meter_serial_number': 'LG022102554', 'replace': True}
                },
            'policy_id': 8838
            }
    @param payload:
    @return:
    """
    # Extract variables
    evaluated_request: dict = payload["evaluated_request"]["payload"]

    policy_id: int = payload["policy_id"]
    replaced_correlation_id: str = evaluated_request["replaced_correlation_id"]
    start_datetime: datetime = datetime.fromisoformat(
        evaluated_request["replacement_start_datetime"]
    )
    end_datetime: datetime = datetime.fromisoformat(
        evaluated_request["replacement_end_datetime"]
    )

    logger.info("Deploying policy with policy id %s", policy_id)

    # Deploy policy
    now: datetime = datetime.now(timezone.utc)
    response: dict = policynet_client.deploy_policy(policy_id)

    status_code: int = response["statusCode"]
    message: str = response["message"]

    if status_code != HTTP_SUCCESS:
        logger.error("Error creating policy; PolicyNet returned response: %s", response)
        raise RuntimeError(
            "Error creating policy; PolicyNet returned response: %s", response
        )
    else:
        # update tracker
        update_tracker(
            correlation_id=replaced_correlation_id,
            stage=Stage.POLICY_DEPLOYED,
            event_datetime=now,
            message=message,
            request_start_date=start_datetime,
            request_end_date=end_datetime,
        )
        return now.isoformat()


def cancel_policy(payload: dict) -> tuple:
    """
    Stop the step function for a request if it exists and undeploy and delete the policy in PolicyNet if it exists.
    Returns the time of cancellation as a string

    @param payload:
    @return:
    """
    request = payload["request"]
    evaluated_request = payload["evaluated_request"]["payload"]
    current_stage: str = request["current_stage"]
    if current_stage in STEP_FUNCTION_IN_PROGRESS_STAGES:
        execution_arn: str = (
            f"{AppConfig.DLC_OVERRIDE_SM_EXECUTION_ARN}:{request['correlation_id']}"
        )
        logger.info("Stopping step function with ARN %s", execution_arn)
        response: dict = step_function_client.stop_execution(
            executionArn=execution_arn, cause=CANCELLATION_REASON
        )
        logger.info("Step function response: %s", response)

    # Cancel policy

    # Time to be sent by Kinesis to mark the time the policy is cancelled
    now: datetime = datetime.now(timezone.utc)
    # Policy ID will be None if the policy has not yet been created
    policy_id: int = request["policy_id"]

    if not policy_id or not policynet_client.check_policy_exists(policy_id):
        logger.info("Policy %s does not exist", policy_id)
    else:
        # Could be in a state where the policy has been created and is waiting to be deployed.
        if current_stage in POLICY_DEPLOYED_STATES:
            logger.info("Undeploying policy id %s", policy_id)
            policynet_client.undeploy_load_control_schedule(policy_id)
        logger.info("Deleting policy id %s", policy_id)
        policynet_client.delete_load_control_schedule(policy_id)
        # If the current stage is EXTENDED_BY then this is the case when we are deleting a request that has been
        # extended by another, so we must cancel both the original policy and the extended policy.
        if current_stage == Stage.EXTENDED_BY.value:
            extended_by_current_stage: str = evaluated_request["extended_by_stage"]
            policy_id: int = evaluated_request["extended_by_policy_id"]
            # Only undeploy if the policy is deployed otherwise it will fail
            if extended_by_current_stage in POLICY_DEPLOYED_STATES:
                logger.info("Undeploying policy id %s", policy_id)
                policynet_client.undeploy_load_control_schedule(policy_id)
            logger.info("Deleting policy id %s", policy_id)
            policynet_client.delete_load_control_schedule(policy_id)
    return payload, now.isoformat()


def cancel_complete(payload: dict) -> None:
    """
    Update the tracker and send an event to Kinesis of the cancellation completion

    @param payload:
    @return:
    """
    # Extract variables
    correlation_id: str = payload["request"]["correlation_id"]
    stopped_datetime: str = payload["stopped_datetime"]["payload"]
    stopped_datetime: datetime = datetime.fromisoformat(stopped_datetime)

    # Update tracker.
    logger.info(
        "Updating load control request tracker to CANCELLED with correlation id %s at %s",
        correlation_id,
        stopped_datetime,
    )
    update_tracker(
        correlation_id=correlation_id,
        stage=Stage.CANCELLED,
        event_datetime=stopped_datetime,
        message=CANCELLATION_REASON,
    )

    # Deliver to kinesis
    payload: dict = assemble_event_payload(
        correlation_id, Stage.CANCELLED, stopped_datetime, CANCELLATION_REASON
    )
    deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)


def failure_handler(payload: dict):
    """
    Function handle any failures in the cancel override state machine.
    This will log the error and send a Kinesis event to say the cancellation failed.

    example payload:
    {
        "action": "failure",
        "payload": {
            "correlation_id": "4103861111-2022-07-14T103743-10a5c03c-3099-4821-8bcd-d5f2607c961c",
            "site": "4103861111",
            "switch_addresses": [
                "TM022014316"
            ],
            "status": "ON",
            "meter_serial_number": "TM022014316",
            "current_stage": "QUEUED",
            "start_datetime": "2022-07-15T22:15:00+00:00",
            "end_datetime": "2022-07-15T22:20:00+00:00",
            "policy_id": "None",
            "extended_by": "None",
            "error": {
                "Error": "ClientError",
                "Cause": {
                    "errorMessage": "An error occurred (AccessDeniedException) when calling the PutRecord operation: User: arn:aws:sts::412144141892:assumed-role/msi-dlc-override-service-LoadControlCancelRequest-1M7H5YQCTU6DA/msi-dev-dlc-cancel-override-statemachine-fn is not authorized to perform: kinesis:PutRecord on resource: arn:aws:kinesis:ap-southeast-2:412144141892:stream/msi-dev-subscriptions-kds because no identity-based policy allows the kinesis:PutRecord action",
                    "errorType": "ClientError",
                    "stackTrace": "  File \\/var/task/src/lambdas/dlc_cancel_override_statemachine_fn.py\\, line 389, in lambda_handler\\n    cancel_complete(event[\\payload\\])\\n,  File \\/var/task/src/lambdas/dlc_cancel_override_statemachine_fn.py\\, line 337, in cancel_complete\\n    deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)\\n,  File \\/var/task/src/utils/kinesis_utils.py\\, line 36, in deliver_to_kinesis\\n    response: dict = KINESIS_CLIENT.put_record(\\n,  File \\/var/task/botocore/client.py\\, line 508, in _api_call\\n    return self._make_api_call(operation_name, kwargs)\\n,  File \\/var/task/botocore/client.py\\, line 915, in _make_api_call\\n    raise error_class(parsed_response, operation_name)\\n"
                }
            }
        }
    }

    @param payload:
    @return:
    """

    error = payload["error"]["Error"]
    cause = payload["error"]["Cause"]

    now: datetime = datetime.now(timezone.utc)

    logger.error(
        "DLC cancel override step function failed with the following error: %s\n cause: %s",
        error,
        cause,
    )

    payload: dict = {
        "eventType": "LOAD_CONTROL",
        "subscription_id": payload["subscription_id"],
        "site": payload["site"],
        "event_description": f"Cancellation of request {payload['correlation_id']} failed due to {error}",
        "event_datetime": now.isoformat(),
    }
    deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)


@alert_on_exception(
    tags=AppConfig.LOAD_CONTROL_TAGS, service_name=LOAD_CONTROL_ALERT_SOURCE
)
@tracer.capture_lambda_handler
def lambda_handler(event: dict, _):
    """
    Lambda handler for DLC cancel override state machine (Step Function).

    :param event:
    :param _: Lambda context (not used)
    :return:
    """
    init_clients()

    logger.info(
        "DLC cancel override step function lambda started with event: %s", event
    )

    action = event["action"]

    # The if statements below are used to recognise which state of the state machine we are in and handles things
    # depending on that

    # Evaluates the cancellation request to decide which path of the state machine this request should take by updating
    # the "replace" key value in the payload.
    if action == SupportedCancelOverrideSMActions.EVALUATE_REQUEST.value:
        return evaluate_request(event["payload"])

    # Three state machine scenarios
    #
    #   Scenario 1
    #       When there is a previous contiguous request to the one to be cancelled that is still
    #       enforcing (start < now < end)
    #
    #   Scenario 2
    #       When there is a contiguous request after the one to be cancelled that has already been deployed
    #
    #   Scenario 3 (default)
    #       Everything else, generally a single request with no contiguous requests

    # Scenario 1 step 1
    # Creates a replacement policy in PolicyNet with the details within the event["payload"]
    elif action == SupportedCancelOverrideSMActions.CREATE_REPLACEMENT_POLICY.value:
        return create_policy(event["payload"])

    # Scenario 1 step 2
    # Deploys the replacement policy in PolicyNet using the policy id provided in the event["payload"]
    elif action == SupportedCancelOverrideSMActions.DEPLOY_REPLACEMENT_POLICY.value:
        return deploy_replacement_policy(event["payload"])

    # Scenario 2 step 1
    # Undeploys and deletes the policies acting on the meter. There are two policies, the original one created for
    # the first request, plus the replacement one created when the extension was put in place.
    # Both must be undeployed and deleted.
    elif action == SupportedCancelOverrideSMActions.UNDEPLOY_POLICY.value:
        payload, _ = cancel_policy(event["payload"])
        return payload

    # Scenario 2 step 2
    # Creates a new policy in PolicyNet for the time specified in event["payload"]
    elif action == SupportedCancelOverrideSMActions.CREATE_NEW_POLICY.value:
        return create_policy(event["payload"])

    # Scenario 2 step 3
    # Deploys the policy in PolicyNet using the policy id provided in the event["payload"]
    elif action == SupportedCancelOverrideSMActions.DEPLOY_NEW_POLICY.value:
        return deploy_replacement_policy(event["payload"])

    # Scenario 3 step 1
    # Undeploys the create dlc policy step function if it is in progress and then undeploys and deletes the policy
    # in PolicyNet if it exists.
    elif action == SupportedCancelOverrideSMActions.CANCEL_POLICY.value:
        _, time_of_cancel = cancel_policy(event["payload"])
        return time_of_cancel

    # Updates the load control request tracker to cancellation complete and sends a event onto the kinesis stream
    # with the same info
    elif action == SupportedCancelOverrideSMActions.CANCELLATION_COMPLETE.value:
        cancel_complete(event["payload"])

    # Sends an event to kinesis to notify of the cancellation failure
    elif action == SupportedCancelOverrideSMActions.FAILURE.value:
        failure_handler(event["payload"])


if __name__ == "__main__":
    # Evaluate request
    evaluate_payload = {
        "request": {
            "correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
            "site": "4407114520",
            "switch_addresses": ["LG022102554"],
            "status": "ON",
            "current_stage": "DEPLOYED",
            "start_datetime": "2022-07-29T21:13:00+00:00",
            "end_datetime": "2022-07-29T21:17:00+00:00	",
            "policy_id": "8571",
        }
    }

    # Create/deploy replacement policy
    deploy_payload = {
        "action": "createReplacementPolicy",
        "payload": {
            "evaluated_request": {
                "payload": {
                    "cancelled_correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
                    "replaced_correlation_id": "4407114520-2022-07-05T123822-76ae3acf-020d-4e06-b566-20e046cdb89c",
                    "replacement_start_datetime": "2022-07-29T21:13:00+00:00",
                    "replacement_end_datetime": "2022-07-29T21:17:00+00:00",
                    "status": "ON",
                    "replace": True,
                    "policy_id": "1234",
                }
            }
        },
    }

    # Cancel policy
    cancel_payload = {
        "action": "cancelPolicy",
        "payload": {
            "request": {
                "correlation_id": "4407114520-2022-07-12T135313-70a089e7-0d9c-488e-aaea-ec4892552c00",
                "site": "4407114520",
                "switch_addresses": ["LG022102554"],
                "status": "ON",
                "meter_serial_number": "LG022102554",
                "current_stage": "QUEUED",
                "start_datetime": "2022-07-26T21:13:00+00:00",
                "end_datetime": "2022-07-26T21:17:00+00:00",
                "policy_id": None,
                "extended_by": None,
            },
            "evaluated_request": {
                "payload": {
                    "cancelled_correlation_id": "4407114520-2022-07-12T135313-70a089e7-0d9c-488e-aaea-ec4892552c00",
                    "policy_id": None,
                    "replace": False,
                }
            },
        },
    }
