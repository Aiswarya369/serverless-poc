"""
Policynet Soap client
"""
import datetime
import logging
import os
import time
from dataclasses import dataclass
from datetime import timedelta, datetime
from distutils.util import strtobool
from logging import Logger
from typing import List, Union, Tuple, Optional, Dict, Any
import uuid
import random
import urllib3
from botocore.client import BaseClient


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: Logger = logging.getLogger(name="PolicyNetClient")

# Environment lookup; if null, set to INFO level.
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

# Constants.
PNET_MAX_POLICY_NAME_LENGTH: int = 64

USER_PREFIX: str = "USER#"
SESSION_ID_PREFIX: str = "SESSION-ID#"

NETWORK_CONNECTIVITY_CONNECTED: str = "CONNECTED"
ENERGY_STATUS_ENERGISED: str = "ENERGIZED"


def deploy_policy(policy_id: int):
    """
    Deploy a given policy.

    :param policy_id:
    :type policy_id:
    :return:
    :rtype:
    """
    logger.info("Deploying policy with policy ID %s", policy_id)

    # Prepare PolicyNet session.
    # self.prepare_session()

    req_data: dict = {
        # "_soapheaders": {
        #     "LoginUsername": self._user,
        #     "SessionId": self._session_id
        # },
        "policyId": policy_id
    }

    # self._plugin_log_request.enabled = True

    try:
        # soap_response = self._service.deployLoadControlSchedule(**req_data)
        logger.info("PNET response")

        deploy_reply = {
            "message": f"Policy {policy_id} deployed successfully",
            "statusCode": 200,
        }
    # except Fault as e:
    #     logger.exception(f"Exception while deploying policy. Fault: {e.message}")
    #     # translated_error = self.translate_soap_fault(e)
    #     deploy_reply = {
    #         "message": f"Exception while deploying policy. {translated_error}",
    #         "statusCode": 400,
    #     }
    except Exception as e:
        deploy_reply = {
            "message": "Exception while deploying policy",
            "statusCode": 400,
        }

        logger.exception(str(e))

    return deploy_reply


def create_lc_override_schedule_policy(
    meter_serials: Union[List, str],
    turn_off: bool,
    start_datetime: datetime,
    duration: int = 30,
    replace: bool = False,
) -> Tuple[str, dict]:
    """
    Create an override Load Control Policy.

    :param meter_serials: List of meter serial numbers
    :param turn_off: boolean flag indicating on/off status
    :param start_datetime:  UTC datetime to start override
    :param duration: override duration
    :param replace: whether the existing in-place policy should be replaced (aka "extended").

    :return: Dict containing created policy ID
    """
    if start_datetime is None:
        policy_start_datetime: str = LocalUtils.current_time_aest()
    else:
        policy_start_datetime: str = LocalUtils.format_datetime_2_xsd_datetime(
            LocalUtils.datetime_to_aest(start_datetime)
        )

    list_meter_serials: list = []

    if isinstance(meter_serials, str):
        list_meter_serials = [meter_serials]
    else:  # Assume list.
        list_meter_serials.extend(meter_serials)

    policy_targets: list = [
        {"utilitySerialNumber": meter} for meter in list_meter_serials
    ]
    device_action: str = "turnOff" if turn_off else "turnOn"
    device_action_display: str = "OFF" if turn_off else "ON"
    ts: int = int(time.time())
    policy_name: str = (
        f"DLCOverride({device_action_display})-"
        + "-".join(list_meter_serials)
        + "-"
        + str(ts)
    )

    logger.info(
        "Creating override policy %s for %s / %s / %s / %s; replace: %s",
        policy_name,
        policy_targets,
        policy_start_datetime,
        device_action,
        duration,
        replace,
    )

    # Prepare PolicyNet session.
    # self.prepare_session()

    req_data: dict = {
        # "_soapheaders": {"LoginUsername": self._user, "SessionId": self._session_id},
        "policy": {
            "directLoadControlOverride": {
                "name": policy_name[:PNET_MAX_POLICY_NAME_LENGTH],
                "description": "",
                "type": "directLoadControlOverride",
                "onRequest": True,
                "reportingWindow": 0,
                "deploymentMethod": "UnicastCops",
                "deployOnNextConnect": False,
                "switchAction": device_action,
                "overrideDuration": duration,
                "spreadOn": 0,
                "spreadOff": 0,
                "schedule": {"minutes": "0", "startTime": policy_start_datetime},
                "targets": policy_targets,
                "replace": replace,
            }
        }
    }

    # self._plugin_log_request.enabled = True

    try:
        # lc_response = self._service.createLoadControlSchedule(**req_data)
        # logger.info("lc_response %s", lc_response)

        policy_id = random.randint(1000,9999)

        override_schedule_reply: dict = {
            "message": "Direct load control override policy created successfully",
            "policyID": policy_id,
            "statusCode": 200,
        }
    # except Fault as e:
    #     logger.exception(
    #         f"Exception while creating direct load control policy. Fault: {e.message}"
    #     )

    #     # translated_error = self.translate_soap_fault(e)
    #     override_schedule_reply: dict = {
    #         "message": f"Exception while creating direct load control policy. {translated_error}",
    #         "statusCode": 400,
    #     }
    except Exception as e:
        logger.exception(str(e))

        override_schedule_reply: dict = {
            "message": "Exception while creating direct load control policy",
            "statusCode": 400,
        }

    return policy_name, override_schedule_reply


def replace_lc_override_schedule_policy(
    meter_serials: Union[List, str],
    turn_off: bool,
    start_datetime: datetime,
    duration: int = 30,
):
    """
    Replace an existing deployed policy in PolicyNet.
    This is the same as creating a new policy, except the "replace" flag is set to indicate that we're
    extending an existing request.

    :param meter_serials:
    :param turn_off:
    :param start_datetime:
    :param duration:
    :return:
    """
    return create_lc_override_schedule_policy(
        meter_serials=meter_serials,
        turn_off=turn_off,
        start_datetime=start_datetime,
        duration=duration,
        replace=True,
    )


def undeploy_load_control_schedule(policy_id):
    """
    Given a load control policy id, undeploy the related policy schedule in PolicyNet
    Requires operations pnet user

    :param policy_id: id of policy
    """
    # Prepare PolicyNet session.
    # self.prepare_session()

    req_data: dict = {
        # "_soapheaders": {
        #     "LoginUsername": self._user,
        #     "SessionId": self._session_id,
        # },
        "policyId": policy_id,
    }

    try:
        # soap_response = self._service.undeployLoadControlSchedule(**req_data)
        logger.info("Undeploy load control response")
    # except Fault as e:
    #     logger.exception(
    #         f"Exception while undeploying load control policy. Fault: {e.message} "
    #     )
    #     raise
    except Exception as e:
        logger.exception(
            f"Exception while undeploying load control policy. Fault: {str(e)} "
        )
        raise


def delete_load_control_schedule(policy_id):
    """
    Given a load control policy id, delete the related policy schedule in PolicyNet.
    Requires operations PNet user.

    :param policy_id: id of policy
    :return:
    """
    # Prepare PolicyNet session.
    # self.prepare_session()

    req_data: dict = {
        # "_soapheaders": {
        #     "LoginUsername": self._user,
        #     "SessionId": self._session_id,
        # },
        "policyId": policy_id,
    }

    # self._plugin_log_request.enabled = True

    try:
        # soap_response = self._service.deleteLoadControlSchedule(**req_data)
        logger.info("delete load control response")
    # except Fault as e:
    #     logger.exception(
    #         f"Exception while deleting load control policy. Fault: {e.message} "
    #     )
    #     raise
    except Exception as e:
        logger.exception(
            f"Exception while deleting load control policy. Fault: {str(e)} "
        )
        raise


class LocalUtils:
    """
    Utilities class of static methods by Policynet client
    """

    @staticmethod
    def format_datetime_2_xsd_datetime(dt: datetime) -> str:
        """
        Format datetime to XSD format string
        :param dt: datetime
        :return: string in format 'YYYY-mm-ddThh:MM:ss'
        """
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def datetime_to_aest(dt: datetime) -> datetime:
        """
        Convert local datetime to AEST
        :param dt: datetime
        :return: AEST datetime (+10:00)
        """
        delta = timedelta(hours=10)
        return dt + delta

    @staticmethod
    def current_time_aest() -> str:
        """
        Get current datetime in AEST
        :return: current AEST datetime (+10:00)
        """
        now: datetime = datetime.now()
        delta: timedelta = timedelta(hours=10)
        return LocalUtils.format_datetime_2_xsd_datetime(now + delta)

    @staticmethod
    def parse_iso_datetime(date_string: str) -> Optional[datetime]:
        """
        Given a string, parse the string and provide a datetime.

        :param date_string:
        :return:
        """
        if not date_string:
            return None

        return datetime.datetime.fromisoformat(date_string)
