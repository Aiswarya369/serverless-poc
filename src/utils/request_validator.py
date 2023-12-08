import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from logging import Logger
from typing import Tuple, Optional, List

import boto3
from boto3.dynamodb.conditions import Key, Attr, Not
from botocore.client import BaseClient
from msi_common import Stage

from src.utils.aws_utils import get_subscription

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger: Logger = logging.getLogger(name="RequestValidator")

# Environment lookup; if null, set to INFO level.
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

# Environmental variables.
REGION: str = os.environ.get("REGION", "ap-south-1")
REQUEST_TRACKER_TABLE_NAME: str = os.environ.get("REQUEST_TRACKER_TABLE_NAME", "msi-dev-request-tracker-ddb")

# Boto3 resource for DynamoDB.
DYNAMO_RESOURCE: BaseClient = boto3.resource("dynamodb", region_name=REGION)

# Constants.
DATE_FORMAT: str = "%Y-%m-%dT%H:%M:%S%z"
VALID_STATUS_VALUES: list = ["ON", "OFF"]
LOAD_CONTROL_SERVICE_NAME: str = "load_control"

MSG_DUPLICATE: str = "Request is the duplicate of an existing request"
MSG_OVERLAP: str = "Request rejected as it overlaps with at least one existing request; " \
                   "please cancel the existing request(s)"

GSI3PK_PREFIX: str = "SITE#MTR#"
GSI3SK_PREFIX: str = "REQUESTENDDATE#"


@dataclass
class ValidationError:
    error: str

    def __hash__(self):
        """
        Define a hashing function as we'll be adding records of this class to a set.

        :return:
        """
        return hash(self.error)


class RequestValidator:
    @staticmethod
    def validate_dlc_override_request(request: dict, override_duration: int) -> List[ValidationError]:
        """
        Perform validation of the supplied request.

        :param request:
        :param override_duration:
        :return: a list of validation errors encountered.

         event_structure = {
            "subscription_id": "ffde6c4bef134b81ad7d5f2b39d98639",
            "service_name": "load_control",
            "site": "NMI0000001",
            "start_datetime": "2021-11-10T03:49:14.756",
            "end_datetime": "2021-11-10T03:49:14.756",
            "status": "ON",
            "switch_addresses": [
                "LG000001/E3"
            ]
        }
        """
        now: datetime = datetime.now(timezone.utc)
        validation_errors: List[ValidationError] = []

        if not request:
            validation_errors.append(ValidationError("Request is empty"))
            return validation_errors

        if "site" not in request:
            validation_errors.append(ValidationError("Site details required"))
        elif not request["site"]:  # checks the value is not none
            validation_errors.append(ValidationError("Site details required"))

        if "switch_addresses" not in request:
            validation_errors.append(ValidationError("Switch addresses (Meter Details) required"))
        elif not request["switch_addresses"]:  # checks the value is not none
            validation_errors.append(ValidationError("Switch addresses (Meter Details) required"))
        elif isinstance(request["switch_addresses"], list) and len(request["switch_addresses"]) > 1:
            # Can provide a string or a list containing one string.
            validation_errors.append(ValidationError("Multiple switch addresses supplied - expected one"))

        if "status" not in request:
            validation_errors.append(ValidationError("DLC status required"))
        else:
            if request["status"] not in VALID_STATUS_VALUES:
                validation_errors.append(ValidationError("DLC status should be either ON or OFF"))

        start: Optional[datetime] = None

        if "start_datetime" in request:
            start_datetime: str = request["start_datetime"]

            try:
                # Make sure the start date is in the correct format.
                start = datetime.strptime(start_datetime, DATE_FORMAT)

                if "end_datetime" not in request:
                    # No end date supplied; check if (start + default duration) is in the past.
                    end: datetime = start + timedelta(minutes=override_duration)

                    if end < now:
                        validation_errors.append(
                            ValidationError("No end date supplied: request's derived end date would be in the past")
                        )
            except ValueError:
                validation_errors.append(
                    ValidationError("Invalid start datetime format supplied - should be YYYY-mm-ddTHH:MM:SS+zz:zz")
                )

        if "end_datetime" in request:
            if "start_datetime" not in request:
                validation_errors.append(ValidationError("Cannot have an end_datetime without a start_datetime"))
            else:
                end_datetime: str = request["end_datetime"]

                try:
                    # Make sure the start date is in the correct format - caught via a ValueError below if not.
                    end: datetime = datetime.strptime(end_datetime, DATE_FORMAT)

                    # Check if the end date is the same as the start date or before the start date.
                    if start and end == start:
                        validation_errors.append(ValidationError("Request's end date is the same as the start date"))
                    elif start and end < start:
                        validation_errors.append(ValidationError("Request's end date is before the start date"))

                    # Check if the end date is in the past.
                    if end < now:
                        validation_errors.append(ValidationError("Request's end date is in the past"))
                except ValueError:
                    validation_errors.append(
                        ValidationError("Invalid end datetime format supplied - should be YYYY-mm-ddTHH:MM:SS+zz:zz")
                    )

        return validation_errors

    @staticmethod
    def validate_request_duration(request: dict, override_duration: int) -> List[ValidationError]:
        """
        Identify whether the supplied requests overlaps with any existing policy.

        :param request:
        :param override_duration:
        :return:
        """
        ddb_table: BaseClient = DYNAMO_RESOURCE.Table(REQUEST_TRACKER_TABLE_NAME)

        # We use a set here, rather than list, as a request can overlap > one deployed policy,
        # but we really only want to see at most one validation error message indicating this.
        validation_errors: set = set()

        site: str = request['site']

        # There should only be one MSN supplied in switch_addresses.
        switch_addresses = request['switch_addresses']
        meter_serial_number: str = switch_addresses[0] if type(switch_addresses) == list else switch_addresses

        if 'start_datetime' in request:
            # Parse and convert to UTC.
            start_datetime: datetime = datetime.fromisoformat(request['start_datetime']).astimezone(timezone.utc)
        else:
            start_datetime: datetime = datetime.now(timezone.utc)

        if 'end_datetime' in request:
            # Parse and convert to UTC.
            end_datetime: datetime = datetime.fromisoformat(request['end_datetime']).astimezone(timezone.utc)
        else:
            end_datetime: datetime = start_datetime + timedelta(minutes=override_duration)

        start: str = start_datetime.isoformat()
        end: str = end_datetime.isoformat()

        # Identify overlapping requests:
        # - the same site and meter serial number as the supplied request
        # - Load Control
        # - Date matching:
        #   - 1. new request starts before existing end date and ends after existing end date:
        #       10 ---- 12 (existing)
        #           11 ---- 13 (new), or
        #   09 ------------ 13 (new)
        #
        #   - 2. new request starts before existing start date and ends after existing start date:
        #       10 ---- 12 (existing)
        #   09 ---- 11 (new), or
        #   09 ------------ 13 (new)
        #
        #   - 3. new request within existing request:
        #   10 ------------ 13 (existing)
        #       11 ---- 12 (new)
        #
        #   - 4. new request starts at same time as existing request but ends earlier:
        #   10 ------------ 12 (existing)
        #   10 ---- 11 (new)
        #
        #   - 5. new request starts after existing request but finishes at the same time:
        #   10 ------------ 12 (existing)
        #           11 ---- 12 (new)
        #
        #   - 6. new request starts before existing request and finishes after:
        #       11 ---- 12 (existing)
        #   10 ------------ 13 (new)
        #
        #   - 7. new request has the same start and end dates (duplicate request):
        #   10 ---- 12 (existing)
        #   10 ---- 12 (new)
        #
        # Any requests where dates match (new request's start date = existing request's end date) are
        # considered contiguous.
        #
        items: list = []
        last_evaluated_key: Optional[dict] = None

        gsi3pk: str = f"{GSI3PK_PREFIX}{site}#{meter_serial_number}"
        gsi3sk: str = f"{GSI3SK_PREFIX}{start}"
        stages: list = [Stage.CANCELLED.value, Stage.DECLINED.value, Stage.DLC_OVERRIDE_FINISHED.value]

        while True:
            if last_evaluated_key:
                # In calls after the first (the second page of result data onwards), provide the LastEvaluatedKey
                # which was supplied as part of the previous page's results - specify as ExclusiveStartKey.
                response: dict = ddb_table.query(
                    IndexName="GSI3",
                    KeyConditionExpression=Key("GSI3PK").eq(gsi3pk)
                                           & Key("GSI3SK").gte(gsi3sk),  # GSI3SK holds the request end date.
                    FilterExpression=Attr("svcName").eq(LOAD_CONTROL_SERVICE_NAME)
                                     & Attr('rqstStrtDt').lte(end)
                                     & Not(Attr('currentStg').is_in(stages)),
                    ExclusiveStartKey=last_evaluated_key
                )
            else:
                # This only runs the first time - provide no ExclusiveStartKey initially.
                response: dict = ddb_table.query(
                    IndexName="GSI3",
                    KeyConditionExpression=Key("GSI3PK").eq(gsi3pk)
                                           & Key("GSI3SK").gte(gsi3sk),  # GSI3SK holds the request end date.
                    FilterExpression=Attr("svcName").gte(LOAD_CONTROL_SERVICE_NAME)
                                     & Attr('rqstStrtDt').lte(end)
                                     & Not(Attr('currentStg').is_in(stages))
                )

            # Append retrieved records to our result set.
            items.extend(response['Items'])

            # Set our LastEvaluatedKey to the value for next operation if there is one.
            # Otherwise, there's no more results; we can exit.
            if 'LastEvaluatedKey' in response:
                last_evaluated_key = response['LastEvaluatedKey']
                logger.debug("Last evaluated key: %s - retrieving more records", last_evaluated_key)
            else:
                break

        logger.debug("Overlap validation: found %s relevant records", len(items))

        for item in items:
            request_start_date: str = item['rqstStrtDt']
            request_end_date: str = item['rqstEndDt']

            if request_start_date == start and request_end_date == end:
                # Duplicate non-CANCELLED request.
                logger.info("Conflicting start and end dates %s : %s", request_start_date, request_end_date)
                validation_errors.add(ValidationError(MSG_DUPLICATE))
                break  # Exit as soon as we can.
            elif request_start_date == end:
                # Contiguous candidate (existing record's start date = new request's end date).
                # Not an error - can be skipped.
                continue
            elif request_end_date == start:
                # Contiguous candidate (existing record's end date = new request's start date).
                # Not an error - can be skipped.
                continue
            else:
                logger.info("Conflicting start and end dates %s : %s", request_start_date, request_end_date)
                # No other option - must be an under/overlap.
                validation_errors.add(ValidationError(MSG_OVERLAP))
                break  # Exit as soon as we can.

        return list(validation_errors)

    # TODO
    # Currently this function only validates that a subscription_id matches an active subscription that allows
    # load control. The original reason appears to be related to performance requirements that were hit by the
    # amount of time required to maintain the subscription --> sit --> meter_number relationship up to date.
    #
    # For more details, check with your nearest BA(s).
    @staticmethod
    def validate_subscription(subscription_id: str, service_type: str) -> Tuple[dict, List[ValidationError]]:
        """
        Validates the subscription id exists, and is active for the given service_type and site.
        Returns a list of errors; if no errors, will be empty.

        :param subscription_id:
        :param service_type:
        :return:
        """
        validation_errors: list = []

        subscription: dict = get_subscription(subscription_id, service_type)

        if subscription:
            logger.debug("Subscription found: %s", subscription)
        else:
            validation_errors.append(
                ValidationError("No active subscription found for the supplied site and subscription id"))

        return subscription, validation_errors
