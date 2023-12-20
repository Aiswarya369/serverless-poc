import datetime
import logging
import os
import boto3

from datetime import datetime, timezone
from logging import Logger
from typing import Tuple, Optional

from boto3.dynamodb.conditions import Key, Attr
from botocore.client import BaseClient


from msi_common import Stage, HeadEnd
from src.model.LCMeterEvent import LCMeterEvent

# from src.lambdas.dlc_event_helper import assemble_event_payload
from src.utils.kinesis_utils import deliver_to_kinesis

KINESIS_DATA_STREAM_NAME: str = os.environ.get("KINESIS_DATA_STREAM_NAME")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: Logger = logging.getLogger(name=__name__)
# Environment lookup; if null, set to INFO level.
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

# Environmental variables.
REGION: str = os.environ.get("REGION", "ap-south-1")
REQUEST_TRACKER_TABLE_NAME: str = os.environ.get("REQUEST_TRACKER_TABLE_NAME", "")

# Boto3 resources.
DYNAMODB_RESOURCE: BaseClient = boto3.resource("dynamodb", region_name=REGION)
REQUEST_TRACKER_TABLE: BaseClient = DYNAMODB_RESOURCE.Table(REQUEST_TRACKER_TABLE_NAME)

# Constants.
LOAD_CONTROL_SERVICE_NAME: str = "load_control"

PK_PREFIX: str = "REQ#"
SK_PREFIX: str = "METADATA#"
SK_REQUEST_PREFIX: str = "METADATA#"
SK_STAGE_PREFIX: str = "STAGES#"
GSI1PK_PREFIX: str = "SITE#"
GSI1SK_PREFIX: str = "REQ#"
GSI2PK_PREFIX: str = "SUBSCRIPTIONID#"
GSI2SK_PREFIX: str = "REQ#"
GSI3PK_PREFIX: str = "SITE#MTR#"
GSI3SK_PREFIX: str = "REQUESTENDDATE#"
GSI4PK_PREFIX: str = "HEADEND#"
GSI4SK_PREFIX: str = "HEADEND_ID#"


class TrackerException(Exception):
    pass


def assemble_event_payload(
    correlation_id: str, stage: Stage, event_datetime: datetime, message: str = ""
) -> dict:
    """
    Assemble event payload.

    :param correlation_id:
    :param stage:
    :param message:
    :param event_datetime:
    :return:
    """
    logger.info(
        "Assembling event payload for correlation id %s, stage %s, event datetime %s, message %s",
        correlation_id,
        stage,
        str(event_datetime),
        message,
    )

    # Get information from tracker record.
    header_record: dict = get_header_record(correlation_id)
    logger.debug(
        "Header record for correlation id %s:\n%s", correlation_id, header_record
    )

    subscription_id: str = header_record["subId"]
    site: str = header_record["site"]
    meter_serial_no: str = header_record["mtrSrlNo"]

    # Assemble payload.
    payload: LCMeterEvent = LCMeterEvent(
        subscription_id=subscription_id,
        correlation_id=correlation_id,
        site=site,
        meter_serial_number=meter_serial_no,
        event_description=message
        if message
        else f"Request moved to stage {stage.value}",
        milestone=stage,
        event_datetime_str=event_datetime.isoformat(timespec="seconds"),
    )

    return payload.as_camelcase_dict()


def create_tracker(
    correlation_id: str,
    sub_id: str,
    request_site: str,
    serial_no: str = None,
    request_start_date: datetime = None,
    request_end_date: datetime = None,
    override: str = None,
    group_id=None,
):
    """
    Create tracker records.

    :param correlation_id: Generated correlation id
    :param sub_id: Subscription id, identified when we validated the subscription
    :param request_site: Site, included in incoming request
    :param serial_no: included in the incoming request
    :param request_start_date: either included in the incoming request or calculated
    :param request_end_date: either included in the incoming request or calculated
    :param override: override direction, either ON of OFF
    :return:
    """
    logger.debug(
        "Creating tracker record for correlation id %s / subscription id %s / site %s",
        correlation_id,
        sub_id,
        request_site,
    )

    now: datetime = datetime.now(timezone.utc)

    header_item: dict = {
        "PK": f"{PK_PREFIX}{correlation_id}",
        "SK": f"{SK_REQUEST_PREFIX}{correlation_id}",
        "GSI1PK": f"{GSI1PK_PREFIX}{request_site}",
        "GSI1SK": f"{GSI1SK_PREFIX}{correlation_id}",
        "GSI2PK": f"{GSI2PK_PREFIX}{sub_id}",
        "GSI2SK": f"{GSI2SK_PREFIX}{correlation_id}",
        "crrltnId": correlation_id,
        "site": request_site,
        "overrdValue": override,
        "subId": sub_id,
        "svcName": LOAD_CONTROL_SERVICE_NAME,
        "noStages": 1,  # This is the number of current detail records.
        "currentStg": Stage.RECEIVED.value,
        "createDt": now.isoformat(),
        "updateDt": now.isoformat(),
    }

    if serial_no:
        header_item["GSI3PK"] = f"{GSI3PK_PREFIX}{request_site}#{serial_no}"
        header_item["mtrSrlNo"] = serial_no

    if request_start_date:
        start: str = request_start_date.isoformat(timespec="seconds")
        header_item["rqstStrtDt"] = start
    if request_end_date:
        header_item["rqstEndDt"] = request_end_date.isoformat(timespec="seconds")

    if override:
        header_item["overrdValue"] = override

    if group_id:
        header_item["group_id"] = group_id

    REQUEST_TRACKER_TABLE.put_item(Item=header_item)

    # Add detail record.
    add_tracker_detail(
        correlation_id=correlation_id,
        sub_id=sub_id,
        request_site=request_site,
        stg_no=1,
        stage=Stage.RECEIVED,
        event_datetime=now,
        message="",
        serial_no=serial_no,
        override=override,
        request_start_date=request_start_date,
        request_end_date=request_end_date,
    )


def get_header_record(correlation_id: str) -> Optional[dict]:
    """
    Get request tracker header record for the supplied correlation_id.
    Will return None if no record found

    :param correlation_id: the request correlation id to use - aligns with tracker Partition Key.
    :return: The header record.
    """
    logger.debug("Getting header record for correlation id %s", correlation_id)

    pk: str = f"{PK_PREFIX}{correlation_id}"
    sk: str = f"{SK_REQUEST_PREFIX}{correlation_id}"

    # Get data from DynamoDB table.
    response: dict = REQUEST_TRACKER_TABLE.get_item(Key={"PK": pk, "SK": sk})
    logger.debug("Response:\n%s", response)

    return response.get("Item")


def get_bulk_header_record(request):
    """
    Get request tracker header record for the supplied correlation_id.
    Will return None if no record found

    :param correlation_id: the request correlation id to use - aligns with tracker Partition Key.
    :return: The header record.
    """
    logger.debug(
        "Getting header record for correlation id %s", request["correlation_id"]
    )
    pk = []
    sk = []
    site_switch_crl_map = {}
    site_switch_crl_ids = request["site_switch_crl_id"]
    for site_switch_crl_id in site_switch_crl_ids:
        site_switch_crl_map[site_switch_crl_id["switch_addresses"]] = site_switch_crl_id
        pk.append(f"{PK_PREFIX}{site_switch_crl_id['correlation_id']}")
        sk.append(f"{SK_REQUEST_PREFIX}{site_switch_crl_id['correlation_id']}")

    items = []
    data_len = len(site_switch_crl_ids)
    if data_len % 100 != 0:
        data_len += 100
    for i in range(int(data_len / 100)):
        last_evaluated_key = False
        while True:
            if last_evaluated_key:
                # In calls after the first (the second page of result data onwards), provide the LastEvaluatedKey
                # which was supplied as part of the previous page's results - specify as ExclusiveStartKey.
                response: dict = REQUEST_TRACKER_TABLE.scan(
                    FilterExpression=Attr("PK").is_in(pk[i * 100 : i * 100 + 100])
                    & Attr("SK").is_in(sk[i * 100 : i * 100 + 100]),
                    ExclusiveStartKey=last_evaluated_key,
                )
            else:
                # This only runs the first time - provide no ExclusiveStartKey initially.
                response: dict = REQUEST_TRACKER_TABLE.scan(
                    FilterExpression=Attr("PK").is_in(pk[i * 100 : i * 100 + 100])
                    & Attr("SK").is_in(sk[i * 100 : i * 100 + 100])
                )
            # Append retrieved records to our result set.
            items.extend(response["Items"])
            if "LastEvaluatedKey" in response:
                last_evaluated_key = response["LastEvaluatedKey"]
                # logger.debug("Last evaluated key: %s - retrieving more records", last_evaluated_key)
            else:
                break
    # logger.debug("Response:\n%s", response)

    for item in items:
        if (
            item["mtrSrlNo"] in site_switch_crl_map
            and item["currentStg"] != Stage.RECEIVED.value
        ):
            site_switch_crl_map.pop(item["mtrSrlNo"])
    return list(site_switch_crl_map.values())


def update_header_record(
    correlation_id: str,
    new_stg_no: int,
    stage: Stage,
    event_datetime: datetime,
    policy_id: int,
    policy_name: str,
    request_start_date: datetime,
    request_end_date: datetime,
    extended_by: str,
    extends: str,
    original_start_datetime=None,
):
    """
    Update tracker header record.

    :param correlation_id: request correlation id
    :param new_stg_no: stage number
    :param stage: current stage (a Stage enum)
    :param event_datetime: event datetime
    :param policy_id: PolicyNet policy id
    :param policy_name: PolicyNet policy name
    :param request_start_date: request start date
    :param request_end_date: request end date
    :param extended_by: correlation id of the request that this request is extended by
    :param extends: correlation id of the request that this request extends
    :return:
    """
    logger.debug("Updating tracker record for correlation id %s", correlation_id)

    pk: str = f"{PK_PREFIX}{correlation_id}"
    sk: str = f"{SK_REQUEST_PREFIX}{correlation_id}"

    update_expression: str = (
        "SET #currentStg = :val1, #noStages = :val2, #updateDt = :val3"
    )

    expression_attribute_names: dict = {
        "#currentStg": "currentStg",
        "#noStages": "noStages",
        "#updateDt": "updateDt",
    }

    expression_attribute_values: dict = {
        ":val1": stage.value,
        ":val2": new_stg_no,
        ":val3": event_datetime.isoformat(),
    }

    # The following may be empty, so only add them to the update statement if required.
    if policy_id:
        update_expression = update_expression + ", #plcyId = :val4"
        expression_attribute_names["#plcyId"] = "plcyId"
        expression_attribute_values[":val4"] = policy_id

        # PolicyNet policy id is used as the partition key of the Global Secondary Index.
        # Other head-ends will presumably use this index too for event start/finish derivation.
        update_expression += ", #GSI4PK = :val5"
        expression_attribute_names["#GSI4PK"] = "GSI4PK"
        expression_attribute_values[":val5"] = f"{GSI4PK_PREFIX}{HeadEnd.POLICYNET}"
        update_expression += ", #GSI4SK = :val6"
        expression_attribute_names["#GSI4SK"] = "GSI4SK"
        expression_attribute_values[":val6"] = f"{GSI4SK_PREFIX}{policy_id}"

    if policy_name:
        update_expression = update_expression + ", #plcyName = :val7"
        expression_attribute_names["#plcyName"] = "plcyName"
        expression_attribute_values[":val7"] = policy_name

    if request_start_date:
        start: str = request_start_date.isoformat(timespec="seconds")

        update_expression += ", #rqstStrtDt = :val8"
        expression_attribute_names["#rqstStrtDt"] = "rqstStrtDt"
        expression_attribute_values[":val8"] = start

    if request_end_date:
        update_expression += ", #rqstEndDt = :val9"
        expression_attribute_names["#rqstEndDt"] = "rqstEndDt"
        expression_attribute_values[":val9"] = request_end_date.isoformat(
            timespec="seconds"
        )

        # Request end date is also used as the sort key of the Global Secondary Index.
        update_expression += ", #GSI3SK = :val10"
        expression_attribute_names["#GSI3SK"] = "GSI3SK"
        expression_attribute_values[
            ":val10"
        ] = f"{GSI3SK_PREFIX}{request_end_date.isoformat()}"

    if extended_by:
        update_expression += ", #extnddBy = :val11"
        expression_attribute_names["#extnddBy"] = "extnddBy"
        expression_attribute_values[":val11"] = extended_by

    if extends:
        update_expression += ", #extnds = :val12"
        expression_attribute_names["#extnds"] = "extnds"
        expression_attribute_values[":val12"] = extends
    if original_start_datetime:
        update_expression += ", #original_start_datetime = :val13"
        expression_attribute_names[
            "#original_start_datetime"
        ] = "original_start_datetime"
        expression_attribute_values[":val13"] = datetime.isoformat(
            original_start_datetime
        )

    REQUEST_TRACKER_TABLE.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
        ConditionExpression="attribute_exists(PK) AND attribute_exists(SK)",
        ReturnValues="UPDATED_NEW",
    )


def update_tracker(
    correlation_id: str,
    stage: Stage,
    event_datetime: datetime,
    message: str = "",
    policy_id: int = None,
    policy_name: str = None,
    request_start_date: datetime = None,
    request_end_date: datetime = None,
    extended_by: str = None,
    extends: str = None,
    original_start_datetime: datetime = None,
):
    """
    Update tracker records.

    :param correlation_id: Generated correlation id
    :param stage: current stage (a Stage enum)
    :param event_datetime: event datetime
    :param message: any message to be recorded
    :param policy_id: PolicyNet policy id
    :param policy_name: PolicyNet policy name
    :param request_start_date: request start date
    :param request_end_date: request end date
    :param extended_by: correlation id of the request that this request is extended by
    :param extends: correlation id of the request that this request extends
    """
    logger.debug("Update tracker: correlation id %s", correlation_id)

    # Get header record.
    header_record = get_header_record(correlation_id)
    if header_record is None:
        raise TrackerException(
            f"Could not find DLC tracker 'header' with correlation id: '{correlation_id}'."
        )

    header_sub_id: str = header_record["subId"]
    header_site: str = header_record["site"]
    header_serial_no: str = header_record["mtrSrlNo"]
    header_override: str = header_record["overrdValue"]

    # Calculate next stage number.
    new_stg_no: int = header_record["noStages"] + 1

    # Update header record.
    update_header_record(
        correlation_id=correlation_id,
        new_stg_no=new_stg_no,
        stage=stage,
        event_datetime=event_datetime,
        policy_id=policy_id,
        policy_name=policy_name,
        request_start_date=request_start_date,
        request_end_date=request_end_date,
        extended_by=extended_by,
        extends=extends,
        original_start_datetime=original_start_datetime,
    )

    # Add detail record.
    add_tracker_detail(
        correlation_id=correlation_id,
        sub_id=header_sub_id,
        request_site=header_site,
        stg_no=new_stg_no,
        stage=stage,
        event_datetime=event_datetime,
        message=message,
        serial_no=header_serial_no,
        override=header_override,
        request_start_date=request_start_date,
        request_end_date=request_end_date,
        policy_id=policy_id,
        policy_name=policy_name,
        extended_by=extended_by,
        extends=extends,
    )


def add_tracker_detail(
    correlation_id: str,
    sub_id: str,
    request_site: str,
    stg_no: int,
    stage: Stage,
    event_datetime: datetime,
    message: str,
    serial_no: str,
    override: str,
    request_start_date: datetime,
    request_end_date: datetime,
    policy_id: int = None,
    policy_name: str = None,
    extended_by: str = None,
    extends: str = None,
):
    """
    Add detail record to request tracker DynamoDB table.

    :param correlation_id: Generated request id
    :param sub_id: Subscription id, identified when we validated the subscription
    :param request_site: Site, included in incoming request
    :param serial_no: meter serial number
    :param override: request override value
    :param stg_no: The stage number to use
    :param stage: The stage to use
    :param event_datetime: event datetime
    :param message: Message, if applicable
    :param policy_id: PolicyNet policy id
    :param policy_name: PolicyNet policy name
    :param request_start_date: request start date
    :param request_end_date: request end date
    :param extended_by: correlation id of the request that this request is extended by
    :param extends: correlation id of the request that this request extends
    """
    logger.debug("Adding tracker detail record for correlation id %s", correlation_id)
    logger.debug(
        "%s / %s / %s / %s / %s / %s",
        sub_id,
        request_site,
        stg_no,
        stage,
        event_datetime,
        message,
    )

    detail_item: dict = {
        "PK": f"{PK_PREFIX}{correlation_id}",
        "SK": f"{SK_STAGE_PREFIX}{stg_no}",
        "GSI1PK": f"{GSI1PK_PREFIX}{request_site}",
        "GSI1SK": f"{GSI1SK_PREFIX}{correlation_id}",
        "GSI2PK": f"{GSI2PK_PREFIX}{sub_id}",
        "GSI2SK": f"{GSI2SK_PREFIX}{correlation_id}",
        "crrltnId": correlation_id,
        "site": request_site,
        "mtrSrlNo": serial_no,
        "overrdValue": override,
        "subId": sub_id,
        "stg": stg_no,
        "stgName": stage.value,
        "createDt": event_datetime.isoformat(),
        "updateDt": event_datetime.isoformat(),
    }

    # Add the following if populated.
    if message:
        detail_item["message"] = message

    if policy_id:
        detail_item["plcyId"] = policy_id

    if policy_name:
        detail_item["plcyName"] = policy_name

    if request_start_date:
        detail_item["rqstStrtDt"] = request_start_date.isoformat(timespec="seconds")

    if request_end_date:
        detail_item["rqstEndDt"] = request_end_date.isoformat(timespec="seconds")

    if extended_by:
        detail_item["extnddBy"] = extended_by

    if extends:
        detail_item["extnds"] = extends

    REQUEST_TRACKER_TABLE.put_item(Item=detail_item)


def bulk_add_tracker_detail(
    data, stage, no_stages, event_datetime, message, policy_id, policy_name
):
    detail_item: dict = {
        "overrdValue": data.get("overrdValue", data.get("status")),
        "stgName": stage.value,
        "createDt": event_datetime.isoformat(),
        "updateDt": event_datetime.isoformat(),
    }
    with REQUEST_TRACKER_TABLE.batch_writer() as batch:
        for item in data["site_switch_crl_id"]:
            correlation_id = item["correlation_id"]
            if stage.value == "EXTENDED_BY":
                correlation_id = item["crrltnId"]
                detail_item[
                    "message"
                ]: str = f"Request {item['crrltnId']} has been extended by request {item['correlation_id']}"
                detail_item["extnddBy"] = item["correlation_id"]
            pk = f"{PK_PREFIX}{correlation_id}"
            detail_item.update(
                {
                    "SK": f"{SK_STAGE_PREFIX}{no_stages[pk]}",
                    "stg": no_stages[pk],
                    "PK": pk,
                    "GSI1PK": f"{GSI1PK_PREFIX}{item['site']}",
                    "GSI1SK": f"{GSI1SK_PREFIX}{correlation_id}",
                    "GSI2PK": f"{GSI2PK_PREFIX}{item['sub_id']}",
                    "GSI2SK": f"{GSI2SK_PREFIX}{correlation_id}",
                    "crrltnId": correlation_id,
                    "mtrSrlNo": item["switch_addresses"],
                    "subId": item["sub_id"],
                }
            )
            if message:
                detail_item["message"] = message

            if policy_id:
                detail_item["plcyId"] = policy_id

            if policy_name:
                detail_item["plcyName"] = policy_name
            if stage.value == "EXTENDS":
                detail_item[
                    "message"
                ]: str = f"Request {item['correlation_id']} extends request {item['crrltnId']}"
                detail_item["extnds"] = item["crrltnId"]

            batch.put_item(Item=detail_item.copy())
            if stage.value not in [
                Stage.POLICY_CREATED.value,
                Stage.POLICY_EXTENDED.value,
                Stage.POLICY_DEPLOYED.value,
            ]:
                payload = assemble_event_payload(
                    item["correlation_id"], stage, event_datetime, message
                )
                deliver_to_kinesis(payload, KINESIS_DATA_STREAM_NAME)


def bulk_update_header_records(
    data,
    stage: Stage,
    event_datetime: datetime,
    policy_id: int = None,
    policy_name: str = None,
    original_start_datetime=None,
    request_start_date=None,
    request_end_date=None,
):
    pk = []
    sk = []
    no_stages = {}
    site_switch_crl_ids = {}
    for site_switch_crl_id in data["site_switch_crl_id"]:
        if stage.value == "EXTENDED_BY":
            correlation_id = site_switch_crl_id["crrltnId"]
        else:
            correlation_id = site_switch_crl_id["correlation_id"]
        site_switch_crl_ids[site_switch_crl_id["switch_addresses"]] = site_switch_crl_id
        pk.append(f"{PK_PREFIX}{correlation_id}")
        sk.append(f"{SK_REQUEST_PREFIX}{correlation_id}")
    table: BaseClient = DYNAMODB_RESOURCE.Table(REQUEST_TRACKER_TABLE_NAME)
    items = []
    data_len = len(data["site_switch_crl_id"])
    if data_len % 100 != 0:
        data_len += 100
    for i in range(int(data_len / 100)):
        last_evaluated_key = False
        while True:
            if last_evaluated_key:
                # In calls after the first (the second page of result data onwards), provide the LastEvaluatedKey
                # which was supplied as part of the previous page's results - specify as ExclusiveStartKey.
                response: dict = table.scan(
                    FilterExpression=Attr("PK").is_in(pk[i * 100 : i * 100 + 100])
                    & Attr("SK").is_in(sk[i * 100 : i * 100 + 100]),
                    ExclusiveStartKey=last_evaluated_key,
                )
            else:
                # This only runs the first time - provide no ExclusiveStartKey initially.
                response: dict = table.scan(
                    FilterExpression=Attr("PK").is_in(pk[i * 100 : i * 100 + 100])
                    & Attr("SK").is_in(sk[i * 100 : i * 100 + 100])
                )
            # Append retrieved records to our result set.
            items.extend(response["Items"])
            if "LastEvaluatedKey" in response:
                last_evaluated_key = response["LastEvaluatedKey"]
                # logger.debug("Last evaluated key: %s - retrieving more records", last_evaluated_key)
            else:
                break
    with table.batch_writer() as batch:
        for item in items:
            no_stages[item["PK"]] = int(item["noStages"] + 1)
            item.update(
                {
                    "currentStg": stage.value,
                    "noStages": int(item["noStages"] + 1),
                    "updateDt": event_datetime.isoformat(),
                }
            )

            if policy_id:
                item.update(
                    {
                        "plcyId": policy_id,
                        "GSI4PK": f"{GSI4PK_PREFIX}{HeadEnd.POLICYNET}",
                        "GSI4SK": f"{GSI4SK_PREFIX}{policy_id}",
                        "plcyName": policy_name,
                    }
                )
            if stage.value == "EXTENDS":
                item["extnds"] = site_switch_crl_ids[item["mtrSrlNo"]]["crrltnId"]
            if stage.value == "EXTENDED_BY":
                item["extnddBy"] = site_switch_crl_ids[item["mtrSrlNo"]][
                    "correlation_id"
                ]
            if original_start_datetime:
                original_start = datetime.isoformat(original_start_datetime)
                item["original_start_datetime"] = original_start
            if request_start_date:
                start_date = datetime.isoformat(request_start_date)
                item["rqstStrtDt"] = start_date
            if request_end_date:
                end_date = datetime.isoformat(request_end_date)
                item["rqstEndDt"] = end_date
                item["GSI3SK"] = f"{GSI3SK_PREFIX}{end_date}"

            batch.put_item(Item=item)
    return no_stages


def bulk_update_records(
    records,
    stage: Stage,
    event_datetime: datetime,
    policy_id: int = None,
    policy_name: str = None,
    message: str = "",
    original_start_datetime=None,
    request_start_date=None,
    request_end_date=None,
):
    no_stages = bulk_update_header_records(
        records,
        stage,
        event_datetime,
        policy_id,
        policy_name,
        original_start_datetime=original_start_datetime,
        request_start_date=request_start_date,
        request_end_date=request_end_date,
    )
    bulk_add_tracker_detail(
        records, stage, no_stages, event_datetime, message, policy_id, policy_name
    )


def get_terminal_request(request: dict) -> dict:
    """
    Get terminal request for the supplied request.

    Return the terminal request in the "extends"/"extended by" chain or the supplied request
    if there is not a terminal request.

    :param request: the Load Control override request to use to get the last request in the chain
    :return: a dict containing the last request in the chain that the supplied request is in
    """
    # Default the result value to be the supplied request.
    terminal_request: dict = request

    while True:
        # Quit if this request is not extending another.
        if "extnds" not in terminal_request:
            logger.debug(
                "Request %s does not extend another - exiting",
                terminal_request["crrltnId"],
            )
            break

        correlation_id: str = terminal_request["extnds"]
        logger.debug("Request extends correlation id %s", correlation_id)

        # Get data from DynamoDB table.
        response: dict = REQUEST_TRACKER_TABLE.get_item(
            Key={
                "PK": f"{PK_PREFIX}{correlation_id}",
                "SK": f"{SK_PREFIX}{correlation_id}",
            }
        )
        logger.debug("Response:\n%s", response)

        if "Item" in response:
            terminal_request = response["Item"]
            logger.info(
                "New candidate for terminal request: %s", terminal_request["crrltnId"]
            )
        else:
            message: str = f"Correlation id {correlation_id} not found when getting terminal request"
            raise RuntimeError(message)

    return terminal_request


def group_contiguous_requests(contiguous_request, current_request):
    """
    Group the contiguous requests further into sub groups based on the terminal request start datetime which is original_start_datetime and overrdValue.

    :param : contiguous and the current request.
    :returns: list of grouped requests.
    """
    contiguous_request_list = (
        contiguous_request if not isinstance(contiguous_request, str) else []
    )
    current_request_list = current_request.get("site_switch_crl_id", [])

    contiguous_request_dict = {
        record["mtrSrlNo"]: record for record in contiguous_request_list
    }

    request_data = {
        "action": "createDLCPolicy",
        "group_id": current_request.get("group_id"),
        "status": current_request.get("status"),
        "start_datetime": current_request.get("start_datetime"),
        "end_datetime": current_request.get("end_datetime"),
        "site": [],
        "switch_addresses": [],
        "correlation_id": [],
        "site_switch_crl_id": [],
    }
    grouped_data = {}

    for req_record in current_request_list:
        switch_addresses = req_record.get("switch_addresses")
        existing_record = contiguous_request_dict.get(switch_addresses)

        if existing_record:
            key = (
                current_request.get("group_id"),
                current_request.get("status"),
                existing_record.get("overrdValue"),
                existing_record.get("original_start_datetime"),
                existing_record.get("rqstStrtDt"),
            )
            if key not in grouped_data:
                if current_request["status"] == existing_record["overrdValue"]:
                    policyType = "contiguousExtension"
                else:
                    policyType = "contiguousCreation"

                grouped_data[key] = {
                    "action": "createDLCPolicy",
                    "policyType": policyType,
                    "group_id": current_request.get("group_id"),
                    "status": current_request.get("status"),
                    "start_datetime": current_request.get("start_datetime"),
                    "end_datetime": current_request.get("end_datetime"),
                    "original_start_datetime": existing_record.get(
                        "original_start_datetime"
                    ),
                    "rqstStrtDt": existing_record.get("rqstStrtDt"),
                    "rqstEndDt": existing_record.get("rqstEndDt"),
                    "site": [req_record.get("site")],
                    "switch_addresses": [req_record.get("switch_addresses")],
                    "correlation_id": [req_record.get("correlation_id")],
                    "crrltnId": [existing_record.get("crrltnId")],
                    "site_switch_crl_id": [
                        {
                            "site": req_record.get("site"),
                            "correlation_id": req_record.get("correlation_id"),
                            "switch_addresses": req_record.get("switch_addresses"),
                            "crrltnId": existing_record.get("crrltnId"),
                            "sub_id": req_record.get("sub_id"),
                        }
                    ],
                }

            else:
                site = req_record.get("site")
                correlation_id = req_record.get("correlation_id")
                switch_addresses = req_record.get("switch_addresses")
                crrltn_id = existing_record.get("crrltnId")
                grouped_data[key]["site"].append(site)
                grouped_data[key]["switch_addresses"].append(switch_addresses)
                grouped_data[key]["correlation_id"].append(correlation_id)
                grouped_data[key]["crrltnId"].append(crrltn_id)
                grouped_data[key]["site_switch_crl_id"].append(
                    {
                        "site": site,
                        "correlation_id": correlation_id,
                        "switch_addresses": switch_addresses,
                        "crrltnId": crrltn_id,
                        "sub_id": req_record.get("sub_id"),
                    }
                )
        else:
            site = req_record.get("site")
            correlation_id = req_record.get("correlation_id")
            switch_addresses = req_record.get("switch_addresses")
            request_data["site"].append(site)
            request_data["switch_addresses"].append(switch_addresses)
            request_data["correlation_id"].append(correlation_id)
            request_data["site_switch_crl_id"].append(
                {
                    "site": site,
                    "correlation_id": correlation_id,
                    "switch_addresses": switch_addresses,
                    "sub_id": req_record.get("sub_id"),
                }
            )

    return [request_data] if len(request_data["site"]) > 1 else [], list(
        grouped_data.values()
    )


def is_request_pending_state_machine(correlation_id: str) -> bool:
    """
    Determines if the request is in a final state and or in progress state.

    :param correlation_id: Correlation ID of a given DLC request.
    :returns: False if the request exists and is not in an in progress or final step. Otherwise, True.
    """
    header_record = get_header_record(correlation_id)
    if header_record is None:
        return True

    return header_record["currentStg"] == Stage.RECEIVED.value


def bulk_is_request_pending_state_machine(requests):
    """
    Determines if the request is in a final state and or in progress state.

    :param correlation_id: Correlation ID of a given DLC request.
    :returns: False if the request exists and is not in an in progress or final step. Otherwise, True.
    """
    site_switch_crl_id = []
    site = []
    switch_addresses = []
    correlation_id = []

    site_switch_crl_ids = get_bulk_header_record(requests)

    if site_switch_crl_ids:
        for item in site_switch_crl_ids:
            site_switch_crl_id.append(item)
            site.append(item["site"])
            switch_addresses.append(item["switch_addresses"])
            correlation_id.append(item["correlation_id"])
        requests["site_switch_crl_id"] = site_switch_crl_id
        requests["site"] = site
        requests["switch_addresses"] = switch_addresses
        requests["correlation_id"] = correlation_id
        return True
    return False


def get_contiguous_request(request: dict) -> Tuple[dict, dict]:
    """
    Get any Load Control requests which have already been deployed that are contiguous to the supplied request.

    You must do a separate check to see if the contiguous request is of the same switch direction or not by checking
    if the requests status is equal to the contiguous request status.

    Matches on:
    - site and meter serial number
    - end date of previous request = this request's start date

    :param request: the Load Control override request to use in order to determine contiguousness
    :return: a tuple containing:
    1. The request contiguous to this one;
    2. The terminal (first or last) request in the chain.
    """
    ddb_table: BaseClient = DYNAMODB_RESOURCE.Table(REQUEST_TRACKER_TABLE_NAME)

    contiguous_request: Optional[dict] = None
    terminal_request: Optional[dict] = None

    site: str = request["site"]
    start_datetime: str = request["start_datetime"]

    # There should only be one MSN supplied in switch_addresses.
    switch_addresses = request["switch_addresses"]
    meter_serial_number: str = (
        switch_addresses[0] if type(switch_addresses) == list else switch_addresses
    )

    logger.info(
        "Looking for contiguous LC requests: %s / %s / %s",
        site,
        meter_serial_number,
        start_datetime,
    )

    items: list = []
    last_evaluated_key: Optional[dict] = None

    # Requests for:
    # - the same site and meter serial number
    # - service: Load Control
    # - the latest status is POLICY_CREATED, POLICY_EXTENDED, POLICY_DEPLOYED or DLC_OVERRIDE_STARTED
    # - end date of period = this request's start date
    gsi3pk: str = f"{GSI3PK_PREFIX}{site}#{meter_serial_number}"
    gsi3sk: str = f"{GSI3SK_PREFIX}{start_datetime}"  # Should be in ISO format.

    stages: list = [
        Stage.POLICY_CREATED.value,
        Stage.POLICY_EXTENDED.value,
        Stage.POLICY_CREATED.value,
        Stage.POLICY_DEPLOYED.value,
        Stage.DLC_OVERRIDE_STARTED.value,
        Stage.EXTENDED_BY.value,  # needed for recognising contiguous requests to a request we are cancelling
    ]

    while True:
        if last_evaluated_key:
            # In calls after the first (the second page of result data onwards), provide the LastEvaluatedKey
            # which was supplied as part of the previous page's results - specify as ExclusiveStartKey.
            response: dict = ddb_table.query(
                IndexName="GSI3",
                KeyConditionExpression=Key("GSI3PK").eq(gsi3pk)
                & Key("GSI3SK").eq(gsi3sk),  # GSI3SK holds the request end date.
                FilterExpression=Attr("svcName").eq(LOAD_CONTROL_SERVICE_NAME)
                & Attr("currentStg").is_in(stages),
                ExclusiveStartKey=last_evaluated_key,
            )
        else:
            # This only runs the first time - provide no ExclusiveStartKey initially.
            response: dict = ddb_table.query(
                IndexName="GSI3",
                KeyConditionExpression=Key("GSI3PK").eq(gsi3pk)
                & Key("GSI3SK").eq(gsi3sk),  # GSI3SK holds the request end date.
                FilterExpression=Attr("svcName").gte(LOAD_CONTROL_SERVICE_NAME)
                & Attr("currentStg").is_in(stages),
            )

        # Append retrieved records to our result set.
        items.extend(response["Items"])

        # Set our LastEvaluatedKey to the value for next operation if there is one.
        # Otherwise, there's no more results; we can exit.
        if "LastEvaluatedKey" in response:
            last_evaluated_key = response["LastEvaluatedKey"]
            logger.debug(
                "Last evaluated key: %s - retrieving more records", last_evaluated_key
            )
        else:
            break

    if items:
        if len(items) == 1:
            contiguous_request = items[0]

            logger.info("Contiguous request %s found", contiguous_request["crrltnId"])

            # If the contiguous request is of the opposite switch direction, then we don't need to get the terminal
            # request.
            if request["status"] != contiguous_request["overrdValue"]:
                logger.info("Contiguous request is of the opposite switch direction")
                terminal_request = contiguous_request
                return contiguous_request, terminal_request

            logger.info("Identifying terminal request")
            terminal_request: dict = get_terminal_request(contiguous_request)
        else:
            message: str = (
                f"More than one contiguous Load Control request found for site {site}, "
                f"meter {meter_serial_number}, start {start_datetime}"
            )

            raise RuntimeError(message)
    else:
        logger.info("No contiguous requests found")

    return contiguous_request, terminal_request


def get_bulk_contiguous_request(request: dict):
    """
    Get any Load Control requests which have already been deployed that are contiguous to the supplied request.

    You must do a separate check to see if the contiguous request is of the same switch direction or not by checking
    if the requests status is equal to the contiguous request status.

    Matches on:
    - site and meter serial number
    - end date of previous request = this request's start date

    :param request: the Load Control override request to use in order to determine contiguousness
    :return: a tuple containing:
    1. The request contiguous to this one;
    2. The terminal (first or last) request in the chain.
    """
    ddb_table: BaseClient = DYNAMODB_RESOURCE.Table(REQUEST_TRACKER_TABLE_NAME)

    site = request["site"]
    start_datetime: str = request["start_datetime"]

    switch_addresses = request["switch_addresses"]

    items: list = []
    last_evaluated_key: Optional[dict] = None

    # Requests for:
    # - the same site and meter serial number
    # - service: Load Control
    # - the latest status is POLICY_CREATED, POLICY_EXTENDED, POLICY_DEPLOYED or DLC_OVERRIDE_STARTED
    # - end date of period = this request's start date
    if not isinstance(switch_addresses, str):
        item_len = len(request["site_switch_crl_id"])
        if item_len % 100 != 0:
            item_len += 100
        site_switch_crl_id = request["site_switch_crl_id"]
        gsi3pk = list(
            map(
                lambda x: f"{GSI3PK_PREFIX}{x['site']}#{x['switch_addresses']}",
                site_switch_crl_id,
            )
        )
    else:
        item_len = 100
        gsi3pk = [f"{GSI3PK_PREFIX}{site}#{switch_addresses}"]

    gsi3sk: str = f"{GSI3SK_PREFIX}{start_datetime}"  # Should be in ISO format.

    stages: list = [
        Stage.POLICY_CREATED.value,
        Stage.POLICY_EXTENDED.value,
        Stage.POLICY_DEPLOYED.value,
        Stage.DLC_OVERRIDE_STARTED.value,
        Stage.EXTENDED_BY.value,  # needed for recognising contiguous requests to a request we are cancelling
    ]
    for i in range(int(item_len / 100)):
        while True:
            if last_evaluated_key:
                # In calls after the first (the second page of result data onwards), provide the LastEvaluatedKey
                # which was supplied as part of the previous page's results - specify as ExclusiveStartKey.
                response: dict = ddb_table.scan(
                    TableName=REQUEST_TRACKER_TABLE_NAME,
                    FilterExpression=Key("GSI3SK").eq(gsi3sk)
                    & Attr("GSI3PK").is_in(gsi3pk[i * 100 : i * 100 + 100])
                    & Attr("svcName").eq(LOAD_CONTROL_SERVICE_NAME)
                    & Attr("currentStg").is_in(stages),
                    ExclusiveStartKey=last_evaluated_key,
                )
            else:
                # This only runs the first time - provide no ExclusiveStartKey initially.
                response: dict = ddb_table.scan(
                    IndexName="GSI3",
                    FilterExpression=Key("GSI3SK").eq(gsi3sk)
                    & Attr("GSI3PK").is_in(gsi3pk[i * 100 : i * 100 + 100])
                    & Attr("svcName").gte(LOAD_CONTROL_SERVICE_NAME)
                    & Attr("currentStg").is_in(stages),
                )

            # Append retrieved records to our result set.
            items.extend(response["Items"])

            # Set our LastEvaluatedKey to the value for next operation if there is one.
            # Otherwise, there's no more results; we can exit.
            if "LastEvaluatedKey" in response:
                last_evaluated_key = response["LastEvaluatedKey"]
                logger.debug(
                    "Last evaluated key: %s - retrieving more records",
                    last_evaluated_key,
                )
            else:
                break

    if not items:
        logger.info("No contiguous requests found")
        request["action"] = "createDLCPolicy"
        return [request]
    if "site_switch_crl_id" not in request:
        contiguous_request = items[0]
        contiguous_request.update(
            {
                "correlation_id": request["correlation_id"],
                "start_datetime": request["start_datetime"],
                "end_datetime": request["end_datetime"],
                "status": request["status"],
                "switch_addresses": request["switch_addresses"],
                "action": "createDLCPolicy",
            }
        )
        if request["status"] == request["overrdValue"]:
            request["action"] = "contiguousExtension"
        else:
            request["action"] = "contiguousCreation"
        return [contiguous_request]

    request, contiguous_requests = group_contiguous_requests(items, request)
    request = contiguous_requests + request
    logger.info("Sub grouped request : %s", request)

    return request


if __name__ == "__main__":
    import uuid

    main_subscription_id = f"test-subscription-{str(uuid.uuid4())}"
    main_site = f"TEST_NMI_{str(uuid.uuid4())}"
    main_msn = "METER_A"
    main_override_value = "ON"

    # 10-11
    main_merged_to_correlation_id = f"test-{str(uuid.uuid4())}"
    main_merged_to_request_start_date = datetime.fromisoformat(
        "2022-06-09T10:00:00+10:00"
    )
    main_merged_to_request_end_date = datetime.fromisoformat(
        "2022-06-09T11:00:00+10:00"
    )
    print("Merge to correlation id: ", main_merged_to_correlation_id)

    # 9-10
    main_merged_from_correlation_id = f"test-{str(uuid.uuid4())}"
    main_merged_from_request_start_date = datetime.fromisoformat(
        "2022-06-09T09:00:00+10:00"
    )
    main_merged_from_request_end_date = datetime.fromisoformat(
        "2022-06-09T10:00:00+10:00"
    )
    print("Merge from correlation id: ", main_merged_from_correlation_id)

    # Resulting 9-11
    main_result_request_start_date = datetime.fromisoformat("2022-06-09T09:00:00+10:00")
    main_result_request_end_date = datetime.fromisoformat("2022-06-09T11:00:00+10:00")

    # Create merged to and merged from metadata and RECEIVED records
    create_tracker(
        correlation_id=main_merged_to_correlation_id,
        sub_id=main_subscription_id,
        request_site=main_site,
        serial_no=main_msn,
        request_start_date=main_merged_to_request_start_date,
        request_end_date=main_merged_to_request_end_date,
        override=main_override_value,
    )

    create_tracker(
        correlation_id=main_merged_from_correlation_id,
        sub_id=main_subscription_id,
        request_site=main_site,
        serial_no=main_msn,
        request_start_date=main_merged_from_request_start_date,
        request_end_date=main_merged_from_request_end_date,
        override=main_override_value,
    )

    # Add QUEUED stage to merged to and merged from
    update_tracker(
        correlation_id=main_merged_to_correlation_id,
        stage=Stage.QUEUED,
        event_datetime=datetime.now(timezone.utc),
        request_start_date=main_merged_to_request_start_date,
        request_end_date=main_merged_to_request_end_date,
    )

    update_tracker(
        correlation_id=main_merged_from_correlation_id,
        stage=Stage.QUEUED,
        event_datetime=datetime.now(timezone.utc),
        request_start_date=main_merged_from_request_start_date,
        request_end_date=main_merged_from_request_end_date,
    )

    # Add QUEUED to merged from
    update_tracker(
        correlation_id=main_merged_from_correlation_id,
        stage=Stage.QUEUED,
        event_datetime=datetime.now(timezone.utc),
        request_start_date=main_result_request_start_date,
        request_end_date=main_result_request_end_date,
    )
