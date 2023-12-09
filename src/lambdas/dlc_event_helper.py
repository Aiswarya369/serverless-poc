import logging
import os

from logging import Logger
from datetime import datetime
from typing import List

from src.model.enums import Stage

from src.model.LCMeterEvent import LCMeterEvent
from src.utils.request_validator import ValidationError
from src.utils.tracker_utils import get_header_record

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger: Logger = logging.getLogger(name=__name__)
# Environment lookup; if null, set to INFO level.
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))


def assemble_event_payload(correlation_id: str, stage: Stage, event_datetime: datetime, message: str = "") -> dict:
    """
    Assemble event payload.

    :param correlation_id:
    :param stage:
    :param message:
    :param event_datetime:
    :return:
    """
    logger.info("Assembling event payload for correlation id %s, stage %s, event datetime %s, message %s",
                correlation_id, stage, str(event_datetime), message)

    # Get information from tracker record.
    header_record: dict = get_header_record(correlation_id)
    logger.debug("Header record for correlation id %s:\n%s", correlation_id, header_record)

    subscription_id: str = header_record["subId"]
    site: str = header_record["site"]
    meter_serial_no: str = header_record["mtrSrlNo"]

    # Assemble payload.
    payload: LCMeterEvent = LCMeterEvent(
        subscription_id=subscription_id,
        correlation_id=correlation_id,
        site=site,
        meter_serial_number=meter_serial_no,
        event_description=message if message else f"Request moved to stage {stage.value}",
        milestone=stage,
        event_datetime_str=event_datetime.isoformat(timespec='seconds')
    )

    return payload.as_camelcase_dict()


def assemble_error_message(errors: List[ValidationError]) -> str:
    """
    Assemble error message from a list of validation errors.

    :param errors:
    :return:
    """
    return "; ".join([e.error for e in errors])
