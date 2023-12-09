from src.model.enums import EventType, Stage
from src.model.BaseMeterEvent import BaseMeterEvent

class LCMeterEvent(BaseMeterEvent):
    """
    Load Control Meter Event to place on Kinesis data stream.

    :param meter_serial_number: Load Control override request's meter serial number.
    :param subscription_id: Load Control override request's subscription id.
    :param correlation_id: Load Control override request's correlation id.
    :param site: Load Control override request's site.
    :param event_description: event description.
    :param milestone: event milestone.
    :param event_datetime_str: event datetime
    """
    def __init__(self, meter_serial_number: str, subscription_id: str, correlation_id: str, site: str, event_description: str,
                 milestone: Stage, event_datetime_str: str):
        super().__init__(
            event_type=EventType.LOAD_CONTROL,
            event_description=event_description,
            meter_serial_number=meter_serial_number,
            event_datetime_str=event_datetime_str,
            event_value=None,
            milestone=milestone,
            subscription_id=subscription_id,
            correlation_id=correlation_id,
            site=site,
            register_id=None,
            tracked=None
            )
