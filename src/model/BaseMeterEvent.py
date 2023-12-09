import dataclasses

from dataclasses import dataclass
from typing import Optional, Any

from src.model.enums import EventType, Stage


@dataclass
class BaseMeterEvent:
    """
    A Generic Base Meter Event that can be extended.
    The method 'as_camelcase_dict' can be used to ensure we return consistent payloads
    """
    event_type: EventType
    event_description: str
    meter_serial_number: str
    event_datetime_str: str
    event_value: Optional[str] = None
    milestone: Optional[Stage] = None
    subscription_id: Optional[str] = None
    correlation_id: Optional[str] = None
    site: Optional[str] = None
    register_id: Optional[str] = None
    tracked: Optional[bool] = None
    headend: Optional[str] = None
    headend_id: Optional[str] = None

    def as_dict(self) -> dict:
        """
        Use dataclass.asdict to convert object to dict

        :return: object as a dictionary
        """
        return dataclasses.asdict(self)

    def as_camelcase_dict(self) -> dict:
        """
        Create a generic event response using camelCase keys

        :return: object as a dictionary with camelCase keys
        """
        payload: dict = {}

        self._add_field(payload, "eventType", self.event_type)
        self._add_field(payload, "eventValue", self.event_value)
        self._add_field(payload, "eventDescription", self.event_description)
        self._add_field(payload, "milestone", self.milestone)
        self._add_field(payload, "subscriptionId", self.subscription_id)
        self._add_field(payload, "correlationId", self.correlation_id)
        self._add_field(payload, "meterSerialNumber", self.meter_serial_number)
        self._add_field(payload, "site", self.site)
        self._add_field(payload, "registerId", self.register_id)
        self._add_field(payload, "eventDatetime", self.event_datetime_str)

        return payload

    @staticmethod
    def _add_field(payload: dict, field: str, value: Any):
        """
        Only add a field to the given dict if it exists

        :param payload: dict to add field to
        :param field: field to add
        :param value: value to add
        """
        if value:
            payload[field] = value
