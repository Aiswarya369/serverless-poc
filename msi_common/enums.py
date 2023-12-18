from enum import Enum


# These needs to extend (str, Enum) to ensure they are JSON serializable
class EventType(str, Enum):
    """
    Event Types
    """
    LOAD_CONTROL = "LOAD_CONTROL"
    BOOST_BUTTON = "BOOST_BUTTON"


class Stage(str, Enum):
    """
    Stages of processes
    """
    RECEIVED = "RECEIVED"
    FAILED = "FAILED"
    DECLINED = "DECLINED"
    QUEUED = "QUEUED"
    POLICY_CREATED = "POLICY_CREATED"
    POLICY_EXTENDED = "POLICY_EXTENDED"
    POLICY_DEPLOYED = "POLICY_DEPLOYED"
    DLC_OVERRIDE_STARTED = "DLC_OVERRIDE_STARTED"
    DLC_OVERRIDE_FINISHED = "DLC_OVERRIDE_FINISHED"
    DLC_OVERRIDE_FAILURE = "DLC_OVERRIDE_FAILURE"
    CANCELLED = "CANCELLED"
    EXTENDED_BY = "EXTENDED_BY"
    EXTENDS = "EXTENDS"


class HeadEnd(str, Enum):
    """
    Head Ends
    """
    POLICYNET = "PolicyNet"
    COMMAND_CENTRE_V4 = "Command Centre v4"
    CONNEXO = "Connexo"
    UIQ = "UIQ"
    MULTIDRIVE_NZ = "Multidrive NZ"
