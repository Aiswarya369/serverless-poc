from enum import Enum


class SupportedOverrideSMActions(Enum):
    CREATE_DLC_POLICY = "createDLCPolicy"
    DEPLOY_DLC_POLICY = "deployDLCPolicy"
    LOGOUT_PNET = "logoutPolicyNet"

    # These two values are not in use currently.
    UNDEPLOY_DLC_POLICY = "undeployDLCPolicy"
    DELETE_DLC_POLICY = "deleteDLCPolicy"


class SupportedCancelOverrideSMActions(Enum):
    EVALUATE_REQUEST = "evaluateRequest"
    CREATE_REPLACEMENT_POLICY = "createReplacementPolicy"
    DEPLOY_REPLACEMENT_POLICY = "deployReplacementPolicy"
    CREATE_NEW_POLICY = "createNewPolicy"
    DEPLOY_NEW_POLICY = "deployNewPolicy"
    UNDEPLOY_POLICY = "undeployPolicy"
    CANCEL_POLICY = "cancelPolicy"
    CANCELLATION_COMPLETE = "cancellationComplete"
    FAILURE = "failure"
