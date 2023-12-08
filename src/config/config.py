import os


class AppConfig:
    REGION_NAME = "ap-south-1"
    LOAD_CONTROL_TAGS = "MSI, Load Control"
    DLC_OVERRIDE_SM_ARN = os.environ.get("DLC_OVERRIDE_SM_ARN", None)
    DLC_OVERRIDE_SM_EXECUTION_ARN = os.environ.get("DLC_OVERRIDE_SM_EXECUTION_ARN", None)
    DLC_CANCEL_OVERRIDE_SM_ARN = os.environ.get("DLC_CANCEL_OVERRIDE_SM_ARN", None)
    PNET_AUTH_DETAILS_SECRET_ID = os.environ.get("PNET_AUTH_DETAILS_SECRET_ID", None)
