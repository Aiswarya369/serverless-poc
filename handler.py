import requests
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

url = "https://rt2tajh0a9.execute-api.ap-south-1.amazonaws.com/dev/service/lc/load/e67baf7fb8514a378baf08b0b4274002"
headers = {"Content-Type": "application/json"}

for i in range(10, 15):
    req = {
        "site": "AJ0000000{}".format(i),
        # "start_datetime": "2024-01-10T08:20:00+00:00",
        # "end_datetime": "2024-01-10T08:25:00+00:00",
        "status": "ON",
        "switch_addresses": "LG02210255{}".format(i),
        "group_id": "G5",
    }
    payload = json.dumps(req)
    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.status_code, response.reason)


# DYNAMODB_RESOURCE = boto3.resource(
#     "dynamodb",
#     region_name="ap-south-1",
#     aws_access_key_id="AKIA26PVSQS56S7BRWW7",
#     aws_secret_access_key="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",
# )

# REQUEST_TRACKER_TABLE = DYNAMODB_RESOURCE.Table("request-tracker-ddb")

# response: dict = REQUEST_TRACKER_TABLE.query(
#     IndexName="GSI3",
#     KeyConditionExpression=Key("GSI3PK").eq("SITE#MTR#2024-01-10T07:25:00+00:00"),
#     FilterExpression=Attr("PK").is_in(
#         "REQ#AJ00000005-2024-01-10T173401-437d0617-4cd0-4970-9be2-01a819f2d855"
#     )
#     & Attr("SK").is_in(
#         "METADATA#4103861111-2022-08-16T124000-95cc3def-21fc-41d5-8a05-0ab692064067"
#     ),
# )

# print(response)
