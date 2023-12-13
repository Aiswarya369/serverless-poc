import requests
import json

url = "https://px1srwr4ab.execute-api.ap-south-1.amazonaws.com/dev/service/lc/load/e67baf7fb8514a378baf08b0b4273856"
headers = {"Content-Type": "application/json"}

for i in range(1, 3):
    payload = json.dumps(
        {
            "site": "AJ0000000{}".format(i),
            "start_datetime": "2023-12-13T09:00:00+00:00",
            "end_datetime": "2023-12-13T09:30:00+00:00",
            "status": "ON",
            "switch_addresses": "LG02210255{}".format(i),
            "group_id": "g1",
        }
    )
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response)

# payload = json.dumps(
#     {
#         "site": "AJ000000012",
#         "start_datetime": "2023-12-12T13:00:00+00:00",
#         "end_datetime": "2023-12-12T13:30:00+00:00",
#         "status": "ON",
#         "switch_addresses": "LG0221025512",
#     }
# )
# response = requests.request("POST", url, headers=headers, data=payload)
# print(response)
