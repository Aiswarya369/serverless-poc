import requests
import json

url = "https://bwfok9l56k.execute-api.ap-south-1.amazonaws.com/dev/service/lc/load/e67baf7fb8514a378baf08b0b4273856"
headers = {"Content-Type": "application/json"}

for i in range(1, 2):
    payload = json.dumps(
        {
            "site": "AJ0000000{}".format(i),
            "start_datetime": "2023-12-14T14:00:00+00:00",
            "end_datetime": "2023-12-14T14:30:00+00:00",
            "status": "ON",
            "switch_addresses": "LG02210255{}".format(i),
            "group_id": "g1",
        }
    )
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response)

# for i in range(4, 6):
#     payload = json.dumps(
#         {
#             "site": "AJ0000000{}".format(i),
#             "start_datetime": "2023-12-14T11:15:00+00:00",
#             "end_datetime": "2023-12-14T11:30:00+00:00",
#             "status": "ON",
#             "switch_addresses": "LG02210255{}".format(i),
#             "group_id": "g1",
#         }
#     )
#     response = requests.request("POST", url, headers=headers, data=payload)
#     print(response)

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
