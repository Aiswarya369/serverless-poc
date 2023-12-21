import requests
import json

url = "https://tpwutkedr3.execute-api.ap-south-1.amazonaws.com/dev/service/lc/load/e67baf7fb8514a378baf08b0b4273856"
headers = {"Content-Type": "application/json"}

for i in range(3, 4):
    req = {
        "site": "AJ0000000{}".format(i),
        "start_datetime": "2023-12-21T10:45:00+00:00",
        "end_datetime": "2023-12-21T11:00:00+00:00",
        "status": "ON",
        "switch_addresses": "LG02210255{}".format(i),
        # "group_id": "G4",
    }
    payload = json.dumps(req)
    response = requests.request("POST", url, headers=headers, data=payload)
    print(json.dumps(req, indent=4))
    print(response.status_code, response.reason)
    print(json.dumps(response.json(), indent=4))
