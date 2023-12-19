import requests
import json

url = "https://bwfok9l56k.execute-api.ap-south-1.amazonaws.com/dev/service/lc/load/e67baf7fb8514a378baf08b0b4273856"
headers = {"Content-Type": "application/json"}

for i in range(1, 2):
    payload = json.dumps(
        {
            "site": "AJ0000000{}".format(i),
            # "start_datetime": "2023-12-18T14:30:00+00:00",
            # "end_datetime": "2023-12-18T14:45:00+00:00",
            "status": "ON",
            "switch_addresses": "LG02210255{}".format(i),
            "group_id": "G1",
        }
    )
    response = requests.request("POST", url, headers=headers, data=payload)
    response_dict = json.loads(response.text)
    print(response_dict)
