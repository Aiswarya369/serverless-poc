import requests
import json

url = "https://5vka5woi90.execute-api.ap-south-1.amazonaws.com/dev/service/lc/load/e67baf7fb8514a378baf08b0b4273856"
headers = {"Content-Type": "application/json"}

for i in range(1, 2):
    payload = json.dumps(
        {
            "site": "CNTEST2000XXXXX",
            "start_datetime": "2023-12-19T15:30:00+00:00",
            "end_datetime": "2023-12-19T15:45:00+00:00",
            "status": "ON",
            "switch_addresses": "LG021812235",
        }
    )
    response = requests.request("POST", url, headers=headers, data=payload)
    response_dict = json.loads(response.text)
    print(response_dict)
