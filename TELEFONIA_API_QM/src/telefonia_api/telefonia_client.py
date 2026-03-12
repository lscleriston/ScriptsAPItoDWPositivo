import requests
from requests.auth import HTTPBasicAuth
import json
from typing import List, Dict, Any
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class TelefoniaClient:
    def __init__(self, base_url: str, auth_user: str, auth_pass: str):
        self.base_url = base_url
        self.auth = HTTPBasicAuth(auth_user, auth_pass)
        self.headers = {"Content-Type": "application/json"}

    def get_data(self, queues: str, from_date: str, to_date: str, block: str) -> List[Dict[str, Any]]:
        params = {
            "method": "Stats.get",
            "queues": queues,
            "from": from_date,
            "to": to_date,
            "block": block
        }

        response = requests.get(self.base_url, params=params, headers=self.headers, auth=self.auth, verify=False)
        response.raise_for_status()
        data = response.json()

        # Assuming the response has the data in a list or dict
        # Adapt based on actual API response
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'data' in data:
            return data['data']
        else:
            return [data]  # or handle accordingly