import ssl

import pandas as pd
import requests
import urllib3


def retrieve_json_dump(file_path: str) -> pd.DataFrame:
    ssl._create_default_https_context = ssl._create_unverified_context
    data = pd.read_json(file_path, compression="gzip", lines=True)
    return data


def retrieve_json_dump_list(global_url: str) -> list:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return requests.get(global_url, verify=False).text.split("\n")[:-1]
