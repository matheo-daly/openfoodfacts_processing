import pandas as pd
import ssl
import requests


def retrieve_json_dump(file_path: str) -> pd.DataFrame:
    ssl._create_default_https_context = ssl._create_unverified_context
    data = pd.read_json(file_path, compression='gzip', lines=True)
    return data


def retrieve_json_dump_list(global_url: str) -> list:
    return requests.get(global_url, verify=False).text.split("\n")
