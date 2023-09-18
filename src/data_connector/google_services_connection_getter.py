import logging

from google.cloud import bigquery

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class GoogleServicesConnectionGetter:
    def __init__(self, service_account_json_key: str = None):
        self.service_account_json_key = service_account_json_key

    def get_bigquery_connection(self) -> bigquery.Client:
        f"""
        Get a big query client instantiated.


        :return: a big query client object that connects to the bigquery api

        """
        bigquery_service = bigquery.Client.from_service_account_json(
            json_credentials_path=self.service_account_json_key
        )
        logger.info("bigquery connection established successfully !")
        return bigquery_service
