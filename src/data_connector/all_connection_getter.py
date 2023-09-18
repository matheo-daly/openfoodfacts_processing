from src.data_connector.google_services_connection_getter import \
    GoogleServicesConnectionGetter
from src.utils.dataclasses_utils import Connections


def get_all_connections() -> Connections:
    google_service_connections = GoogleServicesConnectionGetter(
        service_account_json_key="/Users/MATHEO/Documents/Recipe App/keys/trusty-obelisk-343020-e24e028d01d0.json"
    )
    bigquery_connection = google_service_connections.get_bigquery_connection()

    return Connections(google_bigquery=bigquery_connection)
