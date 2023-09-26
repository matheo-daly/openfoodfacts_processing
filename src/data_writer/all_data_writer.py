from src.data_writer.bigquery_writer import write_to_bigquery
from src.utils.dataclasses_utils import Connections, DataRetrieved
from src.utils.schemas import FinalProcessedDF


def write_all_data(all_connections: Connections, all_data_retrieved: DataRetrieved):
    bigquery_connection = all_connections.google_bigquery
    food_processed: FinalProcessedDF = all_data_retrieved.final_food_processed
    write_to_bigquery(
        client=bigquery_connection,
        full_table_path="trusty-obelisk-343020.openfoodfacts.openfoodfacts_filtered",
        dataframe=food_processed.df,
        overwrite_fields=True,
        overwrite_fields_columns="code",
    )
