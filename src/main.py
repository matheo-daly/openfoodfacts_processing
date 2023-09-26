from src.data_connector.all_connection_getter import get_all_connections
from src.data_retriever.all_data_retriever import get_all_data
from src.data_writer.all_data_writer import write_all_data
from src.utils.dataclasses_utils import Connections, DataRetrieved


def main() -> None:
    all_connections: Connections = get_all_connections()
    all_data_processed: DataRetrieved = get_all_data()

    write_all_data(
        all_connections=all_connections, all_data_retrieved=all_data_processed
    )


if __name__ == "__main__":
    main()
