import pandas as pd
from rich.progress import track

from src.data_processor.food_data_processor import FoodDataProcessor
from src.data_retriever.json_retriever import (retrieve_json_dump,
                                               retrieve_json_dump_list)
from src.utils.dataclasses_utils import DataRetrieved
from src.utils.schemas import FinalProcessedDF


def get_all_data() -> DataRetrieved:
    all_json_food_updates: list = retrieve_json_dump_list(
        global_url="https://static.openfoodfacts.org/data/delta/index.txt"
    )
    list_df: list = []
    for food_update in track(all_json_food_updates):
        raw_data: pd.DataFrame = retrieve_json_dump(
            file_path=f"https://static.openfoodfacts.org/data/delta/{food_update}"
        )
        food_data_processor: FoodDataProcessor = FoodDataProcessor(raw_data=raw_data)
        processed_df: FinalProcessedDF = food_data_processor.processed_df
        list_df.append(processed_df.df)

    all_data_processed_concatenated: FinalProcessedDF = FinalProcessedDF.convert(
        pd.concat(list_df)
    )
    return DataRetrieved(final_food_processed=all_data_processed_concatenated)
