from dataclasses import dataclass
from typing import Union

from google.cloud import bigquery

from src.utils.schemas import FinalProcessedDF


@dataclass(frozen=True)
class Connections:
    google_bigquery: Union[bigquery.Client, None]


@dataclass(frozen=True)
class DataRetrieved:
    final_food_processed: Union[FinalProcessedDF, None]
