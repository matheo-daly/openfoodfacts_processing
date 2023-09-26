import logging
from typing import Union

import pandas as pd
from google.cloud import bigquery

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def write_to_bigquery(
    full_table_path: str,
    client: bigquery.Client,
    dataframe: pd.DataFrame,
    overwrite_all: bool = False,
    overwrite_fields: bool = False,
    overwrite_fields_columns: Union[str, None] = None,
) -> None:
    f"""
    write a dataframe into a bigquery table
    :param overwrite_all: if true, overwrite completely the table
    :param full_table_path: full name of the dataset and the table where data needs to be written
    :param client : bigquery client instantiated
    :param dataframe: dataframe that needs to be written to bigquery
    :param overwrite_fields: if true, delete rows with specified fields before inserting new ones
    :param overwrite_fields_columns: columns to use if we want to delete by fields
    :return: None
    """
    job_config = bigquery.job.LoadJobConfig()
    if overwrite_all:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    if overwrite_fields:
        logger.info(
            f"deleting rows in table {full_table_path} with duplicate values in {overwrite_fields_columns} ..."
        )
        job_config_2 = bigquery.job.LoadJobConfig(
            schema=[
                bigquery.SchemaField(name=overwrite_fields_columns, field_type="STRING")
            ],
            write_disposition="WRITE_TRUNCATE",
        )
        full_table_path_doublon = f"{full_table_path.split('.')[0]}.temp.delete_doublon"
        job = client.load_table_from_dataframe(
            pd.DataFrame(dataframe[overwrite_fields_columns]),
            full_table_path_doublon,
            job_config=job_config_2,
        )
        job.result()
        job = client.query(
            f"""delete from `{full_table_path}` where {overwrite_fields_columns} in (select {overwrite_fields_columns} from `{full_table_path_doublon}`);"""
        )
        job.result()
        job = client.query(f"""DROP TABLE `{full_table_path_doublon}`;""")
        job.result()
    logger.info(f"writing into {full_table_path}...")
    client.load_table_from_dataframe(dataframe, full_table_path, job_config=job_config)
    logger.info(f"data has been written into {full_table_path} with success !")

    return None
