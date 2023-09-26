import numpy as np
import pandas as pd

from src.utils.schemas import EcoscoreDF, FinalProcessedDF, MainColumnsDF, NutrimentsDF


class FoodDataProcessor:
    def __init__(self, raw_data: pd.DataFrame):
        self.raw_data = raw_data
        self.main_columns_df: MainColumnsDF = self.preprocess_main_columns()
        self.carbon_footprint_df: EcoscoreDF = self.preprocess_carbon_footprint()
        self.nutriments_df: NutrimentsDF = self.preprocess_nutriments_from_raw_data()
        self.processed_df: FinalProcessedDF = FinalProcessedDF.convert(
            self.main_columns_df.df.join(self.nutriments_df.df).join(
                self.carbon_footprint_df.df
            )
        )

    def preprocess_nutriments_from_raw_data(self) -> NutrimentsDF:
        list_nutriments = []
        keys = [
            "energy-kcal_100g",
            "fat_100g",
            "saturated-fat_100g",
            "carbohydrates_100g",
            "sugars_100g",
            "proteins_100g",
            "salt_100g",
        ]
        for row in self.raw_data["nutriments"]:
            try:
                list_nutriments.append(
                    {key: row[key] if key in row else np.nan for key in keys}
                )
            except:
                list_nutriments.append(dict.fromkeys(keys))
        df_nutriments: pd.DataFrame = pd.DataFrame.from_dict(list_nutriments)
        df_nutriments = df_nutriments.rename(
            columns={
                "energy-kcal_100g": "energy_kcal_100g",
                "saturated-fat_100g": "saturated_fat_100g",
            }
        )
        return NutrimentsDF.convert(df_nutriments)

    def preprocess_carbon_footprint(self) -> EcoscoreDF:
        co2 = []
        for row in self.raw_data["ecoscore_data"]:
            try:
                co2.append(row["agribalyse"]["co2_total"] * 100)
            except:
                co2.append(np.nan)
        ecoscore_df = pd.DataFrame(co2, columns=["carbon_footprint_100g"])
        return EcoscoreDF.convert(ecoscore_df)

    def preprocess_main_columns(self) -> MainColumnsDF:
        df_main_columns = self.raw_data.loc[
            :,
            [
                "code",
                "creator",
                "countries",
                "countries_tags",
                "created_t",
                "last_modified_t",
                "last_modified_by",
                "product_name",
                "generic_name",
                "categories",
                "categories_tags",
                "quantity",
                "packaging_tags",
                "brands",
                "ingredients_tags",
                "ingredients_analysis_tags",
                "allergens",
                "serving_quantity",
                "nutriscore_score",
                "nutriscore_grade",
                "nova_group",
                "ecoscore_score",
                "ecoscore_grade",
            ],
        ]
        df_main_columns["code"] = df_main_columns["code"].apply(
            lambda x: x if isinstance(x, str) else str(int(x))
        )
        df_main_columns["created_t"] = pd.to_datetime(
            df_main_columns.loc[:, "created_t"], unit="s"
        ).dt.date
        df_main_columns["last_modified_t"] = pd.to_datetime(
            df_main_columns.loc[:, "last_modified_t"], unit="s"
        ).dt.date
        df_main_columns["nova_group"] = [
            str(int(nova_group)) if not np.isnan(nova_group) else np.nan
            for nova_group in self.raw_data["nova_group"]
        ]
        return MainColumnsDF.convert(df_main_columns)
