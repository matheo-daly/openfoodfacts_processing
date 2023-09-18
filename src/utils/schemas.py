import numpy as np
from typedframe import DATE_TIME_DTYPE, TypedDataFrame


class MainColumnsDF(TypedDataFrame):
    schema = {
        "code": str,
        "creator": str,
        "created_t": DATE_TIME_DTYPE,
        "last_modified_t": DATE_TIME_DTYPE,
        "last_modified_by": str,
        "product_name": str,
        "generic_name": str,
        "quantity": str,
        "countries": str,
        "countries_tags": str,
        "categories": str,
        "categories_tags": str,
        "packaging_tags": str,
        "brands": str,
        "ingredients_tags": str,
        "ingredients_analysis_tags": str,
        "allergens": str,
        "serving_quantity": np.float64,
        "nutriscore_score": np.float64,
        "nutriscore_grade": str,
        "nova_group": str,
        "ecoscore_score": np.float64,
        "ecoscore_grade": str,
    }


class NutrimentsDF(TypedDataFrame):
    schema = {
        "energy_kcal_100g": np.float64,
        "fat_100g": np.float64,
        "saturated_fat_100g": np.float64,
        "carbohydrates_100g": np.float64,
        "sugars_100g": np.float64,
        "proteins_100g": np.float64,
        "salt_100g": np.float64,
    }


class EcoscoreDF(TypedDataFrame):
    schema = {
        "carbon_footprint_100g": np.float64,
    }


class FinalProcessedDF(MainColumnsDF, NutrimentsDF, EcoscoreDF):
    schema = {**MainColumnsDF.schema, **NutrimentsDF.schema, **EcoscoreDF.schema}
