import pandas as pd

from dagster import DagsterType


def validate_country_data(_, value: pd.DataFrame):
    for column in {"country", "population"}:
        if column not in value.columns:
            return False
    if not value["country"].dtype == object:
        return False
    if not value["population"].dtype == "int64":
        return False
    return True


CountryDataDf = DagsterType(
    name="CountryDataDf",
    description="Information countries' population.",
    type_check_fn=validate_country_data
)