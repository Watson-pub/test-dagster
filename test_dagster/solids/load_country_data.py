from dagster import solid, Field, String
import pandas as pd
from test_dagster.consts import DATA_PATH
from test_dagster.custom_types.country_data_df import CountryDataDf


@solid(
    config={
        "country_data_location": Field(
            String,
            default_value=str(DATA_PATH.joinpath("country_data.csv")),
            is_required=False,
            description="The location of the country data file."
        )
    }
)
def load_country_data(context) -> CountryDataDf:
    country_data = pd.read_csv(context.solid_config["country_data_location"])
    country_data["population"] = country_data["population"].astype("int64")
    country_data["country"] = country_data["country"].astype("str")
    return country_data
