import pandas as pd

from dagster import lambda_solid

from test_dagster.custom_types.country_data_df import CountryDataDf
from test_dagster.custom_types.patient_data_df import PatientDataDf


@lambda_solid
def add_country_information(statistical_data: pd.DataFrame, country_data: CountryDataDf):
    return statistical_data.merge(country_data[["country", "population"]], on="country")
