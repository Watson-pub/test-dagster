import pandas as pd

from datetime import date

from dagster import lambda_solid

from test_dagster.custom_types.patient_data_df import PatientDataDf


@lambda_solid
def filter_date(patient_data: PatientDataDf, start_date: date):
    return patient_data[patient_data["confirmed_date"] >= pd.to_datetime(start_date)]
