from dagster import pipeline

from test_dagster.solids.add_country_information import add_country_information
from test_dagster.solids.get_statistical_data import get_statistical_data
from test_dagster.solids.filter_date import filter_date
from test_dagster.solids.filter_df import filter_df
from test_dagster.solids.load_country_data import load_country_data
from test_dagster.solids.load_patient_data import load_patient_data


@pipeline
def get_patient_graph():
    patient_data = load_patient_data()
    patient_data = filter_df(patient_data)
    patient_data = filter_date(patient_data)
    statistical_patient_date = get_statistical_data(patient_data)
    add_country_information(statistical_patient_date, load_country_data())
