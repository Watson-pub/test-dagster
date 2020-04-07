from dagster import solid, Field, String
import pandas as pd
from test_dagster.consts import DATA_PATH
from test_dagster.custom_types.patient_data_df import PatientDataDf


@solid(
    config={
        "patient_data_location": Field(
            String,
            default_value=str(DATA_PATH.joinpath("patient_data.csv")),
            is_required=False,
            description="The location of the patient data file."
        )
    }
)
def load_patient_data(context) -> PatientDataDf:
    patient_data = pd.read_csv(context.solid_config["patient_data_location"])
    patient_data["confirmed_date"] = pd.to_datetime(patient_data["confirmed_date"], infer_datetime_format=True)
    patient_data["country"] = patient_data["country"].astype("str")
    return patient_data
