import pandas as pd
from dagster import DagsterType


def validate_patient_data(_, value: pd.DataFrame):
    for column in {"global_num", "country", "province", "city", "confirmed_date"}:
        if column not in value.columns:
            return False
    if not value["confirmed_date"].dtype == "datetime64[ns]":
        return False
    if not value["country"].dtype == object:
        return False
    return True


PatientDataDf = DagsterType(
    name="PatientDataDf",
    description="Information about the infected population.",
    type_check_fn=validate_patient_data
)