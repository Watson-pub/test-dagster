from dagster import lambda_solid

from test_dagster.custom_types.patient_data_df import PatientDataDf


@lambda_solid
def get_statistical_data(data_df: PatientDataDf):
    combined_data = data_df[["global_num", "country"]].groupby(["country", data_df["confirmed_date"].dt.date]).count()
    return combined_data.reset_index().rename(columns={"global_num": "new_cases", "confirmed_date": "date"})
