from dagster import make_python_type_usable_as_dagster_type, PythonObjectDagsterType, input_hydration_config, Selector, \
    Int, Field
from datetime import date
import pandas as pd


make_python_type_usable_as_dagster_type(pd.DataFrame, PythonObjectDagsterType(pd.DataFrame))


@input_hydration_config(Selector({"date": {"year": Field(Int),
                                           "month": Field(Int),
                                           "day": Field(Int)}}))
def parse_date(context, selector):
    date_selector = selector["date"]
    return date(date_selector["year"], date_selector["month"], date_selector["day"])


make_python_type_usable_as_dagster_type(date, PythonObjectDagsterType(date, input_hydration_config=parse_date))
