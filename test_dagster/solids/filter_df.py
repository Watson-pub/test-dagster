import pandas as pd
from dagster import solid, Field
from dagster.config.config_type import Enum as ConfigEnum, EnumValue


@solid(config={
        "filtered_column": Field(
            ConfigEnum("FilteredColumn", [EnumValue("province"), EnumValue("city")]),
            is_required=True,
            description="The name of the column that should be filtered."
        )
    })
def filter_df(context, df: pd.DataFrame, value_to_filter_by: str):
    column_name = str(context.solid_config["filtered_column"])
    context.log.info(f"filtering the column {column_name}, keeping only entries whose value is {value_to_filter_by}.")
    return df[df[column_name] == value_to_filter_by]
