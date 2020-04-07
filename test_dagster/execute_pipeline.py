from dagster import execute_pipeline

from test_dagster.pipeline import get_patient_graph
from test_dagster.solids.get_statistical_data import get_statistical_data


def main():
    config = {"solids": {
        "filter_df": {
            "inputs": {
                "value_to_filter_by": {"value": "Gangseo-gu"}
            },
            "config": {
                "filtered_column": "city"
            }
        },
        "filter_date": {
            "inputs": {
                "start_date": {
                    "date": {
                        "day": 1,
                        "month": 2,
                        "year": 2020
                    }
                }
            }
        }
    }}
    result = execute_pipeline(get_patient_graph, config)
    graph_data = result.output_for_solid("add_country_information")
    return graph_data


if __name__ == "__main__":
    main()
