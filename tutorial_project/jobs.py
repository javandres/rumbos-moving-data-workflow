"""Collection of Cereal jobs"""
from dagster import job

# from dagster_example.ops.cereal import (
#     display_results,
#     download_cereals,
#     find_highest_calorie_cereal,
#     find_highest_protein_cereal,
#     hello_cereal,
# )


@job
def hello_job():
    """Example of a simple Dagster job."""
    print("Hello job")


