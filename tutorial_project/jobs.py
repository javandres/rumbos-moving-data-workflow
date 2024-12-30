"""Collection of Cereal jobs"""

from dagster import job


@job
def hello_job():
    """Example of a simple Dagster job."""
    print("Hello job")
