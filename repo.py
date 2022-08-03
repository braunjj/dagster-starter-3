import csv
import requests
from dagster import asset, define_asset_job, repository, job


@job
def josh():
    name = "Josh"
    return("Hello" + name)


@asset
def cereals(context):
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    cereal_rows = [row for row in csv.DictReader(lines)]
    context.log.info("Hello, world!")
    return cereal_rows


@asset
def nabisco_cereals(cereals):
    """Cereals manufactured by Nabisco"""
    return [row for row in cereals if row["mfr"] == "N"]

all_cereals_job = define_asset_job(name="all_cereals_job")


@repository
def repo():
    return [
        cereals,
        nabisco_cereals,
        all_cereals_job,
        josh
    ]
