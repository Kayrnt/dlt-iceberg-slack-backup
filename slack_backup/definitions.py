from dagster import Definitions, define_asset_job, load_assets_from_modules

from slack_backup import assets
from slack_backup.resources import DDltResource  # noqa: TID252

all_assets = load_assets_from_modules([assets])
simple_pipeline = define_asset_job(name="simple_pipeline", selection= ['slack_pipeline'])

defs = Definitions(
    assets=all_assets,
    jobs=[simple_pipeline],
    resources={
        "pipeline": DDltResource(
            pipeline_name = "slack_backup",
            dataset_name = "slack_data",
        ),
    }
)