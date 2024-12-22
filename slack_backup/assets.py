from dagster import AssetExecutionContext, DailyPartitionsDefinition, MetadataValue, asset, get_dagster_logger
from slack_backup.resources import DDltResource

@asset(partitions_def=DailyPartitionsDefinition(start_date="2024-12-01"))
def slack_pipeline(context: AssetExecutionContext, pipeline: DDltResource):
    
    partition_date_str = context.partition_key

    logger = get_dagster_logger()
    results = pipeline.create_pipeline(partition_date_str)
    logger.info(results)

    md_content=""
    for package in results.load_packages:
        for table_name, table in package.schema_update.items():
            for column_name, column in table["columns"].items():
                md_content= f"\tTable updated: {table_name}: Column changed: {column_name}: {column['data_type']}"

    # Attach the Markdown content as metadata to the asset
    context.add_output_metadata(metadata={"Updates": MetadataValue.md(md_content)})
