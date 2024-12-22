from dagster import ConfigurableResource, get_dagster_logger 
import dlt
from slack import slack_source
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os

logger = get_dagster_logger()

class DDltResource(ConfigurableResource):
    pipeline_name: str
    dataset_name: str

    # Join all public channels
    def check_joined_channels(self):
      slack_token: str = os.getenv("SLACK_ACCESS_TOKEN")
      client = WebClient(token=slack_token)
      try:
          response = client.conversations_list(types="public_channel")
          channels = response["channels"]
          for channel in channels:
              if not channel["is_member"]:
                  client.conversations_join(channel=channel["id"])
                  logger.info(f"Joined channel: {channel['name']}")
      except SlackApiError as e:
          logger.error(f"Error joining channels: {e.response['error']}")


    def create_pipeline(self, run_date: str):

      logger.info("Creating Slack pipeline...")

      self.check_joined_channels()

      logger.info("Joined channels ✅")

      start_date = f"{run_date} 00:00:00"
      end_date = f"{run_date} 23:59:59"

      logger.info(f"Running slack backup from {start_date} to {end_date}.")

      # configure the pipeline with your destination details
      pipeline = dlt.pipeline(
        pipeline_name=self.pipeline_name,
        dataset_name=self.dataset_name,
        destination = "filesystem"
      )

      # Create source with all configurations
      source = slack_source(
          page_size=1000,
          table_per_channel=False,
          start_date = start_date,
          end_date = end_date,
          replies=True,
      )

      # Run pipeline for all available resources
      info = pipeline.run(source, table_format="iceberg")

      return info