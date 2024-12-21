"""Pipeline to load slack into duckdb."""

import dlt
from datetime import datetime
from slack import slack_source
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import toml
import os


# Load secrets from .dlt/secrets.toml
def load_secrets():
    secrets_path = os.path.join(os.path.dirname(__file__), ".dlt", "secrets.toml")
    with open(secrets_path, "r") as f:
        secrets = toml.load(f)
    return secrets["sources"]["slack"]["access_token"]


slack_token = load_secrets()


# Join all public channels
def check_joined_channels():
    client = WebClient(token=slack_token)
    try:
        response = client.conversations_list(types="public_channel")
        channels = response["channels"]
        for channel in channels:
            if not channel["is_member"]:
                client.conversations_join(channel=channel["id"])
                print(f"Joined channel: {channel['name']}")
    except SlackApiError as e:
        print(f"Error joining channels: {e.response['error']}")


def slack_pipeline():

    check_joined_channels()

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="slack_backup", destination="duckdb", dataset_name="slack_data"
    )

    # Create source with all configurations
    source = slack_source(
        page_size=1000,
        start_date=datetime(2024, 12, 12),
        # end_date=datetime(2024, 12, 19),
        table_per_channel=False,
        replies=True,
    ).parallelize()

    # Run pipeline for all available resources
    info = pipeline.run(source)
    print(f"Loaded data: {info}")


if __name__ == "__main__":
    slack_pipeline()
