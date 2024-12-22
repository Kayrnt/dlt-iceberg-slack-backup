from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa

import os

iceberg_path = os.getcwd() + "/data"
catalog_name = "default"
namespace_name = "slack_data"
catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{iceberg_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{iceberg_path}",
    },
)


def import_iceberg_metadata(catalog: Catalog, table_name: str, metadata_dir_path: str):
    """
    Import Iceberg metadata JSON into a SQL catalog.

    Args:
        catalog (Catalog): Iceberg Catalog
        metadata_file_path (str): Path to the Iceberg metadata JSON file
    """
    # list all metadata files in the directory (that ends with .metadata.json)
    metadata_files = [
        f
        for f in os.listdir(metadata_dir_path)
        if os.path.isfile(os.path.join(metadata_dir_path, f)) and f.endswith(".metadata.json")
    ]

    # take the last metadata file by alphabetical order
    metadata_file_path = os.path.join(metadata_dir_path, sorted(metadata_files)[-1])

    # Register the table in the catalog
    catalog.register_table(
        identifier=(catalog_name, namespace_name, table_name),
        metadata_location=metadata_file_path,
    )


# # Path to the Iceberg table
table_path = "slack_data/messages"

# Create a new namespace "slack_data" in the catalog if it doesn't exist
if not catalog._namespace_exists("slack_data"):
    catalog.create_namespace("slack_data")

if not catalog.table_exists("slack_data.messages"):
    import_iceberg_metadata(
        catalog,
        "messages",
        iceberg_path
        + "/slack_data/messages/metadata",
    )

# Read "slack_data.messages" table from the catalog
table = catalog.load_table("slack_data.messages")


print("Table Schema:")
print(table.schema())

# Read data from the Iceberg table
print("\nReading messages from the Iceberg table:")
arrow_data: pa.Table = table.scan().to_arrow()

# Basic display
print(arrow_data)
