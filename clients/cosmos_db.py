from azure.cosmos import CosmosClient, PartitionKey, exceptions
from config import EnvConfig
import os


class CosmosDBClient:
    def __init__(self, config: EnvConfig):
        endpoint = config.cosmosdb_account_host
        key = config.cosmosdb_account_key
        self.client = CosmosClient(endpoint, key)
        self.database_name = "Big-Data-Results"
        self.container_name = "satellite_visibility_1"

    def add_item(self, item):
        try:
            self.container.create_item(body=item)
            print("Item created successfully")
        except exceptions.CosmosResourceExistsError:
            print("Item already exists")


# Usage
if __name__ == "__main__":
    client = CosmosDBClient()
    item = {"id": "1", "your_partition_key": "partition_value", "data": "your_data"}
    client.add_item(item)
