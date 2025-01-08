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
            self.container_name.create_item(body=item)
            print("Item created successfully")
        except exceptions.CosmosResourceExistsError:
            print("Item already exists")
