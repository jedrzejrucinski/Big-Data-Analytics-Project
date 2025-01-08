from azure.cosmos import CosmosClient, PartitionKey, exceptions
from config import EnvConfig
import os


class CosmosDBClient:
    def __init__(self, config: EnvConfig):
        endpoint = config.cosmosdb_account_host
        key = config.cosmosdb_account_key
        self.client = CosmosClient(endpoint, key)
        self.database_name = config.cosmosdb_database
        self.container_name = config.cosmosdb_container
        self.database = self.client.get_database_client(self.database_name)
        self.container = self.database.get_container_client(self.container_name)

    def add_item(self, item):
        try:
            self.container.create_item(body=item)
            print("Item created successfully")
        except exceptions.CosmosResourceExistsError:
            print("Item already exists")
