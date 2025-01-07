from azure.storage.filedatalake import DataLakeServiceClient
import pickle
import json


class ADLSClient:
    def __init__(self, storage_account_name, storage_account_key):
        self.service_client = self.initialize_storage_account(
            storage_account_name, storage_account_key
        )

    def initialize_storage_account(self, storage_account_name, storage_account_key):
        try:
            service_client = DataLakeServiceClient(
                account_url="{}://{}.dfs.core.windows.net".format(
                    "https", storage_account_name
                ),
                credential=storage_account_key,
            )
            return service_client
        except Exception as e:
            print(e)
            return None

    def upload_file_to_container(self, container_name, file_path, file_name):
        try:
            file_system_client = self.service_client.get_file_system_client(
                file_system=container_name
            )
            file_client = file_system_client.create_file(file_name)

            with open(file_path, "rb") as data:
                file_client.upload_data(data, overwrite=True)
            print(f"File {file_name} uploaded to {container_name} successfully.")
        except Exception as e:
            print(e)

    def upload_pickle(self, container_name, file_name, pickle_data):
        try:
            file_system_client = self.service_client.get_file_system_client(
                file_system=container_name
            )
            file_client = file_system_client.create_file(file_name)

            file_client.upload_data(pickle_data, overwrite=True)
            print(f"Model {file_name} uploaded to {container_name} successfully.")
        except Exception as e:
            print(e)

    def download_file_from_container(self, container_name, file_name, download_path):
        try:
            file_system_client = self.service_client.get_file_system_client(
                file_system=container_name
            )
            file_client = file_system_client.get_file_client(file_name)

            with open(download_path, "wb") as data:
                download = file_client.download_file()
                data.write(download.readall())
            print(f"File {file_name} downloaded from {container_name} successfully.")
        except Exception as e:
            print(e)

    def load_pickled_model_from_container(self, container_name, file_name):
        try:

            file_system_client = self.service_client.get_file_system_client(
                file_system=container_name
            )
            file_client = file_system_client.get_file_client(file_name)

            download = file_client.download_file()
            model = pickle.loads(download.readall())
            print(f"Model {file_name} loaded from {container_name} successfully.")
            return model
        except Exception as e:
            print(e)
            return None

    def upload_dict_as_json(self, container_name, file_name, data_dict):
        try:
            file_system_client = self.service_client.get_file_system_client(
                file_system=container_name
            )
            file_client = file_system_client.create_file(file_name)

            json_data = json.dumps(data_dict)
            file_client.upload_data(json_data, overwrite=True)
            print(
                f"Dictionary uploaded as JSON to {file_name} in {container_name} successfully."
            )
        except Exception as e:
            print(e)
