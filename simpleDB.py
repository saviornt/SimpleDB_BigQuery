import bq_config
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer

project_id = bq_config.project_id
regions = ["US", "us-central1", "us-west4", "us-west2", "northamerica-northeast1",
           "us-east4", "us-west3", "southamerica-east1", "southamerica-west1",
           "us-east1", "northamerica_northeast2, asia-south2", "asia-east2",
           "asia-southeast2", "australia-southeast2", "asia-south1", "asia-northeast2",
           "asia-northeast3", "asia-southeast1", "australia-southeast1", "asia-east1",
           "asia-northeast1", "europe-west1", "europe-north1", "europe-west3",
           "europe-west2", "europe-southwest1", "europe-west8", "europe-west4",
           "europe-west9", "europe-central2", "europe-west6"]


class Db:
    # TODO: Create DOCSTRING

    def __init__(self):
        return

    @staticmethod
    def connect():
        # TODO: Create DOCSTRING
        client = bigquery.Client()
        return client

    @staticmethod
    def create_database(name, location):
        # TODO: Create DOCSTRING
        """
        See (https://cloud.google.com/bigquery/docs/locations) for a list of valid locations
        """
        if location not in regions:
            print("Invalid location for database.")
            return

        client = Db.connect()
        datasets = list(client.list_datasets())
        db_list = []
        for dataset in datasets:
            db_list.append("{}".format(dataset.dataset_id))

        if name in db_list:
            print("A database named {} already exists.".format(name))

        else:
            dataset_id = "{}.{}".format(client.project, name)
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = location
            dataset = client.create_dataset(dataset, timeout=30)
            print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

        return

    @staticmethod
    def copy_database(source_databaseName, copy_projectName, copy_databaseName):
        # TODO: Create DOCSTRING
        # First we need to check if the source dataset actually exists for it to be copied.
        client = Db.connect()
        datasets = list(client.list_datasets())
        db_list = []

        for dataset in datasets:
            db_list.append("{}".format(dataset.dataset_id))

        if source_databaseName not in db_list:
            print("A database named {} does exist in the project to be copied.".format(source_databaseName))

        else:
            # Since the source is legit, we need to check to see if the destination dataset already exists
            db_list = []
            for dataset in datasets:
                db_list.append("{}".format(dataset.dataset_id))
            if copy_databaseName in db_list:
                print("The {} database already exists.".format(copy_databaseName))
            else:
                source_project_id = bq_config.project_id
                transfer_client = bigquery_datatransfer.DataTransferServiceClient()
                transfer_config = bigquery_datatransfer.TransferConfig(
                    destination_dataset_id=copy_databaseName,
                    display_name=copy_databaseName,
                    data_source_id="cross_region_copy",
                    params={
                        "source_project_id": source_project_id,
                        "source_dataset_id": source_databaseName,
                    },
                    schedule="every 24 hours",
                )
                transfer_config = transfer_client.create_transfer_config(
                    parent=transfer_client.common_project_path(copy_projectName),
                    transfer_config=transfer_config,
                )
                print(f"Created copy configuration: {transfer_config.name}")
        return

    @staticmethod
    def list_databases():
        # TODO: Create DOCSTRING
        client = Db.connect()
        project = client.project
        datasets = list(client.list_datasets())

        if datasets:
            print("Databases in project {}:".format(project))
            for dataset in datasets:
                print("\t{}".format(dataset.dataset_id))
        else:
            print("{} project does not contain any databases.".format(project))

        return

    @staticmethod
    def delete_database(name):
        # TODO: Create DOCSTRING
        client = Db.connect()
        dataset_id = "{}.{}".format(project_id, name)
        datasets = list(client.list_datasets())
        db_list = []

        for dataset in datasets:
            db_list.append("{}".format(dataset.dataset_id))
        if name in db_list:
            client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
            print("Deleted database '{}'.".format(dataset_id))
        else:
            print("The database was not found in {}.".format(project_id))

        return
