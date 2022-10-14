import bq_config, datetime
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

field_types = ["INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC", "BOOL", "STRING", "BYTES", "DATE", "DATETIME", "TIME",
               "TIMESTAMP", "STRUCT", "GEOGRAPHY", "JSON"]

field_modes = ["NULLABLE", "REQUIRED", "REPEATED"]


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
    def create_database(database_name, location, description=""):
        # TODO: Create DOCSTRING
        """
        See (https://cloud.google.com/bigquery/docs/locations) for a list of valid locations
        """

        if location not in regions:
            err = "Invalid location for database."
            return err

        client = Db.connect()
        datasets = list(client.list_datasets())
        db_list = []
        for dataset in datasets:
            db_list.append("{}".format(dataset.dataset_id))

        if database_name in db_list:
            err = "A database named {} already exists.".format(database_name)
            return err

        else:
            dataset_id = "{}.{}".format(client.project, database_name)
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = location
            dataset.description = description
            dataset = client.create_dataset(dataset, timeout=30)
            dataset = client.update_dataset(dataset, ["description"])
            result = "Created dataset {}.{}".format(client.project, dataset.dataset_id)
        return result

    @staticmethod
    def copy_database(source_database_name, copy_project_name, copy_database_name):
        # TODO: Create DOCSTRING
        # First we need to check if the source dataset actually exists for it to be copied.
        client = Db.connect()
        datasets = list(client.list_datasets())
        db_list = []

        for dataset in datasets:
            db_list.append("{}".format(dataset.dataset_id))

        if source_database_name not in db_list:
            err = "A database named {} does exist in the project to be copied.".format(source_database_name)
            return err
        else:
            # Since the source is legit, we need to check to see if the destination dataset already exists
            db_list = []
            for dataset in datasets:
                db_list.append("{}".format(dataset.dataset_id))
            if copy_database_name in db_list:
                err = "The {} database already exists.".format(copy_database_name)
                return err
            else:
                source_project_id = bq_config.project_id
                transfer_client = bigquery_datatransfer.DataTransferServiceClient()
                transfer_config = bigquery_datatransfer.TransferConfig(
                    destination_dataset_id=copy_database_name,
                    display_name=copy_database_name,
                    data_source_id="cross_region_copy",
                    params={
                        "source_project_id": source_project_id,
                        "source_dataset_id": source_database_name,
                    },
                    schedule="every 24 hours",
                )
                transfer_config = transfer_client.create_transfer_config(
                    parent=transfer_client.common_project_path(copy_project_name),
                    transfer_config=transfer_config,
                )
                result = f"Created copy configuration: {transfer_config.name}"
        return result

    @staticmethod
    def list_databases():
        # TODO: Create DOCSTRING
        client = Db.connect()
        project = client.project
        datasets = list(client.list_datasets())
        db_list = []
        if datasets:
            for dataset in datasets:
                db_list.append("{}".format(dataset.dataset_id))
        return db_list

    @staticmethod
    def update_database_description(database_name, database_description):
        client = Db.connect()
        dataset_id = "{}.{}".format(project_id, database_name)
        dataset = client.get_dataset(dataset_id)
        dataset.description = database_description
        dataset = client.update_dataset(dataset, ["description"])
        return

    @staticmethod
    def delete_database(database_name):
        # TODO: Create DOCSTRING
        client = Db.connect()
        dataset_id = "{}.{}".format(project_id, database_name)
        datasets = list(client.list_datasets())
        db_list = []

        for dataset in datasets:
            db_list.append("{}".format(dataset.dataset_id))
        if database_name in db_list:
            client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
            result = "Deleted database '{}'.".format(dataset_id)
        else:
            result = "The database was not found in {}.".format(project_id)

        return result

    # This method may not be needed, re-evaluate
    @staticmethod
    def create_field(field_name, field_type, field_mode):
        # TODO: Create DOCSTRING
        field_type = field_type.upper()
        field_mode = field_mode.upper()
        if field_type not in field_types:
            err = "Invalid schema field type."
            return err
        if field_mode not in field_modes:
            err = "Invalid schema field mode."
            return err
        schema_field = "{}, {}, mode={}".format(field_name, field_type, field_mode)
        return schema_field

    @staticmethod
    def create_schema(total_fields):
        schema = []
        for i in range(total_fields):
            field_name = input("Enter field name: ")
            field_type = input("Enter field type: ").upper()
            if field_type not in field_types:
                print("Invalid field type.")
                field_type = input("Enter field type: ").upper()
            field_mode = input("Enter field mode: ").upper()
            while field_mode not in field_modes:
                print("Invalid field mode.")
                field_mode = input("Enter field mode: ").upper()
            schema.append(bigquery.SchemaField(field_name, field_type, field_mode))
        return schema

    @staticmethod
    def create_table(database_name, table_name, schema, table_description="", table_expires=0):
        # TODO: Create DOCSTRING
        # table_expires is measured in days, if it is 0, then it does not expire

        client = Db.connect()
        table_id = "{}.{}.{}".format(project_id, database_name, table_name)
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        if table_description != "":
            Db.update_table_description(database_name, table_name, table_description)
        Db.update_table_expiration(database_name, table_name, table_expires)
        result = "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        return result

    @staticmethod
    def update_table_description(database_name, table_name, table_description):
        client = Db.connect()
        project = client.project
        dataset_ref = bigquery.DatasetReference(project, database_name)
        table_ref = dataset_ref.table(table_name)
        table = client.get_table(table_ref)
        table.description = table_description
        table = client.update_table(table, ["description"])
        result = "Updated table description to {}".format(table_description)
        return result

    @staticmethod
    def update_table_expiration(database_name, table_name, table_expiration):
        client = Db.connect()
        project = client.project
        dataset_ref = bigquery.DatasetReference(project, database_name)
        table_ref = dataset_ref.table(table_name)
        table = client.get_table(table_ref)

        if table_expiration == 0:
            table_expiration = "Never"
            result = "Updated the table to never expire"
        else:
            table_expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=table_expiration)
            result = "Updated the table to expire in {} days.".format(table_expiration)
        table.expires = table_expiration
        table = client.update_table(table, ["expires"])
        return result

    @staticmethod
    def copy_table(source_database_name, source_table_name, destination_database_name, destination_table_name):
        client = Db.connect()
        source_table_id = "{}.{}.{}".format(project_id, source_database_name, source_table_name)
        destination_table_id = "{}.{}.{}".format(project_id, destination_database_name, destination_table_name)
        copy_job = client.copy_table(source_table_id, destination_table_id)
        copy_job.result()
        result = "The table {} has been copied to {}.".format(source_table_name, destination_database_name)
        return result

    @staticmethod
    def delete_table(database_name, table_name):
        client = Db.connect()
        table_id = "{}.{}.{}".format(project_id, database_name, table_name)
        client.delete_table(table_id, not_found_ok=True)
        result = "Deleted table {}.".format(table_id)
        return result
