import bq_config
import simpleDB
from simpleDB import Db

project_id = bq_config.project_id
if __name__ == '__main__':
    db_name = "test"
    db_location = "US"
    db_description = "Just a test dataset"
    # Db.create_database(db_name, db_location, db_description)
    # Db.copy_database(db_name, project_id, "test2")
    # Db.list_databases()
    # Db.delete_database("test")
    field_count = 2
    table_name = "test_table"
    table_description = "This is a test table for testing"
    table_expires = 0
    Db.delete_table(db_name, table_name)
    #schema = Db.create_schema(field_count)
    #Db.create_table(db_name, table_name, schema, table_description,table_expires)
