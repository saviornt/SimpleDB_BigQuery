# Imports
import os

# Variables

# The project_id string should be the Project ID or Project Name from the Google Cloud console
project_id = "test-project-365403"

# This string should be the directory where the service account key from Google Cloud is stored
#json_path = "path_to_json_file"
json_path = "/home/defaultuser/Projects/BigQuery/creds.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
