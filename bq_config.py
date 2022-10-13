# Imports
import os

# Variables

# The project_id string should be the Project ID or Project Name from the Google Cloud console
project_id = "project-id-string"

# This string should be the directory where the service account key from Google Cloud is stored
json_path = "/home/xxx/Projects/BigQuery/creds.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
