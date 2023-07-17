# module docstring
"""
This module is the main module for the Cloud Run WAP service.
It is responsible for:
- Getting the WAP data
- Parsing the XML
- Transforming the data
- Loading the data into BigQuery
More info on the API(https://doc.workday.com/adaptive-planning/en-us/integration/managing-data-integration/api-documentation/metadata-and-data-create-update-and-read-methods/mdn1623709213322.html)
"""
# test
import os
import csv
import requests
import xml.etree.ElementTree as et
import json
import pandas as pd
import re
from datetime import datetime
from google.cloud import bigquery
from google.cloud import secretmanager
from flask import Flask, request, jsonify

app = Flask(__name__)


def get_secret(project_id, secret_id):
    """
    Get the secret from Secret Manager
    """
    client = secretmanager.SecretManagerServiceClient()
    secret_name = client.secret_version_path(project_id, secret_id, "latest")
    secret_request = secretmanager.AccessSecretVersionRequest(name=secret_name)
    response = client.access_secret_version(secret_request)
    payload = response.payload.data.decode("UTF-8")
    return json.loads(payload)


@app.route("/", methods=["GET", "POST"])
def main():
    """
    Main function
    """
    current_year = datetime.now().year
    start = request.args.get('START_DATE') or os.environ.get('START_DATE', f'01/{current_year}')
    end = request.args.get('END_DATE') or os.environ.get('END_DATE', f'12/{current_year}')
    level_name = request.args.get('LEVEL_NAME') or os.environ.get('LEVEL_NAME', 'H500A')
    dim_name = request.args.get('DIM_NAME') or os.environ.get('DIM_NAME', 'FF/FI')
    dim = request.args.get('DIM') or os.environ.get('DIM', 'FI')
    version_name = request.args.get('VERSION_NAME') or os.environ.get('VERSION_NAME', 'REEL_AGORA')
    # Set the Google Cloud Platform project, dataset, and table details
    project_id = "$PROJECT_ID"
    dataset_id = "*DATASET_ID"
    table_id = "$TABLE_ID"
    # Set & Get the WAP API
    url = "https://api.adaptiveinsights.com/api/v36"
    data = get_secret(project_id, "adaptive_login")
    login = data["login"]
    password = data["password"]
    payload = f"""<?xml version='1.0' encoding='UTF-8'?>
    <call method="exportData" callerName="wap-extract">
    <credentials login="{login}" password="{password}"/>
    <version name="{version_name}" isDefault="false"/>
    <format useInternalCodes="true" includeUnmappedItems="false" />
    <filters>
    <accounts>
      <account code="ChargesEbitda" isAssumption="false" includeDescendants="true"/>
      <account code="CoutsNonCourant" isAssumption="false" includeDescendants="true"/>
      <account code="ChargesCourantesNonCash" isAssumption="false" includeDescendants="true"/>
      <account code="ResultatHorsExploitation" isAssumption="false" includeDescendants="true"/>
      <account code="ProduitsExploitation" isAssumption="false" includeDescendants="true"/>                     
    </accounts>
    <levels>
        <level name="{level_name}" isRollup="true" includeDescendants="true"/>
    </levels>
    <dimensionValues>
        <dimensionValue dimName="{dim_name}" name="{dim}" directChildren="false"/>
    </dimensionValues>
    <timeSpan start="{start}" end="{end}"/>
    </filters>
    <dimensions>
    <dimension name="{dim_name}"/>
    </dimensions>
    <rules includeZeroRows="true" includeRollupAccounts="false" includeRollupLevels="true">
    <currency useCorporate="false" useLocal="true"/>
    </rules>
    </call>"""
    headers = {
        'Content-Type': 'application/xml'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    # Save the result in a file in the /tmp folder
    file_path = "/tmp/output.xml"
    #debug var bellow
    with open(file_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=128):
            file.write(chunk)
    # Parse the XML
    xtree = et.parse(file_path)
    root = xtree.getroot()
    # Extract the output text from CDATA
    data = root.find('output')
    if data is not None:
        # Get the data from the XML
        data = root.find('output').text.strip().split('\n')
        header = data[0].replace(' ', '').replace('"', '').replace('/', '')
        rows = data[1:]
        # Clean the header
        # Create a CSV file
        # Put file in /tmp/ folder for GCR
        with open('/tmp/output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write the header add insertion_timestamp
            writer.writerow(['insertion_timestamp'] + header.split(','))

            # Write the rows
            for row in rows:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                writer.writerow([timestamp] + row.split(','))
                            
        # Create a BigQuery client
        client = bigquery.Client(project=project_id)

        # Set the path to the CSV file
        csv_file = "/tmp/output.csv"
        # Define the BigQuery dataset and table references
        table_ref = client.dataset(dataset_id).table(table_id)

        # Define the job configuration
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.skip_leading_rows = 1  # Skip the header row
        job_config.allow_jagged_rows = True  # Allow jagged rows
        job_config.quote_character = '"'  # Set the quote character to double quote


        # Load data from CSV file into BigQuery table
        with open(csv_file, "rb") as file:
            job = client.load_table_from_file(file, table_ref, job_config=job_config)

        job.result()  # Wait for the job to complete        
        table = client.get_table(table_ref)  # Make an API request.
        print(
             "Loaded {} rows and {} columns to {}".format(
                 table.num_rows, len(table.schema), table_id
             )
         )
        # return an appropriate response
        loaded_data = {
            "success": "Workday Adaptive Planning response",
            "response_text": response.text,
        }
    else:
        # return an appropriate error response
        error_data = {
            "error": "Workday Adaptive Planning error",
            "response_text": response.text,
        }
        return jsonify(error_data), 500

    return jsonify(loaded_data), 200


if __name__ == "__main__":
    server_port = os.environ.get("PORT", "8080")
    app.run(debug=False, port=server_port, host="0.0.0.0")


if __name__ == "__main__":
    server_port = os.environ.get("PORT", "8080")
    app.run(debug=False, port=server_port, host="0.0.0.0")
