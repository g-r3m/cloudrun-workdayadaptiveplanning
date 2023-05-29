# module docstring
"""
This module is the main module for the Cloud Run WAP service.
It is responsible for:
- Getting the WAP data
- Parsing the XML
- Transforming the data
- Loading the data into BigQuery
"""
import os
import requests
import xml.etree.ElementTree as et
import json
import pandas as pd
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
    start = request.args.get('START_DATE') or os.environ.get(
        'START_DATE', 'DD/YYYY')
    period = start.replace('/', '')
    account = request.args.get(
        'ACCOUNT') or os.environ.get('ACCOUNT', 'Account1')
    level_name = request.args.get(
        'LEVEL_NAME') or os.environ.get('LEVEL_NAME', 'LevelName1')
    dim_name = request.args.get(
        'DIM_NAME') or os.environ.get('DIM_NAME', 'DimName1')
    dimname = dim_name.replace('/', '')        
    dim = request.args.get('DIM') or os.environ.get('DIM', 'Dim1')
    version_name = request.args.get('VERSION_NAME') or os.environ.get(
        'VERSION_NAME', 'VersionName1')
    # Set the Google Cloud Platform project, dataset, and table details
    project_id = "PROJECT_ID"
    dataset_id = "DATASET_ID"
    table_id = "TABLE_ID"
    # Set & Get the WAP API
    url = "https://api.adaptiveinsights.com/api/v36"
    data = get_secret(project_id, "adaptive_login")
    login = data['login']
    password = data['password']
    payload = f"""<?xml version='1.0' encoding='UTF-8'?>
    <call method="exportData" callerName="wap-extract">
    <credentials login="{login}" password="{password}"/>
    <version name="{version_name}" isDefault="false"/>
    <format useInternalCodes="true" includeUnmappedItems="false" />
    <filters>
    <accounts>
        <account code="{account}" isAssumption="false" includeDescendants="true"/>
    </accounts>
    <levels>
        <level name="{level_name}" isRollup="true" includeDescendants="false"/>
    </levels>
    <dimensionValues>
        <dimensionValue dimName="{dim_name}" name="{dim}" directChildren="false"/>
    </dimensionValues>
    <timeSpan start="{start}" end="{start}"/>
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
    print(payload)
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)
    # Save the result in a file in the /tmp folder
    file_path = "/tmp/output.xml"

    with open(file_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=128):
            file.write(chunk)
    # Parse the XML
    xtree = et.parse(file_path)
    root = xtree.getroot()
    print(xtree)
    # Extract the output text from CDATA
    output_text = root.find("./output")
    if output_text is not None:
        output_text = output_text.text
        # Split the output into lines
        lines = output_text.strip().split('\n')
        # Split the header and values
        header = lines[0].split(',')
        values = lines[1].split(',')
        # Create a dictionary to store the data
        data_dict = {}
        for i, col in enumerate(header):
            data_dict[col] = [values[i]]
        # Create the DataFrame
        df = pd.DataFrame(data_dict)
        # Remove specific characters from the table name
        # Remove specific characters from dtype names
        df.columns = df.columns.str.replace('%', '')
        df.columns = df.columns.str.replace('/', '')
        df.columns = df.columns.str.replace('!', '')
        df.columns = df.columns.str.replace('"', '')
        # Preprocess DataFrame to remove extra quotes
        df["Account Name"] = df["Account Name"].str.strip('"""')
        df["Account Code"] = df["Account Code"].str.strip('"""')
        df["Level Name"] = df["Level Name"].str.strip('"""')
        df[dimname] = df[dimname].str.strip('"""')
        df = df.rename(columns={
            "Account Name": "Name",
            "Account Code": "Code",
            "Level Name": "Lvl_Name",
            dimname: "Dim_name",
            period: "Value"
        })
        # Add related info (from columns in WAP APIs)
        df["Period"] = period
        df["Dim"]= dim
        # Get current timestamp
        current_timestamp = datetime.now()
        # Convert timestamp to desired format
        current_timestamp = pd.to_datetime(
            current_timestamp).strftime("%Y-%m-%d %H:%M:%S")
        # Add insert_timestamp column to DataFrame

        df["insert_timestamp"] = current_timestamp        
        from google.cloud import bigquery

        # Create a BigQuery client using the default credentials
        client = bigquery.Client(project=project_id)
        # Print df transformed ready for loading
        print(df)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.schema = [
            bigquery.SchemaField("Name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("Code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "Lvl_Name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("Dim_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("Dim", bigquery.enums.SqlTypeNames.STRING)
        ]
        print(df)
        # Write DataFrame to BigQuery table
        client.load_table_from_dataframe(
            df, table_ref, job_config=job_config).result()
        table = client.get_table(table_ref)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
        # return an appropriate response
        loaded_data = {
            'success': 'Workday Adaptive Planning response',
            'response_text': response.text,
        }
    else: 
        # return an appropriate error response
        error_data = {
            'error': 'Workday Adaptive Planning error',
            'response_text': response.text,
        }
        return jsonify(error_data), 500

    return jsonify(loaded_data), 200    


if __name__ == "__main__":
    server_port = os.environ.get("PORT", "8080")
    app.run(debug=False, port=server_port, host="0.0.0.0")
