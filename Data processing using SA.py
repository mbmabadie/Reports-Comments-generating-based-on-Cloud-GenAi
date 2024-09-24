# similar to the original scripts but using Service account to access External BQ tables
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import datetime
import os


def filter_data_for_month_year(df, month, year):
    df['ladate'] = pd.to_datetime(df['ladate'])
    return df[(df['ladate'].dt.year == year) & (df['ladate'].dt.month == month)]

def filter_data_based_on_columns(df, e_offer_values, c_data_type_values, country_list):
    return df[
        df['E_OFFER'].isin(e_offer_values) &
        df['C_DATA_TYPE'].isin(c_data_type_values) &
        df['country'].isin(country_list)
    ]

def calculate_sums_and_shares(df, columns_to_sum, month, year):
    sum_by_country = df.groupby('country')[columns_to_sum].sum().reset_index()
    sum_by_country['E2E_Digital_share'] = ((sum_by_country['E2E_Digital_sales'] / sum_by_country['All_channels_sales']) * 100).round(1)
    sum_by_country['Assisted_Digital_share'] = ((sum_by_country['Assisted_Digital_sales'] / sum_by_country['All_channels_sales']) * 100).round(1)
    sum_by_country['share_digital_all'] = (((sum_by_country['E2E_Digital_sales'] + sum_by_country['Assisted_Digital_sales']) / sum_by_country['All_channels_sales']) * 100).round(1)

    # Assign month and year to the dataframe
    sum_by_country['month'] = month
    sum_by_country['year'] = year
    return sum_by_country

def add_total_row(sum_by_country, columns_to_sum, month, year):
    # Sum the specified columns across all countries
    total_sum = sum_by_country[columns_to_sum].sum()
    # Calculate the total E2E Digital share percentage
    total_e2e_digital_share = (total_sum['E2E_Digital_sales'] / total_sum['All_channels_sales']) * 100
    # Calculate the total Assisted Digital share percentage
    total_assisted_digital_share = (total_sum['Assisted_Digital_sales'] / total_sum['All_channels_sales']) * 100
    # Calculate the total Digital share percentage
    total_share_digital_all = ((total_sum['E2E_Digital_sales'] + total_sum['Assisted_Digital_sales']) / total_sum['All_channels_sales']) * 100

    # Create a dataframe for the total row
    total_row = pd.DataFrame(data={
        'E2E_Digital_sales': [total_sum['E2E_Digital_sales']],
        'Assisted_Digital_sales': [total_sum['Assisted_Digital_sales']],
        'All_channels_sales': [total_sum['All_channels_sales']],
        'E2E_Digital_share': [round(total_e2e_digital_share, 1)],
        'Assisted_Digital_share': [round(total_assisted_digital_share, 1)],
        'share_digital_all': [round(total_share_digital_all, 1)],
        'month': [month],
        'year': [year],
        'country': 'Total'
    })

    # Concatenate the total row with the original dataframe
    sum_by_country_with_total = pd.concat([sum_by_country, total_row])

    return sum_by_country_with_total


def process_data(df, e_offer_values, c_data_type_values, columns_to_sum, country_list):
    results = []

    # Get unique months and years from the dataframe
    unique_months_years = df[['ladate']].apply(lambda x: (x['ladate'].month, x['ladate'].year), axis=1).unique()

    for month, year in unique_months_years:
        # Filter data for the current month and year
        filtered_df = filter_data_for_month_year(df, month, year)
        filtered_df = filter_data_based_on_columns(filtered_df, e_offer_values, c_data_type_values, country_list)
        sum_by_country = calculate_sums_and_shares(filtered_df, columns_to_sum, month, year)
        sum_by_country_with_total = add_total_row(sum_by_country, columns_to_sum, month, year)
        results.append(sum_by_country_with_total)

    # Concatenate all results into a single dataframe
    final_result = pd.concat(results)

    return final_result

def main(request):
    # Define the bucket and key file details
    bucket_name = 'BUCKET NAME WHERE THE KEY IS SAVED'
    key_blob_name = 'YOUR SA KEY FILE.json'
    local_key_path = 'PATH TO KEY FILE'
    # Initialize the Storage client using the default credentials
    storage_client = storage.Client()
    # Get the bucket and blob objects
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(key_blob_name)
    # Download the key file to the local filesystem
    blob.download_to_filename(local_key_path)

    # Initialize BigQuery client with the service account credentials
    credentials = service_account.Credentials.from_service_account_file(local_key_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    # Remove the key file after initializing the client
    os.remove(local_key_path)

    # Define BigQuery table to save the processed data
    project_id = 
    dataset_id = 
    table_id = 
    table_full = f'{project_id}.{dataset_id}.{table_id}'
    # Define filter values
    e_offer_values = ['Mobile Only postpaid', 'Fixed Only', 'Mobile Convergent postpaid', 'Fixed Convergent']
    c_data_type_values = ['Acquisitions', 'Renewals']
    columns_to_sum = ['E2E_Digital_sales', 'Assisted_Digital_sales', 'All_channels_sales']
    countries = ['opl', 'obe', 'oro', 'osk', 'omd', 'olu']

    # Initialize BigQuery client
    client = bigquery.Client()

    # Construct a BigQuery SQL query to fetch your data
    query = """
        SELECT * FROM `YOUR BQ TABLE`;
    """
    df = client.query_and_wait(query).to_dataframe()
    print('Start loading the data...')
    # Process the data
    result = process_data(df, e_offer_values, c_data_type_values, columns_to_sum, countries)
    dataframe = pd.DataFrame(result)
    job_config = bigquery.LoadJobConfig(
      schema=[
          bigquery.SchemaField("month", bigquery.enums.SqlTypeNames.INTEGER, mode="REQUIRED"),
          bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INTEGER, mode="REQUIRED"),
          bigquery.SchemaField("country", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
          bigquery.SchemaField("E2E_Digital_sales", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
          bigquery.SchemaField("Assisted_Digital_sales", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
          bigquery.SchemaField("All_channels_sales", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
          bigquery.SchemaField("E2E_Digital_share", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
          bigquery.SchemaField("Assisted_Digital_share", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
          bigquery.SchemaField("share_digital_all", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE")
      ],
      write_disposition='WRITE_TRUNCATE',  # Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    )

    job = client.load_table_from_dataframe(
        dataframe, table_full, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_full)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_full
        )
    )

    return "Done!"
