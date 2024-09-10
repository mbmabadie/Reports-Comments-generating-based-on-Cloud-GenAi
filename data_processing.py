from google.cloud import bigquery
import pandas as pd
import datetime

# Filter data based on month and year
def filter_data_for_month_year(df, month, year):
    df['ladate'] = pd.to_datetime(df['ladate'])  # Convert 'ladate' to datetime format
    return df[(df['ladate'].dt.year == year) & (df['ladate'].dt.month == month)]  # Filter data for the specified month and year

# Filter data based on specific column values
def filter_data_based_on_columns(df, e_offer_values, c_data_type_values, country_list):
    return df[
        df['E_OFFER'].isin(e_offer_values) &  # Filter by 'E_OFFER' values
        df['C_DATA_TYPE'].isin(c_data_type_values) &  # Filter by 'C_DATA_TYPE' values
        df['country'].isin(country_list)  # Filter by 'country' values
    ]

# Calculate sums and shares for the filtered data
def calculate_sums_and_shares(df, columns_to_sum, month, year):
    sum_by_country = df.groupby('country')[columns_to_sum].sum().reset_index()  # Sum values by country
    # Calculate digital share percentages
    sum_by_country['E2E_Digital_share'] = ((sum_by_country['E2E_Digital_sales'] / sum_by_country['All_channels_sales']) * 100).round(1)
    sum_by_country['Assisted_Digital_share'] = ((sum_by_country['Assisted_Digital_sales'] / sum_by_country['All_channels_sales']) * 100).round(1)
    sum_by_country['share_digital_all'] = (((sum_by_country['E2E_Digital_sales'] + sum_by_country['Assisted_Digital_sales']) / sum_by_country['All_channels_sales']) * 100).round(1)

    sum_by_country['month'] = month  # Add month to dataframe
    sum_by_country['year'] = year  # Add year to dataframe
    return sum_by_country

# Add a total row to the results
def add_total_row(sum_by_country, columns_to_sum, month, year):
    total_sum = sum_by_country[columns_to_sum].sum()  # Sum across all countries
    # Calculate total shares
    total_e2e_digital_share = (total_sum['E2E_Digital_sales'] / total_sum['All_channels_sales']) * 100
    total_assisted_digital_share = (total_sum['Assisted_Digital_sales'] / total_sum['All_channels_sales']) * 100
    total_share_digital_all = ((total_sum['E2E_Digital_sales'] + total_sum['Assisted_Digital_sales']) / total_sum['All_channels_sales']) * 100

    # Create a total row dataframe
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

    return pd.concat([sum_by_country, total_row])  # Concatenate the total row with the original data

# Main data processing function
def process_data(df, e_offer_values, c_data_type_values, columns_to_sum, country_list):
    results = []
    unique_months_years = df[['ladate']].apply(lambda x: (x['ladate'].month, x['ladate'].year), axis=1).unique()  # Extract unique month-year pairs

    for month, year in unique_months_years:
        filtered_df = filter_data_for_month_year(df, month, year)  # Filter data by month and year
        filtered_df = filter_data_based_on_columns(filtered_df, e_offer_values, c_data_type_values, country_list)  # Filter based on columns
        sum_by_country = calculate_sums_and_shares(filtered_df, columns_to_sum, month, year)  # Calculate sums and shares
        sum_by_country_with_total = add_total_row(sum_by_country, columns_to_sum, month, year)  # Add total row
        results.append(sum_by_country_with_total)  # Collect results

    return pd.concat(results)  # Concatenate all results into a single dataframe

# Main function to execute the data processing and save results to BigQuery
def main(request):
    project_id = 'eur-itnanalytics-97661-sbx'
    dataset_id = 'EUR_Digital_Scorecard'
    table_id = 'Digital E2E -Raw data'
    table_full = 'eur-itnanalytics-97661-sbx.EUR_Digital_Scorecard.Digital E2E -Raw data'
    
    e_offer_values = ['Mobile Only postpaid', 'Fixed Only', 'Mobile Convergent postpaid', 'Fixed Convergent']
    c_data_type_values = ['Acquisitions', 'Renewals']
    columns_to_sum = ['E2E_Digital_sales', 'Assisted_Digital_sales', 'All_channels_sales']
    countries = ['opl', 'obe', 'oro', 'osk', 'omd', 'olu']

    client = bigquery.Client()

    query = """
        SELECT *
        FROM `eur-itnanalytics-97661-sbx.EUR_Digital_Scorecard.Full extract`;
    """
    df = client.query_and_wait(query).to_dataframe()  # Fetch data from BigQuery
    print('Start loading the data...')
    result = process_data(df, e_offer_values, c_data_type_values, columns_to_sum, countries)  # Process the data
    dataframe = pd.DataFrame(result)

    # Configure job for loading data back into BigQuery
    job_config = bigquery.LoadJobConfig(
      schema=[
          bigquery.SchemaField("month", bigquery.enums.SqlTypeNames.INTEGER, mode="REQUIRED"),
          bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INTEGER, mode="REQUIRED"),
          bigquery.SchemaField("country", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
          bigquery.SchemaField("E2E_Digital_sales", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
          bigquery.SchemaField("Assisted_Digital_sales", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
          bigquery.SchemaField("All_channels_sales", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
          bigquery.SchemaField("E2E_Digital_share", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE"),
          bigquery.SchemaField("Assisted_Digital_share", bigquery.enums.SqlTypeNames.FLOAT, mode="NULLABLE")
      ],
      write_disposition='WRITE_TRUNCATE',
    )

    job = client.load_table_from_dataframe(dataframe, table_full, job_config=job_config)  # Load processed data to BigQuery
    job.result()  # Wait for the job to complete

    table = client.get_table(table_full)  # Get table details
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_full
        )
    )

    return "Done!"
