from google.cloud import bigquery
import pandas as pd
import openai
import os

# Set up the Azure OpenAI configuration
openai.api_type = "azure"
openai.api_base = "Your deployment"
openai.api_key = "Your key"
openai.api_version = "2023-09-15-preview"

# Generate a comment about Year-over-Year (YoY) performance metric
def generate_comment_for_share(month, year, country, column_name, current_value, previous_value):
    prompt_ = (f"For the month of {month} {year}, in {country}, the Year-to-Date (YTD) "
                f"{column_name.replace('_', ' ')} is {current_value:.2f}%, "
                f"compared to {previous_value:.2f}% in the same period last year. "
                "Please generate a one sentence comment about the YoY performance metric.")
    
    messages = [
        {"role": "system", "content": "You will generate comments based on the given data."},
        {"role": "user", "content": prompt_}
    ]
    
    # Send a completion call to Azure OpenAI to generate a comment
    response = openai.ChatCompletion.create(
        engine="Your deployment",  # Specify the engine or deployment name
        messages=messages,
        max_tokens=50,
        temperature=0.35
    )

    return response['choices'][0]['message']['content']  # Return the generated comment

def main(request):
    # Define BigQuery tables for data processing and storing comments
    project_id = 
    dataset_id =
    table_id = 
    table_full = f'{project_id}.{dataset_id}.{table_id}'
    dist_table_id = 
    dist_full = f'{project_id}.{dataset_id}.{dist_table_id}'

    # Initialize BigQuery client
    client = bigquery.Client()

    # Construct a SQL query to fetch data from BigQuery
    query = f"""
        SELECT *
        FROM `{table_full}`;
    """
    df = client.query(query).to_dataframe()  # Execute the query and load data into a DataFrame
    print('Start loading the data...')

    # Convert 'month' and 'year' to datetime and sort by date
    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
    df = df.sort_values(by='date', ascending=[False])

    # Generate comments based on YoY performance metrics
    comments = []
    share_columns = ['E2E_Digital_share', 'Assisted_Digital_share', 'share_digital_all']

    for idx, row in df.iterrows():
        for share_col in share_columns:
            current_value = row[share_col]
            previous_year = row['year'] - 1
            previous_value_row = df[(df['month'] == row['month']) & (df['year'] == previous_year)]
            if not previous_value_row.empty:
                previous_value = previous_value_row[share_col].values[0]
                country = row['country'].upper()
                comment = generate_comment_for_share(row['month'], row['year'], country, share_col, current_value, previous_value)
                comments.append((row['month'], row['year'], country, share_col, current_value, comment))
        print("Sub step!")
    
    # Create a DataFrame for the generated comments
    comments_df = pd.DataFrame(comments, columns=['month', 'year', 'country', 'share_column', 'YTD_current_value', 'comment'])

    # Configure job to load the comments DataFrame into BigQuery
    job_config = bigquery.LoadJobConfig(
      schema=[
        bigquery.SchemaField("month", "INTEGER"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("share_column", "STRING"),
        bigquery.SchemaField("YTD_current_value", "FLOAT"),
        bigquery.SchemaField("comment", "STRING")
    ],
      write_disposition='WRITE_TRUNCATE',  # Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    )

    # Load the comments DataFrame into BigQuery
    job = client.load_table_from_dataframe(comments_df, dist_full, job_config=job_config)
    job.result()  # Wait for the job to complete

    table = client.get_table(dist_full)  # Get table details
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {dist_full}")

    return "Done!"
