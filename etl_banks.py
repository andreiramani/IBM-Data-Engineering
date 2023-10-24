# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
import sqlite3
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
url_exchange = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attribs = ["Rank", "Bank name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'largest_banks'
csv_path = 'largest_banks_data.csv'
count = 0

html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()  # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_bank_log.txt", "a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the table under the heading 'By market capitalization'
        table_heading = soup.find('span', {'id': 'By_market_capitalization'})
        if table_heading:
            # Navigate to the parent (usually a heading tag) and find the next table sibling
            table = table_heading.find_next('table')

            # Extract table data into a list of lists
            table_data = []
            for row in table.find_all('tr'):
                row_data = [cell.get_text(strip=True) for cell in row.find_all(['th', 'td'])]
                table_data.append(row_data)

            # Convert the list of lists into a pandas DataFrame
            df = pd.DataFrame(table_data[1:], columns=table_attribs)
            
            # Convert MC_USD_Billion column from object to float
            df = df.astype({'MC_USD_Billion':'float'})

            return df

        else:
            print("Table not found under the heading 'By market capitalization'")
            return None
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return None

def transform(df, exchange_rate_file='exchange_rate.csv'):
    # Read exchange rate information from the CSV file
    exchange_rates = pd.read_csv(exchange_rate_file)

    # Merge the original DataFrame with the exchange rate DataFrame based on Currency
    # df_merged = pd.merge(df, exchange_rates, how='left', on='Currency')

    # Convert Market Capitalization to GBP, EUR, and INR using the exchange rates
    df['MC_GBP_Billion'] = df['MC_USD_Billion'] * 0.8
    df['MC_EUR_Billion'] = df['MC_USD_Billion'] * 0.93
    df['MC_INR_Billion'] = df['MC_USD_Billion'] * 82.95

    # Round the new columns to 2 decimal places
    df['MC_GBP_Billion'] = df['MC_GBP_Billion'].round(2)
    df['MC_EUR_Billion'] = df['MC_EUR_Billion'].round(2)
    df['MC_INR_Billion'] = df['MC_INR_Billion'].round(2)
        
    # Display the transformed DataFrame
    print("\nTransformed DataFrame:")
    print(df)

def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, url_exchange)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
