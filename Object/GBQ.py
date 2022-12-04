from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import date, timedelta

class GBQ:
    def __init__(self):
        self.project = 'winter-anchor-259905'
        self.schema = 'ACQ_stock'
        self.dividend_table = 'ACQ_i3_Dividend'
        self.bq = ''


    def initiate_connection(self, service_account_file):
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes = ['https://www.googleapis.com/auth/cloud-platform']
            )
        self.bq = bigquery.Client(
            credentials = credentials,
            project = credentials.project_id
        )

    def delete_dividend_data(self, days=7):
        query = (
            f"DELETE FROM { self.project }.{ self.schema }.{ self.dividend_table } "
            f"WHERE Announce_Date >= DATE_ADD('{ date.today().strftime('%Y-%m-%d') }', INTERVAL -{ days } DAY) "
        )
        #bigquery_table = self.bq.dataset(self.schema).table(self.dividend_table)
        self.bq.query(query)

    def update_dividend_data(self, data, days=7):
        if len(data) == 0:
            raise ValueError('Data is blank')
        if len(data[0]) != 9:
            raise ValueError('Number of columns not matched')

        columns = ['Announce_Date', 'Stock_Name', 'Opening_Price', 'Current_Price', 'Dividend', 'Ex_Date', 'Stock_Entitlement', 'Unknown', 'Stock_Entitlement_Detail']
        drop_columns = ['Opening_Price', 'Current_Price', 'Unknown']
        df = pd.DataFrame(data, columns=columns)
        df = df.drop(columns=drop_columns)
        df['Ingestion_Date'] = date.today()
        convert = {
            'Announce_Date': 'datetime64',
            'Ex_Date': 'datetime64',
            'Ingestion_Date': 'datetime64'
        }
        df = df.astype(convert)
        df = df[df['Announce_Date'] >= str(date.today() - timedelta(days=days))]

        self.delete_dividend_data(days=days)
        bigquery_table = self.bq.dataset(self.schema).table(self.dividend_table)
        self.bq.load_table_from_dataframe(df, bigquery_table)


