# Mencionando as bibliotecas do Facebook

from msilib import schema
from statistics import mode
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi

# Importando biblioteca do Google Cloud

from google.cloud import bigquery

# Importando a biblioteca Pandas para tratar e exportar os dados em planilha e importando o Google Cloud para enviar os dados para um banco de dados SQL

import pandas as pd
import datetime
import pytz
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\getdata-345817-cc9cab43bb92.json"

# Definindo as chaves do App para processar as requisições da API

access_token = ''
ad_account_id = ''
app_secret = ''
app_id = ''
FacebookAdsApi.init(access_token=access_token)

# Escrevendo as informações que queremos puxar (consultar documentação da API)

fields = [
    'ad_name',
    'reach',
    'spend',
    'impressions',
    'frequency',
    'campaign_name',
    'conversions',
    'cpc',
    'cpm',
    'ctr',
    'objective',
    'clicks',
    'cost_per_conversion',
    'inline_post_engagement',
]
params = {
    'date_presets' : 'lifetime',
    'time_increment' : 'all_days',
    'filtering': [],
    'level': 'ad',
    'export_columns': [],
    'export_format' : 'csv',
    'export_name' : 'beetools',
}

# Definindo qual é o DataFrame que estamos trabalhando

df = pd.DataFrame(AdAccount(ad_account_id).get_insights(fields, params))

# Exportando para CSV

csv = df.to_csv(index=False)
df.to_csv("beetools.csv")

# Exportando para Excel

datatoexcel = pd.ExcelWriter('beetols.xlsx')
df.to_excel(datatoexcel)
datatoexcel.save()

# Escrevendo para o BigQuery

df = df.drop_duplicates()

client = bigquery.Client()

table_id = "getdata-345817.get_data.fbinsights"

job = client.load_table_from_dataframe(
    df, table_id
)  

job_config = bigquery.CopyJobConfig()
job_config.write_disposition = "WRITE_TRUNCATE"

job.result()  

table = client.get_table(table_id)
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)