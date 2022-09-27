import json
import pandas as pd
import datetime as dt
import os
from gspread_dataframe import set_with_dataframe
import pandas_gbq
from google.oauth2 import service_account
import sheets
from facebookapi import Facebook

def main():
  # Inicialização do BigQuery
  project_id = 'project ID'
  gbq_path = 'path/to/service/account/file.json'
  credentials = service_account.Credentials.from_service_account_file(gbq_path,
                                                                      scopes=['https://www.googleapis.com/auth/bigquery',
                                                                              'https://www.googleapis.com/auth/drive'])
  schema = [{'name': 'post_clicks', 'type': 'INTEGER'},
            {'name': 'post_clicks_unique', 'type': 'INTEGER'},
            {'name': 'post_impressions', 'type': 'INTEGER'},
            {'name': 'post_impressions_unique', 'type': 'INTEGER'},
            {'name': 'post_engaged_users', 'type': 'INTEGER'},
            {'name': 'id', 'type': 'STRING'}]

  # Inicialização do Sheets
  secret_path = 'path/to/gspread/secret.json'
  key = 'sheets key'

  # Cópia da tabela original
  sh = sheets.init_sheets(secret_path, key)
  df_og = sheets.sheets_to_dataframe(sh.get_worksheet(0))
  df_og['created_time'] = pd.to_datetime(df_og['created_time'])

  # Inicialização da API do Facebook
  with open('path/to/meta/secret.json', 'r') as f:
    secrets = json.load(f)
  for key, item in secrets.items():
    os.environ[key] = item

  # ---
 
  face = Facebook()

  data_inicial = (dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y-%m-%d')

  df_posts = face.get_posts(data_inicial)
  df_posts = pd.concat([df_og, df_posts], axis=0)
  df_posts = df_posts.drop_duplicates(subset='id', keep='first')
  # Cópia pro sheets
  set_with_dataframe(sh.get_worksheet(0), df_posts)

  # Append das linhas novas no BigQuery
  df_posts['data'] = [x.strftime('%Y-%m-%d') for x in df_posts['created_time']]
  df_gbq = df_posts.query('data == @data_inicial')
  df_gbq = df_gbq.drop('data', axis=1)
  pandas_gbq.to_gbq(dataframe=df_gbq,
                    destination_table='table.name',
                    project_id=project_id,
                    credentials=credentials,
                    if_exists='append')


  # Extração da performance dos posts listados
  df_fb_performance = face.get_post_performance(df_posts)
  # Cópia para o sheets
  set_with_dataframe(sh.get_worksheet(1), df_fb_performance)

  # Insert no BigQuery
  pandas_gbq.to_gbq(dataframe=df_fb_performance,
                    destination_table='table.name',
                    project_id=project_id,
                    credentials=credentials,
                    if_exists='replace',
                    table_schema=schema)

  return


# === === === ===


if __name__ == '__main__':
  main()