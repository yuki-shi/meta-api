import requests
import json
import pandas as pd
import datetime as dt
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import sheets


def get_og_df():
  secret_path = 'path/to/secret'
  sh_ids = sheets.init_sheets(secret_path, 'sheets-key')
  ws = sh_ids.get_worksheet(0)
  df = sheets.sheets_to_dataframe(ws)

  return ws, df

def get_ids():
  data_inicial = (dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y-%m-%d')
  url = f'https://graph.facebook.com/v14.0/{IG_ID}/media'

  params = {
      'fields': 'id,caption,media_type,media_url,permalink,timestamp',
      'since': data_inicial,
      'limit': 10,
      'access_token': TOKEN
  }

  response = requests.get(url, params)
  response.raise_for_status()
  response = json.loads(response.text)
    
  if len(response['data']) == 0:
    raise Exception('Sem posts novos!')
  
  return pd.DataFrame(response['data'])
