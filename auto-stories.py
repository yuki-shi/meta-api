import requests
import json
import pandas as pd
import datetime as dt
import re
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from gspread_dataframe import set_with_dataframe

def main():
  # Formata o timestamp para datetime
  def format_timestamp(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['timestamp'] = df['timestamp'].apply(lambda x: x.strftime('%Y-%m-%d'))
    
    return df

  # Formata os JSONs das requisições para DataFrame
  def format_dataframe(df):
    df = df[['name', 'values']]
    df.loc[:, 'values'] = [re.findall(r'(?<=: ).*(?=})', str(x)) for x in df['values']]
    df.loc[:, 'values'] = [re.sub("[\[\]']", '', str(x)) for x in df['values']]
    df['values'] = df['values'].astype(int)
    df = df.transpose().reset_index()
    header = df.iloc[0, :]
    df.columns = header
    df.drop(df.index[0], inplace=True)

    return df

  def subtract_tz_offset(date):
    return (dt.datetime.strptime(date,'%Y-%m-%dT%H:%M:%S%z') - dt.timedelta(hours=3)).strftime('%Y-%m-%dT%H:%M:%S')

  def init_sheets(secrets_path, key):
    with open(secrets_path) as f:
      secrets = json.load(f)
    
    auth = secrets
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        auth,
        scopes=scopes
    )

    gc = gspread.authorize(credentials)

    return (gc.open_by_key(key))

  # Converte planilha para dataframe, tomando uma aba como input
  def sheets_to_dataframe(ws):
    rows = ws.get_all_values()
    df = pd.DataFrame.from_records(rows)
    columns = df.iloc[0].values
    df.columns = columns
    df.drop(index=0, axis=0, inplace=True)

    return df

  # Retorna um DataFrame contendo a performance dos stories
  def get_stories_performance():
    def get_stories_ids():
      url = f'{ENDPOINT}/{IG_ID}/stories'

      params = {
          'fields': 'id,permalink,media_type,timestamp',
          'access_token': TOKEN}

      response = requests.get(url, params)
      response.raise_for_status()
      response = json.loads(response.text)
    
      if len(response['data']) == 0:
        raise Exception('Sem stories novos!')
      
      for story in response['data']:
        story['timestamp'] = subtract_tz_offset(story['timestamp'])

      return pd.DataFrame(response['data'])

    def get_stories_data(id):
      url = f'{ENDPOINT}/{id}/insights'

      params = {
          'metric': 'exits,impressions,reach,replies,taps_forward,taps_back',
          'access_token': TOKEN
      }

      response = json.loads(requests.get(url, params).text)
      return pd.DataFrame(response['data'])

    df_ids = get_stories_ids()

    stories = {}
    stories_f = []

    for id in df_ids['id']:
      stories[id] = format_dataframe(get_stories_data(id))

    for key, df in stories.items():
      df.iloc[0, 0] = key
      df.rename(columns={'name': 'id'}, inplace=True)
      stories_f.append(df)

    df_performance = pd.concat(stories_f, axis=0, ignore_index=True)
    df_final = df_performance.merge(df_ids, on='id')

    return df_final


  # === === ===

  gspread_json = 'path/to/json'
  meta_json = 'path/to/json'
  sheets_id = 'id'

  sh = init_sheets(gspread_json, '1Ym5sibVbDvE7vB3FYz2GXOkI4JCuLBrzomovjXLlFY4')
  ws = sh.get_worksheet(0)

  with open(meta_json, 'r') as f:
    secret = json.load(f)

  for key, item in secret.items():
    os.environ[key] = item

  TOKEN = os.environ['token']
  CLIENT_ID = os.environ['client_id']
  CLIENT_SECRET = os.environ['client_secret']
  ENDPOINT = 'https://graph.facebook.com/v14.0' 
  IG_ID = os.environ['ig_id']

  # ---

  df = get_stories_performance()
  df['extraction'] = subtract_tz_offset(dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S+0000'))
  df_og = sheets_to_dataframe(ws)

  df_final = pd.concat([df, df_og], axis=0)
  df_final.iloc[:, :7] = df_final.iloc[:, :7].astype(int)

  df_final.drop_duplicates(subset='id', keep='first', inplace=True)
  df_final.sort_values('timestamp', ascending=False, inplace=True)

  # ws.clear()
  return set_with_dataframe(sh.get_worksheet(0), df_final)