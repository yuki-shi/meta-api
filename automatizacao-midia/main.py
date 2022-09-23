import json
import os
import sheets
import gspread
from gspread_dataframe import set_with_dataframe
import postperf

def main():
  with open('meta-secret.json', 'r') as f:
    secret = json.load(f)

  for key, item in secret.items():
    os.environ[key] = item

  secret_path = 'gspread.json'
  sh = sheets.init_sheets(secret_path, '1H3do6uz0oFyykQCnnX4BRNti9OwQEc9JUaTzqSX4os4')
  df_og = sheets.sheets_to_dataframe(sh.get_worksheet(0))

  insta = postperf.Instagram(os.environ['token'], os.environ['client_id'],
                      os.environ['client_secret'], os.environ['ig_id'])

  dfs = insta.get_media_performance(df_og)

  for index, df in enumerate(dfs.values()):
    set_with_dataframe(sh.get_worksheet(index+1), df)

  return

if __name__ == '__main':
  main()