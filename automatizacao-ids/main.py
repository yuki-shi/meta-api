import pandas as pd
import json
import os
from gspread_dataframe import set_with_dataframe
from auto_ids import *

def main():
  with open('path/to/secret.json', 'r') as f:
    secret = json.load(f)

  for key, item in secret.items():
    os.environ[key] = item

  TOKEN = os.environ['TOKEN']
  IG_ID = os.environ['IG_ID']

  df_ids = get_ids()
  ws, df_og = get_og_df()
  df_final = pd.concat([df_og, df_ids], ignore_index=True)
  df_final = df_final.drop_duplicates(subset='id', keep='last')

  return set_with_dataframe(ws, df_final)


if __name__ == '__main__':
  main()
