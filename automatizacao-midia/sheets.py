from oauth2client.service_account import ServiceAccountCredentials
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
import json

# === === === ===

# Inicializa a API do Sheets, criando um objeto a partir
# da chave encontrada na URL do arquivo
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