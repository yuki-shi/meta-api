import requests
import os
import pandas as pd
import re
import json

class Facebook():
  def __init__(self):
    self.token = os.environ['PAGE_TOKEN']
    self.client_id = os.environ['CLIENT_ID']
    self.client_secret = os.environ['CLIENT_SECRET']
    self.fb_id = os.environ['FB_ID']
    self.endpoint = 'https://graph.facebook.com/v14.0'

  @staticmethod
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

  def get_posts(self, data_inicial):
    url = f'{self.endpoint}/{self.fb_id}/posts'

    params = {
        'access_token': self.token,
        'fields': 'permalink_url,created_time,message,id',
        'since': data_inicial,
        'limit': 100
    }

    response = requests.get(url, params)
    response.raise_for_status()
    posts = json.loads(response.text)

    if len(posts['data']) == 0:
      raise Exception('Sem posts novos!')
    
    df = pd.DataFrame(posts['data'])
    df['created_time'] = pd.to_datetime(df['created_time'])

    return df

  def get_post_data(self, id):
    url = f'{self.endpoint}/{id}/insights'

    params = {
        'metric': 'post_clicks,post_clicks_unique,post_impressions,post_impressions_unique,post_engaged_users',
        'access_token': self.token
    }

    response = requests.get(url, params)
    response.raise_for_status()
    response = json.loads(response.text)

    df = pd.DataFrame(response['data'])
    return self.format_dataframe(df)

  def get_post_performance(self, df):
    posts = []

    for id in df['id']:
      df = self.get_post_data(id)
      df['id'] = id
      df = df.drop('name', axis=1)
      
      posts.append(df)

    return pd.concat(posts, axis=0, ignore_index=True)