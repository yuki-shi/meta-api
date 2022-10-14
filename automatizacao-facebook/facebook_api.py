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
    self.endpoint = 'https://graph.facebook.com/v15.0'

  @staticmethod
  def format_dataframe(df):
    df['values'] = df['values'].astype(int)
    df = df.transpose().reset_index()
    header = df.iloc[0, :]
    df.columns = header
    df = df.drop(df.index[0])

    return df

  def get_posts(self, data_inicial):
    url = f'{self.endpoint}/{self.fb_id}/posts'

    params = {
        'access_token': self.token,
        'fields': 'permalink_url,created_time,message,shares,id',
        'since': data_inicial,
        'limit': 100
    }

    response = requests.get(url, params)
    response.raise_for_status()
    posts = json.loads(response.text)

    if len(posts['data']) == 0:
      raise Exception('Sem posts novos!')

    df = pd.DataFrame(posts['data'])
    df['shares'] = (df['shares'].astype(str)
                                .str.extract(r'([0-9]+)')
                                .fillna(0)
                                .astype(int))
    df['created_time'] = pd.to_datetime(df['created_time'])
    df['created_time'] -= dt.timedelta(hours=3) # Para GMT-03:00

    return df

  def get_post_data(self, id):
    url = f'{self.endpoint}/{id}/insights'

    params = {
        'metric': 'post_clicks,post_clicks_unique,post_impressions,post_impressions_unique,post_engaged_users,post_reactions_by_type_total',
        'access_token': self.token
    }

    response = requests.get(url, params)
    response_f = json.loads(response.text)
    data = {} 

    if response.status_code == 400:
      data['post_clicks'] = 0
      data['post_clicks_unique'] = 0
      data['post_impressions'] = 0
      data['post_impressions_unique'] = 0
      data['post_engaged_users'] = 0
      data['reactions_total'] = 0

      return pd.DataFrame(data, index=[0])
     
    for entry in response_f['data']:
      if entry['name'] != 'post_reactions_by_type_total':
        data[entry['name']] = entry['values'][0]['value']
      else:
        data['reactions_total'] = sum(entry['values'][0]['value'].values())

    return pd.DataFrame(data, index=[0])

  def get_post_comments(self, id):
    url = f'{self.endpoint}/{id}/comments'

    params = {
        'access_token': self.token,
        'summary': 1
    }

    response = requests.get(url, params)
    response_f = json.loads(response.text)

    if response.status_code == 400:
      return 0

    return response_f['summary']['total_count']

  def get_post_performance(self, df):
    posts = []

    for id in df['id']:
      df = self.get_post_data(id)
      df['comments'] = self.get_post_comments(id)
      df['engagement'] = df['post_clicks'] + df['reactions_total'] + df['comments']
      df['id'] = id
      
      posts.append(df)

    return pd.concat(posts, axis=0, ignore_index=True)
