import requests
import json
import pandas as pd
import datetime as dt
import re
import time
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread

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

# Formata datetime para unix
def date_to_unix(date):
  return time.mktime(date.timetuple())

class Instagram():
  def __init__(self, token, client_id, client_secret, ig_id):
    self.token = token
    self.client_id = client_id
    self.client_secret = client_secret
    self.endpoint = 'https://graph.facebook.com/v14.0' 
    self.ig_id = ig_id

  # === === ===
  # Meta - Instagram
  # === === ===

  def refresh_token(self):
   url = f"{self.endpoint}/oauth/access_token?grant_type=fb_exchange_token&client_id={self.client_id}&client_secret={self.client_secret}&fb_exchange_token={self.token}"

   return json.loads(requests.get(url).text)

  # Retorna DataFrame contendo performance de reels
  def get_reels_data(self, id):
    url = f'{self.endpoint}/{id}/insights'
    

    params = {
        'metric': 'comments,likes,plays,shares,saved,reach,total_interactions',
        'access_token': self.token
    }

    response = json.loads(requests.get(url, params).text)
    return (pd.DataFrame(response['data']))

  # Retorna DataFrame contendo performance de carrossel
  def get_carousel_data(self, id):
    url = f'{self.endpoint}/{id}/insights'
    
    params = {
        'metric': 'carousel_album_engagement,carousel_album_impressions,carousel_album_reach,carousel_album_video_views,carousel_album_saved',
        'access_token': self.token
    }

    response = json.loads(requests.get(url, params).text)
    return pd.DataFrame(response['data'])

  def get_image_data(self, id):
    url = f'{self.endpoint}/{id}/insights'

    params = {
        'metric': 'engagement,impressions,reach,saved',
        'access_token': self.token
    }

    response = json.loads(requests.get(url, params).text)
    return pd.DataFrame(response['data'])

  def get_stories_data(self, id):
    url = f'{self.endpoint}/{id}/insights'

    params = {
        'metric': 'exits,impressions,reach,replies,taps_forward,taps_back',
        'access_token': insta.token
    }

    response = json.loads(requests.get(url, params).text)
    return pd.DataFrame(response['data'])

  # Retorna performance geral das mídias utilizando as duas funções acima
  def get_media_performance(self, df):
    reels = {}
    carousel = {}
    image = {}
    reels_f = []
    carousel_f = []
    image_f = []

    df_reels_final = None
    df_carousel_final = None
    df_image_final = None

    for id, media_type in zip(df['id'], df['media_type']):
      if media_type == 'VIDEO':
        reels[id] = format_dataframe(self.get_reels_data(id))
      elif media_type == 'CAROUSEL_ALBUM':
        carousel[id] = format_dataframe(self.get_carousel_data(id))
      elif media_type == 'IMAGE':
        image[id] = format_dataframe(self.get_image_data(id))
      else:
        raise Exception('Yuki não fez :)')

    # Reels
    for key, df in reels.items():
      df.iloc[0, 0] = key
      df.rename(columns={'name': 'id',
                         'plays': 'impressions'}, inplace=True)
      reels_f.append(df)

    if len(reels_f) > 0:
      df_reels_final = pd.concat(reels_f, axis=0, ignore_index=True)
      df_reels_final['engagement'] = df_reels_final['comments'] + df_reels_final['likes'] + df_reels_final['saved']

    # Carrossel
    for key, df in carousel.items():
      df.iloc[0, 0] = key
      df.rename(columns={'name': 'id',
                        'carousel_album_engagement': 'engagement',
                        'carousel_album_impressions': 'impressions',
                        'carousel_album_reach': 'reach',
                        'carousel_album_saved': 'saved'}, inplace=True)
      carousel_f.append(df)
    
    if len(carousel_f) > 0:
      df_carousel_final = pd.concat(carousel_f, axis=0, ignore_index=True)
      df_carousel_final['engagement'] += df_carousel_final['saved']

    # Imagem
    for key, df in image.items():
      df.iloc[0, 0] = key
      df.rename(columns={'name': 'id'}, inplace=True)
      image_f.append(df)

    if len(image_f) > 0:
      df_image_final = pd.concat(image_f, axis=0, ignore_index=True)
      df_image_final['engagement'] += df_image_final['saved']


    df_final = pd.concat([df_carousel_final, df_reels_final, df_image_final])

    dfs = {
        'carousel': df_carousel_final,
        'reels': df_reels_final,
        'image': df_image_final,
        'merged': df_final
    }

    return dfs

  # Retorna performance geral da página
  def get_ig_page_data(self, data_inicial, data_final):
    url = f'{self.endpoint}/{self.ig_id}/insights'

    params = {
        'metric': 'impressions,reach,follower_count,profile_views',
        'period': 'day',
        'since': data_inicial,
        'until': data_final,
        'access_token': self.token
    }

    response = json.loads(requests.get(url, params).text)
    return (pd.DataFrame(response['data']))

  # Retorna id do hashtag inputado
  def get_hashtag_id(self, hashtag):
    url = 'https://graph.facebook.com/v14.0/ig_hashtag_search'

    params = {
       'user_id': self.ig_id,
        'q': hashtag,
        'access_token': self.token
    }

    response = json.loads(requests.get(url, params).text)
    return response['data'][0]['id']

  # Retorna dados das principais mídias com o hashtag inputado
  def get_hashtag_data(self, hashtag):
    hashtag_id = self.get_hashtag_id(hashtag)

    url = f'https://graph.facebook.com/{hashtag_id}/top_media'

    params = {
      'user_id': self.ig_id,
     'fields': 'caption,id,media_type,comments_count,like_count',
     'access_token': self.token
    }

    response = json.loads(requests.get(url, params).text)
    return pd.DataFrame(response['data'])