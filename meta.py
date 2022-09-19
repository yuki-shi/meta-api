import requests
import json
import pandas as pd
import datetime as dt
import re
import time
import os
from dotenv import load_dotenv

load_dotenv()


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

class Meta():
  def __init__(self):
    self.token = os.environ['TOKEN']
    self.client_id = os.environ['CLIENT_ID']
    self.client_secret = os.environ['CLIENT_SECRET']
    self.endpoint = 'https://graph.facebook.com/v14.0' 
    self.fb_id = os.environ['FB_ID']
    self.ig_id = os.environ['IG_ID']

  # === === ===
  # Meta - Instagram
  # === === ===

  def refresh_token(self):
   url = f"{self.endpoint}/oauth/access_token?grant_type=fb_exchange_token&client_id={self.client_id}&client_secret={self.client_secret}&fb_exchange_token={self.token}"

   return json.loads(requests.get(url).text)

  # Retorna DataFrame contendo midias postadas no Insta em um range de datas
  def get_ig_data(self, data_inicial, data_final):
    url = f'{self.endpoint}/{self.ig_id}/media'

    params = {
        'fields': 'id,caption,media_type,media_url,permalink,timestamp',
        'since': data_inicial,
        'until': data_final,
        'limit': 50,
        'access_token': self.token
    }

    response = json.loads(requests.get(url, params).text)
    return (pd.DataFrame(response['data']))

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
        'access_token': self.token
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

    for key, df in reels.items():
      df.iloc[0, 0] = key
      df.rename(columns={'name': 'id'}, inplace=True)
      reels_f.append(df)

    for key, df in carousel.items():
      df.iloc[0, 0] = key
      df.rename(columns={'name': 'id',
                        'carousel_album_engagement': 'engagement',
                        'carousel_album_impressions': 'plays',
                        'carousel_album_reach': 'reach',
                        'carousel_album_saved': 'saved'}, inplace=True)
      carousel_f.append(df)

    for key, df in image.items():
      df.iloc[0, 0] = key
      df.rename(columns={'name': 'id'}, inplace=True)
      image_f.append(df)

    if len(carousel_f) > 0:
      df_carousel_final = pd.concat(carousel_f, axis=0, ignore_index=True)
      df_carousel_final['engagement'] += df_carousel_final['saved']

    if len(reels_f) > 0:
      df_reels_final = pd.concat(reels_f, axis=0, ignore_index=True)
      df_reels_final['engagement'] = df_reels_final['comments'] + df_reels_final['likes'] + df_reels_final['saved']

    if len(image_f) > 0:
      df_image_final = pd.concat(image_f, axis=0, ignore_index=True)
      df_image_final['engagement'] += df_image_final['saved']

    df_final = pd.concat([df_carousel_final, df_reels_final, df_image_final])

    return df_final

  def get_stories_performance(self, ids):
    stories = {}
    stories_f = []

    for id in ids:
      stories[id] = format_dataframe(self.get_stories_data(id))

    for key, value in stories.items():
      stories.iloc[0, 0] = key
      stories.rename(columns={'name': 'id'}, inplace=True)
      stories.append(stories)

    return pd.concat(stories_f, axis=0, ignore_index=True)

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

    # === === === 
    # Meta - Facebook
    # === === ===

  def get_fb_posts(self):
    posts = json.loads(requests.get(f'{self.endpoint}/{self.fb_id}/posts?access_token={self.access_token}&limit=100').text)
    df = pd.DataFrame(posts['data'])
    df['created_time'] = pd.to_datetime(df['created_time'])
    df['created_time'] = df['created_time'].apply(lambda x: x.strftime('%Y-%m-%d'))

    return df

  def get_post_data(self, id):
    url = f'{self.endpoint}/{id}/insights'

    params = {
        'metric': 'post_clicks,post_clicks_unique,post_impressions,post_impressions_unique,post_engaged_users',
        'access_token': access_token
    }

    response = json.loads(requests.get(url, params).text)
    df = pd.DataFrame(response['data'])
    return format_dataframe(df)

  def get_post_performance(self, df):
    posts = []

    for message, id in zip(df['message'], df['id']):
      df = self.get_post_data(id)
      df['message'] = message
      df.drop('name', axis=1, inplace=True)
      df['url'] = f'https://facebook.com/{re.findall(r"(?<=_).*", id)[0]}'
      
      posts.append(df)

    return pd.concat(posts, axis=0, ignore_index=True)