  # === === === 
    # Meta - Facebook
    # === === ===

class Facebook():
  def __init__(self):
    self.token = os.environ['TOKEN']
    self.client_id = os.environ['CLIENT_ID']
    self.client_secret = os.environ['CLIENT_SECRET']
    self.endpoint = 'https://graph.facebook.com/v14.0' 
    self.fb_id = os.environ['FB_ID']

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