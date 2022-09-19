from meta import *

def main():
  meta = Meta()

  data_inicial = date_to_unix(dt.datetime(2022, 9, 12))
  data_final = date_to_unix(dt.datetime(2022, 9, 19))

  df_ig = meta.get_ig_data(data_inicial, data_final) # retorna todas as mídias do Insta
  df_ig = format_timestamp(df_ig) # formata o timestamp para date legível
  df_midia = meta.get_media_performance(df_ig) # retorna a performance das mídias presentesm em df_ig

  df_ig_final = df_midia.merge(df_ig, on='id') # join para termos o nome de cada mídia 

  # --- --- ---

  df_posts = meta.get_fb_posts()
  df_fb_performance = meta.get_post_performance(df_posts)

  df_fb_final = df_fb_performance.merge(df_posts, on='message')

  # --- --- ---

  dfs_meta = {
    'instagram': df_ig_final,
    'facebook': df_fb_final
  }
  
  return dfs_meta

if __name__ == '__main__':
  main()