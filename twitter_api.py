#Modules a utilizar
import tweepy
from tweepy import Stream
import configparser
from pandas import DataFrame
from fastparquet import write
import os

#VARIABLES GLOBALES
#Letedo datos de el arcivo config.ini
config = configparser.ConfigParser()
config.read('config.ini')

#keys generadas y extraidas desde config.ini
api_key = config['twitter']['api_key']
api_key_secret =  config['twitter']['api_key_secret']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']

def download_tweets():
    #Usando Autenticacion y asignamos nombre de usuario a revisar, a la variable user_name
    user_name = "aboveandbeyond"
    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    #Instancia a API
    api = tweepy.API(auth)

    #Tweets publicados por el usuario elegido evitamos retweets y tomamos todos los tweets 
    data_tweet_user = api.user_timeline(screen_name = user_name,
                                        count = 200,
                                        include_rts = False,
                                        tweet_mode = 'extended'
                                        )

    #Obetenemos solo el valor de author id.                                       
    author_id = api.get_user(screen_name = user_name).id_str

    #Generamos una lista de listas en el iterador para almacenar los rows que sera parte de un dataframe que se usara posteriormente
    
    count = 0
    #tweet_lst = []
    tpl_row = []
    
    for tweet in data_tweet_user:
        if count < 100:            
            tpl_row.append([author_id, tweet.full_text, tweet.source, tweet.created_at])
            count += 1

    #Con la lista de listas obtenida podremos generar el dataframe que sera usado posteriorment para escribir el archivo parquet 
    df_tweets = DataFrame(tpl_row, columns=["author_id", "text", "source", "created_at"])

    #se genera el archivo .parq con compresion GZIP y usamos el dataframe generado
    path_parq = 'output_file.parquet'
    write(path_parq, df_tweets, compression='GZIP')
    current_directory = os.getcwd()
    
    return df_tweets, path_parq, current_directory

if __name__ == '__main__':
    df_tweets, path_parq, current_directory = download_tweets()
    print(df_tweets)
    print('El archivo ' + path_parq + ' fue generado en la siguiente ruta ' + current_directory)


