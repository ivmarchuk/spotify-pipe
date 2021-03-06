import requests
import datetime
import mysql.connector
from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine
import os 

db_user = os.environ.get('db_user')
db_pass = os.environ.get('db_pass')


def check_data(df: pd.DataFrame) -> bool:
        # empty  
        if df.empty: 
            print('There is no data to continue.')
            return False
        
        # Nulls
        if df.isnull().values.any():
            raise Exception('Found some NULL values.')
        
        # PK 
        if pd.Series(df['played_at']).is_unique:
            pass
        else: 
            raise Exception('Something wrong with Primary keys [play-time]. Try to check the data.')
        
        # Timestamp 
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

        timestamps = df["timestamp"].tolist()
        for timestamp in timestamps:
            if datetime.datetime.strptime(timestamp, '%Y-%m-%d') < yesterday:
                raise Exception('Something wrong with the timestamps, try to check data you downloaded.')
        
        return True




def etl_main(): 
    # Spotify credentials 
    username = 'SPOTIFY-USERNAME'
    token = 'SPOTIFY-API-TOKEN'
    
    # Get JSON data from API
    header = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {token}'.format(token = token)
    }
        
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days = 1)
    yesterday_unix = int(yesterday.timestamp()) * 1000
        
    request = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time = yesterday_unix), headers = header)
    data = request.json()   
    print(data)
        
        
    # Parsind JSON, creating DataFrame with played songs
    songs = []
    artist = []
    durations = []
    play_time = []
    time_st = []
    artist_id = []
        
    for song in data['items']:
        songs.append(song['track']['name'])
        artist.append(song['track']['album']['artists'][0]['name'])
        durations.append(song['track']['duration_ms'])
        play_time.append(song['played_at'])
        time_st.append(song['played_at'][0:10])   
        artist_id.append(song['track']['artists'][0]['id'])

    song_d = {
        'song_name': songs,
        'artist_name': artist,
        'duration': durations,
        'played_at': play_time,
        'timestamp': time_st
    }

    df_song = pd.DataFrame(song_d, columns = ['song_name', 'artist_name','duration', 'played_at', 'timestamp'])


    # Check validity of data
    if check_data(df = df_song):
        print('Data is valid')
    
    
    # MySQL database connection
    try: 
        connection = mysql.connector.connect(host = 'HOST',
                                            database = 'DB-NAME',
                                            user = db_user,
                                            password = db_pass)
        
        if connection.is_connected():
            db_info = connection.get_server_info()
            print('Connectoin was established correctly. Version:', db_info)
            
            cursor = connection.cursor()
            cursor.execute('select database();')
            
            # This method retrieves the next row of a query result set and returns a single sequence, or None if no more rows are available
            record = cursor.fetchone()
            print('Database: ', record)
            
            engine = create_engine("mysql+pymysql://{user}:{password}@localhost/{database}"
                        .format(user="USERNAME",
                                password= db_pass,
                                database= db_user))
            
            cursor = connection.cursor()
            print('Cursor was created successfuly')

    except Error as e:
        print('Error connection', e)  
    
    # Load
    # sql_query = '''
    # CREATE TABLE IF NOT EXISTS songs(
    #     song_name VARCHAR(200),
    #     artist_name VARCHAR(200),
    #     played_at VARCHAR(200),
    #     timestamp VARCHAR(200),
    #     duration VARCHAR(200), 
    #     band_genfre VARCHAR(200),
    #     PRIMARY KEY (played_at))'''

    # cursor.execute(sql_query)
    # print('Database works correctly')
    
    try: 
        df_song.to_sql("songs2", engine, index=False, if_exists='append')
        print('Data Loaded.')
    except: 
        print('Data is already in database')

    if connection.is_connected():
        cursor.close()
        connection.close()
        print('Connection closed')
    
    
