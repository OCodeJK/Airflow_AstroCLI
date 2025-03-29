from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


_CONN_ID = "postgres_conn_spotify"  # define this connection in the Airflow UI
_TABLE = "top_track_table"

# Your Spotify API credentials
SPOTIPY_CLIENT_ID = "ENTER YOUR CLIENT ID HERE"
SPOTIPY_CLIENT_SECRET = "ENTER YOUR CLIENT SECRET HERE"

@dag(
    start_date=datetime(2025,3,24),
    schedule="@daily",
    catchup=False,
    template_searchpath=["include"],
)
def spotify_dag():

    @task
    def extract():
        import pandas as pd
        import spotipy 
        from spotipy.oauth2 import SpotifyClientCredentials
        import random

        # Set up authentication
        # Use Client Credentials Flow (No need for user interaction)
        auth_manager = SpotifyClientCredentials(
            client_id=SPOTIPY_CLIENT_ID,
            client_secret=SPOTIPY_CLIENT_SECRET
        )
        sp = spotipy.Spotify(auth_manager=auth_manager)

        # hard-coded artist name because it cannot pull an artist name randomly
        artist_names = ["Bruno Mars", "Lady Gaga", "The Weeknd", "Kendrick Lamar", "Billie Eilish", "Coldplay", "SZA", "Rihanna", "Bad Bunny", "Taylor Swift"]
        artist_name = random.choice(artist_names)
        
        # search the artist ID
        artist = sp.search(q=artist_name, type="artist", limit=10)
        artist_id = artist["artists"]["items"][0]["id"]
        print(f"\n{artist_name}'s Spotify Artist ID is {artist_id}")

        # search for artist top tracks
        top_tracks = sp.artist_top_tracks(artist_id=artist_id)
        track_info = []
        for tracks in top_tracks["tracks"]:
            track_name = tracks["name"]
            track_release_date = tracks["album"]["release_date"]
            track_popularity = tracks["popularity"]
            track_duration = tracks["duration_ms"]
            track_explicit = tracks["explicit"]
            track_type = tracks["type"]


            # Add the track info as a dictionary to the list
            track_info.append({
                "Name": track_name,
                "Duration": track_duration,
                "Explicit": track_explicit,
                "Release": track_release_date,
                "Popularity": track_popularity,
                "Type": track_type,
            })

        # Create a DataFrame from the list of track info
        df = pd.DataFrame(track_info)
        return df

    @task
    def transform_dataframe(dataframe):
        import pandas as pd

        # Convert 'Release Date' to datetime format
        dataframe['Release'] = pd.to_datetime(dataframe['Release'], errors='coerce')

        # Sort the DataFrame by 'Popularity' in descending order
        dataframe = dataframe.sort_values(by='Popularity', ascending=False)

        # Add a new column 'Track Age' based on the 'Release' to calculate how old the track is
        dataframe['Age'] = (pd.to_datetime('today') - dataframe['Release']).dt.days // 365  # Track age in years

        # function to change to minutes:second
        def ms_to_minutes_seconds(ms):
            total_seconds = ms // 1000 # convert to milliseconds to second
            minutes = total_seconds // 60 # get minutes
            seconds = total_seconds // 60 # get remaining seconds
            return f"00:{int(minutes):02}:{int(seconds):02}"
        
        # Change Duration to minutes:seconds
        dataframe['Duration'] = dataframe['Duration'].apply(ms_to_minutes_seconds)

        return dataframe
    
    _create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=_CONN_ID,
        sql="create_top_track_table.sql",
        params={"table": _TABLE},
    )

    @task
    def load_dataframe(transformed_dataframe):
        from sqlalchemy import create_engine

        engine = create_engine('postgresql+psycopg2://postgres:postgres@postgres/spotify')

        #convert all column name to lowercase
        transformed_dataframe.columns = transformed_dataframe.columns.str.lower() 
        transformed_dataframe.to_sql(_TABLE, engine, if_exists="append", index=False)


    _extract = extract()
    _transform = transform_dataframe(dataframe=_extract)
    _load = load_dataframe(transformed_dataframe=_transform)
    chain(_create_table, _load)



spotify_dag()