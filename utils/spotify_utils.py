import spotipy
import datetime
import json
from datetime import datetime
from spotipy.oauth2 import SpotifyOAuth
from utils.cache_handler import CacheFileHandler

def get_user_recently_played_track(client_id, client_key, uri, scope, cache_filepath):
    
    cache_handler = CacheFileHandler(cache_path=cache_filepath)

    auth = SpotifyOAuth(client_id=client_id,
                    client_secret=client_key, 
                    redirect_uri=uri,
                    cache_path=cache_filepath,
                    scope=scope)

    token_info = auth.get_cached_token()

    if not token_info or auth.is_token_expired(token_info):
        token_info = auth.get_access_token()

    cache_handler.save_token_to_cache(token_info)

    response = spotipy.Spotify(auth=token_info['access_token'])

    response = response.current_user_recently_played()

    result = []
    
    for iterate_data in response.get("items"):
                
        artists = ""
        track = iterate_data.get("track").get("name")
        album = iterate_data.get("track").get("album").get("name")
        played_at = iterate_data.get("played_at")
        
        played_at = datetime.strptime(played_at, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
        
        for iter_artist in iterate_data.get("track").get("artists"):
            artists += iter_artist.get("name") + ", "
            artists = artists.rstrip(", ")
        
            payload = {
                "artist_name":artists,
                "title":track,
                "album_name":album,
                "played_at":played_at
            }
            result.append(payload)
        
    # user_recently_played_track = json.dumps(result, ensure_ascii=False)
    return result 

if __name__ == "__main__":
    
    client_id = 'd5f618d9068846ccb70441d7a34c2546'
    client_key = '06d502285c5d4be59c6f804f1d0113c1'
    uri = 'http://localhost:8088/callback'

    scope = "user-read-recently-played"
    cache_filepath = '/root/kafka/kafka_spark_postgresql/kafka_config/token.json'
    
    data = get_user_recently_played_track(client_id, client_key, uri, scope, cache_filepath)
    
    for i in data:
        
        artist = i.get("artist_name")
        title = i.get("title")
        album = i.get("album_name")
        played_at = i.get("played_track")
        
        payload = {
            "artist_name":artist,
            "title":title,
            "album_name":album,
            "played_at":played_at
        }
        print(payload)