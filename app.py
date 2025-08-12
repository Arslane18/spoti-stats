import spotipy
import time
import os 
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()


CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")

SCOPE = "user-read-recently-played"


def get_last_24h_tracks():
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE
    ))

    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    after_timestamp = int(yesterday.timestamp() * 1000)  

    results = sp.current_user_recently_played(after=after_timestamp)

    tracks = []
    for item in results['items']:
        track = item['track']
        played_at = datetime.strptime(item['played_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
        tracks.append({
            'played_at': played_at,
            'name': track['name'],
            'artist': track['artists'][0]['name']
        })
    
    return tracks
