import spotipy
import time
import os
import csv 
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
            'artist': track['artists'][0]['name'],
            'album': track['album']['name'],
            'id': track['id'],
            'duration_ms': track['duration_ms']
        })
    
    return tracks


def write_tracks_to_csv(tracks):
    with open('recent_24h.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['played_at', 'track_name', 'artist', 'album', 'track_id', 'duration_ms'])
        for track in tracks:
            writer.writerow([track['played_at'], track['name'], track['artist'], track['album'], track['id'], track['duration_ms']])


if __name__ == "__main__":
    tracks = get_last_24h_tracks()
    if tracks:
        print("Tracks played in the last 24 hours:")
        for track in tracks:
            print(f"{track['played_at']} - {track['name']} by {track['artist']}")
        write_tracks_to_csv(tracks)
    else:
        print("No tracks played in the last 24 hours.")