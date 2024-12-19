import json

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id="7a147125e91241caa6fe166112d4284d",
                                                           client_secret="47e1105db200405a8c254307eda7fadd"))


# Spotify track ID (example)
track_id = "0mBKv9DkYfQHjdMcw2jdyI"  # Replace with your desired track ID

# Get track details
track_info = sp.track(track_id)
artist_id = track_info['artists'][0]['id']

# Get artist details (to retrieve genres)
artist_info = sp.artist(artist_id)
genres = artist_info['genres']

# # Print the genres
# print(f"Track Name: {track_info['name']}")
# print(f"Artist: {track_info['artists'][0]['name']}")
# print("Genres:", genres)

print(json.dumps(track_info, indent=4))

print("\n\n\n\n\n")

print(json.dumps(artist_info, indent=4))