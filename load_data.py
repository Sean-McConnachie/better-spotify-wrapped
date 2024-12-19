from dotenv import load_dotenv
import spotipy
from spotipy import SpotifyClientCredentials
from tqdm import tqdm

from persistence import md5_dir, db_init, DB, DBInfo, DBPlay, DBSong, SpotifySongSpan, db_clear_user_data, \
    DBSpotipySong, DBSpotipyArtist, DBSpotipyAlbum, Config, load_model_json


def sync_spotify_data_to_db(db: DB, data_dir: str):
    curr_hash = md5_dir(data_dir)
    db_hash = DBInfo.get(db)
    if db_hash is not None and curr_hash == db_hash.last_dir_hash:
        return

    db_clear_user_data(db)

    spans = SpotifySongSpan.load_history(data_dir)
    filtered_spans = [s for s in spans if s.valid_span()]
    print(
        f"Total spans: {len(spans)}, valid spans: {len(filtered_spans)} (filtered out {len(spans) - len(filtered_spans)})")

    songs = {s.spotify_track_uri: DBSong.from_spotify_song_span(s) for s in filtered_spans}
    plays = [DBPlay.from_spotify_song_span(s) for s in filtered_spans]

    DBSong.insert_many(db, list(songs.values()))
    DBPlay.insert_many(db, plays)
    DBInfo.insert(db, curr_hash)


def sync_song_artist_album_data(db: DB):
    print("Syncing song, artist, and album data")
    sp = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials()
    )

    def batch_iter(iterable, batch_size, desc: str = None):
        for i in tqdm(
                range(0, len(iterable), batch_size),
                desc=desc
        ):
            yield iterable[i:i + batch_size]

    def make_spotipy_albums_data(d: dict) -> DBSpotipyAlbum:
        album_batch = [
            DBSpotipyAlbum(
                album_id=track['album']['id'],
                album_name=track['album']['name'],
                release_date=track['album']['release_date'],
                total_tracks=track['album']['total_tracks'],
            )
            for track in d['tracks']
        ]
        DBSpotipyAlbum.insert_many(db, album_batch)

    def fetch_spotipy_songs_data(ids: list[str]) -> DBSpotipySong:
        BATCH_SIZE = 50
        for batch in batch_iter(ids, BATCH_SIZE, desc="Fetching Spotify song data"):
            data = sp.tracks(batch)
            make_spotipy_albums_data(data)
            song_batch = [
                DBSpotipySong(
                    track_id=track['id'],
                    track_name=track['name'],
                    explicit=track['explicit'],
                    duration_ms=track['duration_ms'],
                    album_id=track['album']['id'],
                    artist_ids_csv=",".join(a['id'] for a in track['artists'])
                )
                for track in data['tracks']
            ]
            DBSpotipySong.insert_many(db, song_batch)

    def fetch_spotipy_artists_data(ids: list[str]) -> DBSpotipyArtist:
        BATCH_SIZE = 50
        for batch in batch_iter(ids, BATCH_SIZE, desc="Fetching Spotify artist data"):
            data = sp.artists(batch)
            artist_batch = [
                DBSpotipyArtist(
                    artist_id=artist['id'],
                    artist_name=artist['name'],
                    genres_csv=",".join(artist['genres']),
                    popularity=artist['popularity'],
                    followers=artist['followers']['total'],
                )
                for artist in data['artists']
            ]
            DBSpotipyArtist.insert_many(db, artist_batch)

    unq_song_ids = DBSong.get_all_song_ids(db)
    print(f"> Unique songs: {len(unq_song_ids)}")
    ex_song_ids = DBSpotipySong.get_all_song_ids(db)
    print(f"> Existing songs: {len(ex_song_ids)}")

    new_song_ids = list(set(unq_song_ids) - set(ex_song_ids))
    print(f"> New songs: {len(new_song_ids)}")
    fetch_spotipy_songs_data(new_song_ids)

    artists = DBSpotipySong.get_all_artist_ids(db)
    print(f"> Unique artists: {len(artists)}")
    ex_artist_ids = DBSpotipyArtist.get_all_artist_ids(db)
    print(f"> Existing artists: {len(ex_artist_ids)}")

    new_artist_ids = list(set(artists) - set(ex_artist_ids))
    print(f"> New artists: {len(new_artist_ids)}")
    fetch_spotipy_artists_data(new_artist_ids)


def main(cfg: Config):
    db = db_init(cfg.db_fp)
    sync_spotify_data_to_db(db, cfg.spotify_data_dir)
    sync_song_artist_album_data(db)


if __name__ == "__main__":
    load_dotenv()
    cfg = load_model_json(Config, "config.json")
    main(cfg)
