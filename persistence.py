import os
import json
from typing import Optional
from pydantic import BaseModel
import sqlite3
import datetime as dt
from sqlite3 import Error
from dataclasses import dataclass
from tqdm import tqdm
from typing import Type, TypeVar

import hashlib
from _hashlib import HASH as Hash
from pathlib import Path
from typing import Union

DB = sqlite3.Connection

T = TypeVar('T')


def load_model_json(model: Type[T], fp: str) -> T:
    with open(fp, 'r') as f:
        return model.model_validate_json(f.read())


class Config(BaseModel):
    spotify_data_dir: str
    db_fp: str
    ollama_model: str
    ollama_host: str
    classified_genres_fp: str
    fundamental_genres: list[str]

class SpotifySongSpan(BaseModel):
    ts: dt.datetime
    platform: str
    ms_played: int
    conn_country: str
    ip_addr: Optional[str]
    master_metadata_track_name: Optional[str]
    master_metadata_album_artist_name: Optional[str]
    master_metadata_album_album_name: Optional[str]
    spotify_track_uri: Optional[str]
    episode_name: Optional[str]
    episode_show_name: Optional[str]
    spotify_episode_uri: Optional[str]
    reason_start: Optional[str]
    reason_end: Optional[str]
    shuffle: bool
    skipped: Optional[bool]
    offline: Optional[bool]
    offline_timestamp: Optional[int]
    incognito_mode: bool

    @staticmethod
    def load_history(dir: str) -> list["SpotifySongSpan"]:
        files = [f for f in os.listdir(dir) if f.endswith(".json")]
        songs = []
        for f in tqdm(
                files,
                desc="Loading Spotify song spans",
        ):
            with open(f"{dir}/{f}", 'r') as file:
                data = json.load(file)
                songs.extend([SpotifySongSpan(**d) for d in data])
        songs.sort(key=lambda x: x.ts)
        return songs

    def valid_span(self) -> bool:
        return self.spotify_track_uri is not None \
            and self.master_metadata_track_name is not None \
            and self.master_metadata_album_artist_name is not None \
            and self.master_metadata_album_album_name is not None \
            and self.ts is not None \
            and self.ms_played is not None


@dataclass
class DBSong:
    track_id: str
    track_name: str
    album_artist_name: str
    album_name: str

    @staticmethod
    def from_spotify_song_span(s: SpotifySongSpan):
        assert s.spotify_track_uri is not None and s.master_metadata_track_name is not None and s.master_metadata_album_artist_name is not None and s.master_metadata_album_album_name is not None
        return DBSong(
            s.spotify_track_uri.split(":")[-1],
            s.master_metadata_track_name,
            s.master_metadata_album_artist_name,
            s.master_metadata_album_album_name
        )

    @staticmethod
    def insert_many(db: DB, songs: list["DBSong"]) -> None:
        c = db.cursor()
        c.executemany("INSERT INTO songs (track_id, track_name, album_artist_name, album_name) VALUES (?, ?, ?, ?)",
                      [(s.track_id, s.track_name, s.album_artist_name, s.album_name) for s in songs])
        db.commit()

    @staticmethod
    def get_all_song_ids(db: DB) -> list[str]:
        c = db.cursor()
        c.execute("SELECT track_id FROM songs")
        rows = c.fetchall()
        return [row[0] for row in rows]

    @staticmethod
    def get_all(db: DB) -> list["DBSong"]:
        c = db.cursor()
        c.execute("SELECT * FROM songs")
        rows = c.fetchall()
        return [DBSong(row[0], row[1], row[2], row[3]) for row in rows]


@dataclass
class DBPlay:
    play_id: Optional[int]
    ts: dt.datetime
    track_id: str
    ms_played: int

    @staticmethod
    def from_spotify_song_span(s: SpotifySongSpan):
        assert s.spotify_track_uri is not None and s.ts is not None and s.ms_played is not None
        return DBPlay(
            None,
            s.ts,
            s.spotify_track_uri.split(":")[-1],
            s.ms_played
        )

    @staticmethod
    def insert_many(db: DB, plays: list["DBPlay"]) -> None:
        c = db.cursor()
        c.executemany("INSERT INTO plays (ts, track_id, ms_played) VALUES (?, ?, ?)",
                      [(p.ts, p.track_id, p.ms_played) for p in plays])
        db.commit()

    @staticmethod
    def get_all(db: DB) -> list["DBPlay"]:
        c = db.cursor()
        c.execute("SELECT * FROM plays")
        rows = c.fetchall()
        return [DBPlay(row[0], row[1], row[2], row[3]) for row in rows]


@dataclass
class DBInfo:
    hash_id: int
    last_dir_hash: str

    @staticmethod
    def get(db: DB) -> Optional["DBInfo"]:
        c = db.cursor()
        c.execute("SELECT * FROM info ORDER BY hash_id DESC LIMIT 1")
        row = c.fetchone()
        if row is None:
            return None
        return DBInfo(row[0], row[1])

    @staticmethod
    def insert(db: DB, dir_hash: str) -> "DBInfo":
        c = db.cursor()
        c.execute("INSERT INTO info (last_dir_hash) VALUES (?)", (dir_hash,))
        db.commit()
        return DBInfo(c.lastrowid, dir_hash)


@dataclass
class DBSpotipySong:
    track_id: str
    track_name: str
    explicit: bool
    duration_ms: int
    album_id: str
    artist_ids_csv: str

    @staticmethod
    def get_all_song_ids(db: DB) -> list[str]:
        c = db.cursor()
        c.execute("SELECT track_id FROM spotipy_songs")
        rows = c.fetchall()
        return [row[0] for row in rows]

    @staticmethod
    def get_all_artist_ids(db: DB) -> list[str]:
        c = db.cursor()
        c.execute("SELECT artist_ids_csv FROM spotipy_songs")
        rows = c.fetchall()
        artist_csvs = [row[0] for row in rows]
        return list(set([artist_id for csv in artist_csvs for artist_id in csv.split(",")]))

    @staticmethod
    def insert_many(db: DB, songs: list["DBSpotipySong"]) -> None:
        c = db.cursor()
        c.executemany(
            "INSERT INTO spotipy_songs (track_id, track_name, explicit, duration_ms, album_id, artist_ids_csv) VALUES (?, ?, ?, ?, ?, ?)"
            "ON CONFLICT(track_id) DO NOTHING",
            [(s.track_id, s.track_name, s.explicit, s.duration_ms, s.album_id, s.artist_ids_csv) for s in songs])
        db.commit()

    @staticmethod
    def get_all(db: DB) -> list["DBSpotipySong"]:
        c = db.cursor()
        c.execute("SELECT * FROM spotipy_songs")
        rows = c.fetchall()
        return [DBSpotipySong(row[0], row[1], row[2], row[3], row[4], row[5]) for row in rows]


@dataclass
class DBSpotipyAlbum:
    album_id: str
    album_name: str
    release_date: str
    total_tracks: int

    @staticmethod
    def insert_many(db: DB, albums: list["DBSpotipyAlbum"]) -> None:
        c = db.cursor()
        c.executemany(
            "INSERT INTO spotipy_albums (album_id, album_name, release_date, total_tracks) VALUES (?, ?, ?, ?)"
            "ON CONFLICT(album_id) DO NOTHING",
            [(a.album_id, a.album_name, a.release_date, a.total_tracks) for a in albums])
        db.commit()

    @staticmethod
    def get_all(db: DB) -> list["DBSpotipyAlbum"]:
        c = db.cursor()
        c.execute("SELECT * FROM spotipy_albums")
        rows = c.fetchall()
        return [DBSpotipyAlbum(row[0], row[1], row[2], row[3]) for row in rows]


@dataclass
class DBSpotipyArtist:
    artist_id: str
    artist_name: str
    genres_csv: str
    popularity: int
    followers: int

    @staticmethod
    def get_all_artist_ids(db: DB) -> list[str]:
        c = db.cursor()
        c.execute("SELECT artist_id FROM spotipy_artists")
        rows = c.fetchall()
        return [row[0] for row in rows]

    @staticmethod
    def insert_many(db: DB, artists: list["DBSpotipyArtist"]) -> None:
        c = db.cursor()
        c.executemany(
            "INSERT INTO spotipy_artists (artist_id, artist_name, genres_csv, popularity, followers) VALUES (?, ?, ?, ?, ?)"
            "ON CONFLICT(artist_id) DO NOTHING",
            [(a.artist_id, a.artist_name, a.genres_csv, a.popularity, a.followers) for a in artists])
        db.commit()

    @staticmethod
    def get_all(db: DB) -> list["DBSpotipyArtist"]:
        c = db.cursor()
        c.execute("SELECT * FROM spotipy_artists")
        rows = c.fetchall()
        return [DBSpotipyArtist(row[0], row[1], row[2], row[3], row[4]) for row in rows]


def db_init(db_file: str) -> DB:
    conn = db_create_conn(db_file)
    db_execute(conn, """
    CREATE TABLE IF NOT EXISTS songs (
        track_id TEXT NOT NULL PRIMARY KEY,
        track_name TEXT NOT NULL,
        album_artist_name TEXT,
        album_name TEXT
    )""")
    db_execute(conn, """
    CREATE TABLE IF NOT EXISTS plays (
        play_id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts DATETIME NOT NULL,
        track_id TEXT NOT NULL,
        ms_played INTEGER NOT NULL
    )""")
    db_execute(conn, """
    CREATE TABLE IF NOT EXISTS info (
        hash_id INTEGER PRIMARY KEY AUTOINCREMENT,
        last_dir_hash TEXT
    )""")
    db_execute(conn, """
    CREATE TABLE IF NOT EXISTS spotipy_songs (
        track_id TEXT NOT NULL PRIMARY KEY,
        track_name TEXT NOT NULL,
        explicit BOOLEAN NOT NULL,
        duration_ms INTEGER NOT NULL,
        album_id TEXT NOT NULL,
        artist_ids_csv TEXT NOT NULL
    )""")
    db_execute(conn, """
    CREATE TABLE IF NOT EXISTS spotipy_albums (
        album_id TEXT NOT NULL PRIMARY KEY,
        album_name TEXT NOT NULL,
        release_date TEXT NOT NULL,
        total_tracks INTEGER NOT NULL
    )""")
    db_execute(conn, """
    CREATE TABLE IF NOT EXISTS spotipy_artists (
        artist_id TEXT NOT NULL PRIMARY KEY,
        artist_name TEXT NOT NULL,
        genres_csv TEXT NOT NULL,
        popularity INTEGER NOT NULL,
        followers INTEGER NOT NULL
    )""")
    return conn


def db_clear_user_data(db: DB) -> None:
    c = db.cursor()
    c.execute("DELETE FROM plays")
    c.execute("DELETE FROM songs")
    db.commit()


def db_create_conn(db_file: str) -> DB:
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn


def db_execute(conn: DB, sql: str) -> None:
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def md5_update_from_file(filename: Union[str, Path], hash: Hash) -> Hash:
    assert Path(filename).is_file()
    with open(str(filename), "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash.update(chunk)
    return hash


def md5_file(filename: Union[str, Path]) -> str:
    return str(md5_update_from_file(filename, hashlib.md5()).hexdigest())


def md5_update_from_dir(directory: Union[str, Path], hash: Hash) -> Hash:
    assert Path(directory).is_dir()
    for path in sorted(Path(directory).iterdir(), key=lambda p: str(p).lower()):
        hash.update(path.name.encode())
        if path.is_file():
            hash = md5_update_from_file(path, hash)
        elif path.is_dir():
            hash = md5_update_from_dir(path, hash)
    return hash


def md5_dir(directory: Union[str, Path]) -> str:
    return str(md5_update_from_dir(directory, hashlib.md5()).hexdigest())
