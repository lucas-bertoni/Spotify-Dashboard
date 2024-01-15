import pandas as pd
import json

with open('/opt/airflow/files/temp/json/tracks.json', 'r') as json_file:
    tracks = json.load(json_file)
    song_rows = []
    artist_rows = []
    album_rows = []
    song_artist_rows = []
    song_album_rows = []
    album_artist_rows = []

    for track in tracks:
        # For the main tables
        song_row = {
            'song_id': track['id'],
            'song_name': track['name'],
            'song_duration_ms': track['duration_ms']
        }
        song_rows.append(song_row)

        album = track['album']
        album_row = {
            'album_id': album['id'],
            'album_name': album['name']
        }
        album_rows.append(album_row)

        track_artists = track['artists']
        for track_artist in track_artists:
            artist_row = {
                'artist_id': track_artist['id'],
                'artist_name': track_artist['name']
            }
            artist_rows.append(artist_row)

        # For the bridge tables
        song_artists = track['artists']
        for song_artist in song_artists:
            song_artist_row = {
                'song_id': track['id'],
                'artist_id': song_artist['id']
            }
            song_artist_rows.append(song_artist_row)

        song_album = track['album']
        song_album_row = {
            'song_id': track['id'],
            'album_id': song_album['id']
        }
        song_album_rows.append(song_album_row)
            

        album_artists = album['artists']
        for album_artist in album_artists:
            album_artist_row = {
                'album_id': album['id'],
                'artist_id': album_artist['id']
            }
            album_artist_rows.append(album_artist_row)



    songs_df = pd.DataFrame(song_rows)
    songs_df = songs_df.drop_duplicates()
    songs_df.to_csv('/opt/airflow/files/temp/csv/songs.csv', index=False)

    artists_df = pd.DataFrame(artist_rows)
    artists_df = artists_df.drop_duplicates()
    artists_df.to_csv('/opt/airflow/files/temp/csv/artists.csv', index=False)

    albums_df = pd.DataFrame(album_rows)
    albums_df = albums_df.drop_duplicates()
    albums_df.to_csv('/opt/airflow/files/temp/csv/albums.csv', index=False)

    song_artists_df = pd.DataFrame(song_artist_rows)
    song_artists_df = song_artists_df.drop_duplicates()
    song_artists_df.to_csv('/opt/airflow/files/temp/csv/song_artists.csv', index=False)

    song_albums_df = pd.DataFrame(song_album_rows)
    song_albums_df = song_albums_df.drop_duplicates()
    song_albums_df.to_csv('/opt/airflow/files/temp/csv/song_album.csv', index=False)

    album_artists_df = pd.DataFrame(album_artist_rows)
    album_artists_df = album_artists_df.drop_duplicates()
    album_artists_df.to_csv('/opt/airflow/files/temp/csv/album_artists.csv', index=False)