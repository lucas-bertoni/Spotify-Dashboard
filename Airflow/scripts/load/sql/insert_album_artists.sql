INSERT INTO "SPOTIFY_DATA"."ALBUM_ARTISTS"(%s)
VALUES %%s
ON CONFLICT(album_id, artist_id) DO NOTHING;