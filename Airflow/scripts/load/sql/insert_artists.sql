INSERT INTO "SPOTIFY_DATA"."ARTISTS"(%s)
VALUES %%s
ON CONFLICT(artist_id) DO NOTHING;;