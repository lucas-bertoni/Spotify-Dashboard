INSERT INTO "SPOTIFY_DATA"."LISTENING_HISTORY"(%s)
VALUES %%s
ON CONFLICT(played_at) DO NOTHING;