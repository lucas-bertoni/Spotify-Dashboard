--
-- PostgreSQL database dump
--

-- Dumped from database version 16.1
-- Dumped by pg_dump version 16.1

-- Started on 2024-01-15 14:24:13

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 6 (class 2615 OID 16503)
-- Name: SPOTIFY_DATA; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA "SPOTIFY_DATA";


ALTER SCHEMA "SPOTIFY_DATA" OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 220 (class 1259 OID 16564)
-- Name: ALBUMS; Type: TABLE; Schema: SPOTIFY_DATA; Owner: postgres
--

CREATE TABLE "SPOTIFY_DATA"."ALBUMS" (
    album_id character varying NOT NULL,
    album_name character varying
);


ALTER TABLE "SPOTIFY_DATA"."ALBUMS" OWNER TO postgres;

--
-- TOC entry 223 (class 1259 OID 16629)
-- Name: ALBUM_ARTISTS; Type: TABLE; Schema: SPOTIFY_DATA; Owner: postgres
--

CREATE TABLE "SPOTIFY_DATA"."ALBUM_ARTISTS" (
    album_id character varying NOT NULL,
    artist_id character varying NOT NULL
);


ALTER TABLE "SPOTIFY_DATA"."ALBUM_ARTISTS" OWNER TO postgres;

--
-- TOC entry 219 (class 1259 OID 16557)
-- Name: ARTISTS; Type: TABLE; Schema: SPOTIFY_DATA; Owner: postgres
--

CREATE TABLE "SPOTIFY_DATA"."ARTISTS" (
    artist_id character varying NOT NULL,
    artist_name character varying
);


ALTER TABLE "SPOTIFY_DATA"."ARTISTS" OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 16518)
-- Name: CONFIG; Type: TABLE; Schema: SPOTIFY_DATA; Owner: postgres
--

CREATE TABLE "SPOTIFY_DATA"."CONFIG" (
    last_extract_time timestamp without time zone
);


ALTER TABLE "SPOTIFY_DATA"."CONFIG" OWNER TO postgres;

--
-- TOC entry 216 (class 1259 OID 16505)
-- Name: LISTENING_HISTORY; Type: TABLE; Schema: SPOTIFY_DATA; Owner: postgres
--

CREATE TABLE "SPOTIFY_DATA"."LISTENING_HISTORY" (
    played_at timestamp with time zone NOT NULL,
    duration_played_ms integer NOT NULL,
    song_id character varying NOT NULL
);


ALTER TABLE "SPOTIFY_DATA"."LISTENING_HISTORY" OWNER TO postgres;

--
-- TOC entry 218 (class 1259 OID 16542)
-- Name: SONGS; Type: TABLE; Schema: SPOTIFY_DATA; Owner: postgres
--

CREATE TABLE "SPOTIFY_DATA"."SONGS" (
    song_id character varying NOT NULL,
    song_name character varying,
    song_duration_ms integer
);


ALTER TABLE "SPOTIFY_DATA"."SONGS" OWNER TO postgres;

--
-- TOC entry 222 (class 1259 OID 16622)
-- Name: SONG_ALBUM; Type: TABLE; Schema: SPOTIFY_DATA; Owner: postgres
--

CREATE TABLE "SPOTIFY_DATA"."SONG_ALBUM" (
    song_id character varying NOT NULL,
    album_id character varying NOT NULL
);


ALTER TABLE "SPOTIFY_DATA"."SONG_ALBUM" OWNER TO postgres;

--
-- TOC entry 221 (class 1259 OID 16615)
-- Name: SONG_ARTISTS; Type: TABLE; Schema: SPOTIFY_DATA; Owner: postgres
--

CREATE TABLE "SPOTIFY_DATA"."SONG_ARTISTS" (
    song_id character varying NOT NULL,
    artist_id character varying NOT NULL
);


ALTER TABLE "SPOTIFY_DATA"."SONG_ARTISTS" OWNER TO postgres;

--
-- TOC entry 224 (class 1259 OID 16680)
-- Name: history; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.history AS
 SELECT lh.played_at,
    s.song_name AS song,
    array_agg(art.artist_name) AS artists,
    alb.album_name AS album,
    s.song_duration_ms,
    lh.duration_played_ms,
    round(((lh.duration_played_ms / s.song_duration_ms))::numeric, 2) AS amount_played
   FROM ((((("SPOTIFY_DATA"."LISTENING_HISTORY" lh
     JOIN "SPOTIFY_DATA"."SONGS" s ON (((lh.song_id)::text = (s.song_id)::text)))
     JOIN "SPOTIFY_DATA"."SONG_ARTISTS" sart ON (((s.song_id)::text = (sart.song_id)::text)))
     JOIN "SPOTIFY_DATA"."ARTISTS" art ON (((sart.artist_id)::text = (art.artist_id)::text)))
     JOIN "SPOTIFY_DATA"."SONG_ALBUM" salb ON (((s.song_id)::text = (salb.song_id)::text)))
     JOIN "SPOTIFY_DATA"."ALBUMS" alb ON (((salb.album_id)::text = (alb.album_id)::text)))
  GROUP BY lh.played_at, s.song_name, alb.album_name, lh.duration_played_ms, s.song_duration_ms
  ORDER BY lh.played_at DESC;


ALTER VIEW public.history OWNER TO postgres;

--
-- TOC entry 4731 (class 2606 OID 16570)
-- Name: ALBUMS ALBUMS_pkey; Type: CONSTRAINT; Schema: SPOTIFY_DATA; Owner: postgres
--

ALTER TABLE ONLY "SPOTIFY_DATA"."ALBUMS"
    ADD CONSTRAINT "ALBUMS_pkey" PRIMARY KEY (album_id);


--
-- TOC entry 4727 (class 2606 OID 16563)
-- Name: ARTISTS ARTISTS_pkey; Type: CONSTRAINT; Schema: SPOTIFY_DATA; Owner: postgres
--

ALTER TABLE ONLY "SPOTIFY_DATA"."ARTISTS"
    ADD CONSTRAINT "ARTISTS_pkey" PRIMARY KEY (artist_id);


--
-- TOC entry 4723 (class 2606 OID 16548)
-- Name: SONGS SONGS_pkey; Type: CONSTRAINT; Schema: SPOTIFY_DATA; Owner: postgres
--

ALTER TABLE ONLY "SPOTIFY_DATA"."SONGS"
    ADD CONSTRAINT "SONGS_pkey" PRIMARY KEY (song_id);


--
-- TOC entry 4739 (class 2606 OID 16635)
-- Name: ALBUM_ARTISTS album_artist_unique; Type: CONSTRAINT; Schema: SPOTIFY_DATA; Owner: postgres
--

ALTER TABLE ONLY "SPOTIFY_DATA"."ALBUM_ARTISTS"
    ADD CONSTRAINT album_artist_unique UNIQUE (album_id, artist_id);


--
-- TOC entry 4733 (class 2606 OID 16596)
-- Name: ALBUMS album_id_unique; Type: CONSTRAINT; Schema: SPOTIFY_DATA; Owner: postgres
--

ALTER TABLE ONLY "SPOTIFY_DATA"."ALBUMS"
    ADD CONSTRAINT album_id_unique UNIQUE (album_id);


--
-- TOC entry 4729 (class 2606 OID 16594)
-- Name: ARTISTS artist_id_unique; Type: CONSTRAINT; Schema: SPOTIFY_DATA; Owner: postgres
--

ALTER TABLE ONLY "SPOTIFY_DATA"."ARTISTS"
    ADD CONSTRAINT artist_id_unique UNIQUE (artist_id);


--
-- TOC entry 4721 (class 2606 OID 16586)
-- Name: LISTENING_HISTORY played_at_unique; Type: CONSTRAINT; Schema: SPOTIFY_DATA; Owner: postgres
--

ALTER TABLE ONLY "SPOTIFY_DATA"."LISTENING_HISTORY"
    ADD CONSTRAINT played_at_unique UNIQUE (played_at);


--
-- TOC entry 4737 (class 2606 OID 16628)
-- Name: SONG_ALBUM song_album_unique; Type: CONSTRAINT; Schema: SPOTIFY_DATA; Owner: postgres
--

ALTER TABLE ONLY "SPOTIFY_DATA"."SONG_ALBUM"
    ADD CONSTRAINT song_album_unique UNIQUE (song_id, album_id);


--
-- TOC entry 4735 (class 2606 OID 16621)
-- Name: SONG_ARTISTS song_artist_unique; Type: CONSTRAINT; Schema: SPOTIFY_DATA; Owner: postgres
--

ALTER TABLE ONLY "SPOTIFY_DATA"."SONG_ARTISTS"
    ADD CONSTRAINT song_artist_unique UNIQUE (song_id, artist_id);


--
-- TOC entry 4725 (class 2606 OID 16592)
-- Name: SONGS song_id_unique; Type: CONSTRAINT; Schema: SPOTIFY_DATA; Owner: postgres
--

ALTER TABLE ONLY "SPOTIFY_DATA"."SONGS"
    ADD CONSTRAINT song_id_unique UNIQUE (song_id);


-- Completed on 2024-01-15 14:24:14

--
-- PostgreSQL database dump complete
--