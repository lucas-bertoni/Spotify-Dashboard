interface Song {
    played_at: string,
    song: string,
    artists: string[],
    album: string[],
    song_duration_ms: number,
    duration_played_ms: number,
    amount_played: number
}

import { DatabaseConnection } from './DatabaseConnection';
import { QueryResult } from 'pg';

const dbConn = new DatabaseConnection();
dbConn.connect();

// Increase cursor by 1 if you want 50 more records
const getRecentlyPlayed = async (numRecords: number = 50, cursor: number = 0) => {
    try {
        const query = `
            WITH HISTORY_WITH_ROW_NUMS AS (
                SELECT *, ROW_NUMBER() OVER(ORDER BY played_at DESC) AS row_num
                FROM HISTORY
            )
            SELECT *
            FROM HISTORY_WITH_ROW_NUMS
            WHERE row_num BETWEEN ${cursor * numRecords} AND ${(cursor + 1) * numRecords}
            ORDER BY played_at DESC;
        `;

        const result: QueryResult = await dbConn.pool.query(query);

        return result.rows;
    } catch (error) {
        console.log('\nThere was an error getting the recently played songs');
        console.log(error)
    }
};

// Get the top songs between the dates given by before and after
const getTopSongs = async (numRecords: number = 10, cursor: number = 0, before: string = '', after: string = '') => {
    try {
        const query = `
            WITH TOP_SONGS AS (
                SELECT song_id, song_name, artist_names, COUNT(*) AS num_plays
                FROM HISTORY
                WHERE amount_played >= 0.75 ${before === '' || after === '' ? '' : ' AND played_at BETWEEN ' + before + ' AND ' + after}
                GROUP BY song_id, song_name, artist_names
                ORDER BY num_plays DESC
            ),
            TOP_SONGS_WITH_ROW_NUMS AS (
                SELECT *, ROW_NUMBER() OVER(ORDER BY num_plays DESC) AS row_num
                FROM TOP_SONGS
            )
            SELECT row_num, song_id, song_name, artist_names, num_plays
            FROM TOP_SONGS_WITH_ROW_NUMS
            WHERE row_num BETWEEN ${cursor * numRecords} AND ${(cursor + 1) * numRecords}
            ORDER BY num_plays DESC;
        `;

        const result: QueryResult = await dbConn.pool.query(query);

        return result.rows;
    } catch (error) {
        console.log('\nThere was an error getting the top songs');
        console.log(error)
    }
};

// Get the top artists between the dates given by before and after
const getTopArtists = async (numRecords: number = 10, cursor: number = 0, before: string = '', after: string = '') => {
    try {
        const query = `
            WITH UNNEST_ARTISTS AS (
                SELECT played_at, UNNEST(artist_ids) AS artist_id, amount_played
                FROM HISTORY
                WHERE amount_played >= 0.75 ${before === '' || after === '' ? '' : ' AND played_at BETWEEN ' + before + ' AND ' + after}
            ),
            TOP_ARTISTS AS (
                SELECT artist_id, COUNT(*) AS num_plays
                FROM UNNEST_ARTISTS
                GROUP BY artist_id
                ORDER BY num_plays DESC
            ),
            TOP_ARTISTS_WITH_ROW_NUMS AS (
                SELECT *, ROW_NUMBER() OVER(ORDER BY num_plays DESC) AS row_num
                FROM TOP_ARTISTS
            )
            SELECT row_num, artist_id, artist_name, num_plays
            FROM TOP_ARTISTS_WITH_ROW_NUMS TA
            JOIN "SPOTIFY_DATA"."ARTISTS" A ON TA.artist_id = A.artist_id 
            WHERE row_num BETWEEN ${cursor * numRecords} AND ${(cursor + 1) * numRecords}
            ORDER BY num_plays DESC;
        `;

        const result: QueryResult = await dbConn.pool.query(query);

        return result.rows;
    } catch (error) {
        console.log('\nThere was an error getting the top artists');
        console.log(error)
    }
};

// Get the top songs between the dates given by before and after
const getTopAlbums = async (numRecords: number = 10, cursor: number = 0, before: string = '', after: string = '') => {
    try {
        const query = `
            WITH TOP_ALBUMS AS (
                SELECT album_id, album_name, COUNT(*) AS num_plays
                FROM HISTORY
                WHERE amount_played >= 0.75 ${before === '' || after === '' ? '' : ' AND played_at BETWEEN ' + before + ' AND ' + after}
                GROUP BY album_id, album_name
                ORDER BY num_plays DESC
            ),
            TOP_ALBUMS_WITH_ROW_NUMS AS (
                SELECT *, ROW_NUMBER() OVER(ORDER BY num_plays DESC) AS row_num
                FROM TOP_ALBUMS
            )
            SELECT row_num, AL.album_id, album_name, ARRAY_AGG(artist_name) AS artist_names, num_plays
            FROM TOP_ALBUMS_WITH_ROW_NUMS AL
            JOIN "SPOTIFY_DATA"."ALBUM_ARTISTS" ALAR ON AL.album_id = ALAR.album_id
            JOIN "SPOTIFY_DATA"."ARTISTS" AR ON ALAR.artist_id = AR.artist_id
            WHERE row_num BETWEEN ${cursor * numRecords} AND ${(cursor + 1) * numRecords}
            GROUP BY AL.album_id, album_name, num_plays
            ORDER BY num_plays DESC;
        `;

        const result: QueryResult = await dbConn.pool.query(query);

        return result.rows;
    } catch (error) {
        console.log('\nThere was an error getting the top albums');
        console.log(error)
    }
};

export { getRecentlyPlayed, getTopSongs, getTopArtists, getTopAlbums }