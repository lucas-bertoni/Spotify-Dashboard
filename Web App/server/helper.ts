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
import moment from 'moment-timezone';

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

// Get the top songs between the dates given by before and before
const getTopSongs = async (numRecords: number = 10, cursor: number = 0, after: string = '', before: string = '') => {
    try {
        const dateConstraintString = getDateConstraintString(after, before);

        const query = `
            WITH TOP_SONGS AS (
                SELECT song_id, song_name, artist_names, COUNT(*) AS num_plays
                FROM HISTORY
                WHERE amount_played >= 0.75 ${dateConstraintString}
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

        console.log(query)

        const result: QueryResult = await dbConn.pool.query(query);

        return result.rows;
    } catch (error) {
        console.log('\nThere was an error getting the top songs');
        console.log(error)
    }
};

// Get the top artists between the dates given by before and before
const getTopArtists = async (numRecords: number = 10, cursor: number = 0, after: string = '', before: string = '') => {
    try {
        const dateConstraintString = getDateConstraintString(after, before);

        const query = `
            WITH UNNEST_ARTISTS AS (
                SELECT played_at, UNNEST(artist_ids) AS artist_id, amount_played
                FROM HISTORY
                WHERE amount_played >= 0.75 ${dateConstraintString}
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
            SELECT row_num, TA.artist_id, artist_name, num_plays
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

// Get the top songs between the dates given by before and before
const getTopAlbums = async (numRecords: number = 10, cursor: number = 0, after: string = '', before: string = '') => {
    try {
        const dateConstraintString = getDateConstraintString(after, before);

        const query = `
            WITH TOP_ALBUMS AS (
                SELECT album_id, album_name, COUNT(*) AS num_plays
                FROM HISTORY
                WHERE amount_played >= 0.75 ${dateConstraintString}
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

function getDateConstraintString(after: string, before: string): string {
    const tzFormatter = Intl.DateTimeFormat('en-US', {
        timeZone: "America/New_York"
     })

    let dateConstraintString = '';

    let afterDate = moment.tz(after, 'America/New_York');
    let beforeDate = moment.tz(before, 'America/New_York');

    if (after !== '' && before === '') {            // Only after is defined

        dateConstraintString = `AND DATE(played_at) > '${afterDate}'`;

    } else if (after === '' && before !== '') {     // Only before is defined

        dateConstraintString = `AND DATE(played_at) < '${beforeDate}'`;

    } else if (after === before && after !== '') {  // Both are defined and equal

        dateConstraintString = `AND DATE(played_at) = '${afterDate}'`;

    } else if (after !== '' && before !== '') {     // Both are defined and not equal

        dateConstraintString = `AND DATE(played_at) >= '${afterDate.format('YYYY-MM-DD')}' AND DATE(played_at) < '${beforeDate.format('YYYY-MM-DD')}'`;

    }

    return dateConstraintString;
}

export { getRecentlyPlayed, getTopSongs, getTopArtists, getTopAlbums }