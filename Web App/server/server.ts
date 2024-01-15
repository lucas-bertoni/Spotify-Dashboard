interface Song {
    played_at: string | undefined,
    song: string,
    artists: string[],
    album: string[],
    song_duration_ms: number,
    duration_played_ms: number | undefined,
    amount_played: number | undefined
}

interface TopSong {
    song: string,
    artists: string[],
    album: string[],
    num_plays: number
}

interface TopArtist {
    artist: string,
    num_plays: number
}

interface TopAlbum {
    album: string,
    artists: string[]
}

import cors from 'cors';
import express, { Express, Request, Response } from 'express';
// @ts-ignore
import * as helper from './helper.js';

const app: Express = express();
const PORT: any = process.env.PORT || 3000;

app.use(express.json());
app.use(cors());

// Get the 50 most recently played songs
app.get('/api/v1/recently_played', async (req: Request, res: Response) => {
    const { numRecords, cursor }: { numRecords: number, cursor: number } = req.body;

    try {
        const songs: Song[] | undefined = await helper.getRecentlyPlayed(numRecords, cursor);
        res.status(200).send({ songs: songs });
    } catch (error) {
        res.status(500).send('Error getting recently played songs');
    }
});

// Get the top 10 most played songs for a given time period
app.get('/api/v1/top_songs', async (req: Request, res: Response) => {
    const { numRecords, cursor }: { numRecords: number, cursor: number } = req.body;

    try {
        const songs: TopSong[] | undefined = await helper.getTopSongs(numRecords, cursor);
        res.status(200).send({ songs: songs });
    } catch (error) {
        res.status(500).send('Error getting top songs');
    }
});

// Get the top 10 most played artists for a given time period
app.get('/api/v1/top_artists', async (req: Request, res: Response) => {
    const { numRecords, cursor }: { numRecords: number, cursor: number } = req.body;

    try {
        const artists: TopArtist[] | undefined = await helper.getTopArtists(numRecords, cursor);
        res.status(200).send({ artists: artists });
    } catch (error) {
        res.status(500).send('Error getting top artists');
    }
});

// Get the top 10 most played artists for a given time period
app.get('/api/v1/top_albums', async (req: Request, res: Response) => {
    const { numRecords, cursor }: { numRecords: number, cursor: number } = req.body;

    try {
        const albums: TopAlbum[] | undefined = await helper.getTopAlbums(numRecords, cursor);
        res.status(200).send({ albums: albums });
    } catch (error) {
        res.status(500).send('Error getting top albums');
    }
});

app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`)
});