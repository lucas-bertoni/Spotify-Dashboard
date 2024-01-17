async function getRecentlyPlayed() {    
    const url = 'http://localhost:3000/api/v1/recently_played';
    await axios
        .get(url)
        .then((response) => {
            const table = document.getElementById('recently-played-table-body');
            const songs = response.data['songs'];
            songs.forEach((song) => {
                const songRow = document.createElement('tr');
                const data = [moment(Date.parse(song['played_at'])).format('MMM-DD-YYYY : hh:mm A'), song['song_name'], song['artist_names'], song['album_name']];
                data.forEach((column) => {
                    const songRowData = document.createElement('td');
                    const textNode = document.createTextNode(column);
                    songRowData.appendChild(textNode);
                    songRow.appendChild(songRowData);
                });
                table.appendChild(songRow);
            });
        })
        .catch((error) => {
            console.log('There was an error getting the recently played songs');
            console.log(error);
            console.log('----------------------------------------------------');
        });
}