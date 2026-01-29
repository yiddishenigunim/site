// ============================================
// FIXED SINGLE SONG ENDPOINT - /api/song/:id
// ============================================
// Replace the existing single song endpoint section in your Cloudflare Worker with this code

// IMPORTANT: Use the correct table ID!
const CODA_DOC_ID = '6QcJRx7e13';
const SONGS_TABLE_ID = 'grid-YycmDjJ8pS';  // FIXED: was 'grid-sync-1073-File'

// Inside your fetch handler, replace the single song section with:

const singleSongMatch = path.match(/^\/api\/song\/(.+)$/);
if (singleSongMatch && request.method === 'GET') {
    const songId = singleSongMatch[1];

    // Determine if it's a rowId (i-xxxxx) or customId (number)
    const isRowId = songId.startsWith('i-');

    let codaUrl;
    if (isRowId) {
        // Fetch directly with rowId
        codaUrl = `https://coda.io/apis/v1/docs/${CODA_DOC_ID}/tables/${SONGS_TABLE_ID}/rows/${songId}?valueFormat=rich`;
    } else {
        // For customId, we need to search
        // Coda's query parameter does full-text search
        // The customId in the table is stored as "#123" format, so search for that
        const searchTerm = songId.startsWith('#') ? songId : `#${songId}`;
        const query = encodeURIComponent(searchTerm);
        codaUrl = `https://coda.io/apis/v1/docs/${CODA_DOC_ID}/tables/${SONGS_TABLE_ID}/rows?query=${query}&valueFormat=rich&limit=10`;
    }

    console.log(`Fetching single song: ${songId}, isRowId: ${isRowId}, url: ${codaUrl}`);

    const codaResponse = await fetchFromCoda(codaUrl, {
        method: 'GET',
        headers: {
            'Authorization': `Bearer ${env.CODA_API_KEY}`,
            'Content-Type': 'application/json',
        },
    });

    if (!codaResponse.ok) {
        const errorBody = await codaResponse.text();
        console.error('Single song fetch error:', {
            status: codaResponse.status,
            songId: songId,
            error: errorBody.substring(0, 200),
        });

        return new Response(JSON.stringify({
            error: 'Song not found',
            songId: songId,
            status: codaResponse.status,
        }), {
            status: codaResponse.status === 404 ? 404 : 500,
            headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
    }

    const data = await codaResponse.json();

    // Process the result
    let song = null;

    if (isRowId) {
        // Direct row fetch returns the row object
        song = data;
    } else {
        // Query returns items array - find the exact match
        if (data.items && data.items.length > 0) {
            // Find the song with matching customId
            // The customId column is 'c-DhElYWayZ-' and contains values like "#152"
            const targetId = songId.startsWith('#') ? songId : `#${songId}`;

            song = data.items.find(item => {
                const values = item.values || {};
                // Check the customId column
                const customIdValue = values['c-DhElYWayZ-'];
                if (customIdValue) {
                    const extracted = typeof customIdValue === 'string'
                        ? customIdValue
                        : (customIdValue.value || customIdValue.name || customIdValue.display || '');
                    return extracted === targetId || extracted === songId || extracted === `#${songId}`;
                }
                // Also check if any value matches (fallback)
                for (const val of Object.values(values)) {
                    const text = typeof val === 'string' ? val : (val?.value || val?.name || val?.display || '');
                    if (text === targetId || text === `#${songId}`) {
                        return true;
                    }
                }
                return false;
            });

            // If no exact match, take the first result (Coda's relevance ranking)
            if (!song && data.items.length > 0) {
                song = data.items[0];
                console.log(`No exact customId match for ${songId}, using first search result`);
            }
        }

        if (!song) {
            return new Response(JSON.stringify({
                error: 'Song not found',
                songId: songId,
            }), {
                status: 404,
                headers: { 'Content-Type': 'application/json', ...corsHeaders },
            });
        }
    }

    return new Response(JSON.stringify(song), {
        status: 200,
        headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'public, max-age=300', // Cache for 5 minutes
            ...corsHeaders,
        },
    });
}
