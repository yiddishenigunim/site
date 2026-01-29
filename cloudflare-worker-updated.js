export default {
    async fetch(request, env, ctx) {
        const corsHeaders = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        };

        if (request.method === 'OPTIONS') {
            return new Response(null, { headers: corsHeaders });
        }

        // Allowed rating column IDs - only these can be updated via PUT
        const ALLOWED_RATING_COLUMNS = [
            'c-v5V7PENdFy', // Recording slot 1 rating
            'c-YQ-aWW59ej', // Recording slot 2 rating
            'c-gmz2rln2dD', // Recording slot 3 rating
            'c-_GlQy78PAc', // Recording slot 4 rating
            'c-yuKHpnbHfM', // Recording slot 5 rating
        ];

        const url = new URL(request.url);
        const path = url.pathname;

        // FIXED: Correct table IDs
        const CODA_DOC_ID = '6QcJRx7e13';
        const SONGS_TABLE_ID = 'grid-YycmDjJ8pS';  // FIXED: was 'grid-sync-1073-File'

        // Helper function to fetch from Coda with timeout
        async function fetchFromCoda(codaUrl, options) {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 25000); // 25 second timeout

            try {
                const response = await fetch(codaUrl, {
                    ...options,
                    signal: controller.signal,
                });
                clearTimeout(timeoutId);
                return response;
            } catch (error) {
                clearTimeout(timeoutId);
                throw error;
            }
        }

        try {
            // ============================================
            // SINGLE SONG ENDPOINT - /api/song/:id
            // ============================================
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
                    // For customId, use multiple search strategies
                    // Strategy 1: Try with hash prefix (most common format)
                    // Strategy 2: Try without hash
                    // Strategy 3: Try as plain number search
                    const searchTerms = [
                        `#${songId}`,  // With hash prefix
                        songId,         // Plain number
                        `"#${songId}"`, // Quoted with hash
                    ];
                    // Use first search term, we'll try others if this fails
                    const query = encodeURIComponent(searchTerms[0]);
                    codaUrl = `https://coda.io/apis/v1/docs/${CODA_DOC_ID}/tables/${SONGS_TABLE_ID}/rows?query=${query}&valueFormat=rich&limit=25`;
                }

                console.log(`Fetching single song: ${songId}, isRowId: ${isRowId}`);

                let song = null;

                if (isRowId) {
                    // Direct row fetch by rowId
                    const codaResponse = await fetchFromCoda(codaUrl, {
                        method: 'GET',
                        headers: {
                            'Authorization': `Bearer ${env.CODA_API_KEY}`,
                            'Content-Type': 'application/json',
                        },
                    });

                    if (codaResponse.ok) {
                        song = await codaResponse.json();
                    }
                } else {
                    // The customId column is c-DhElYWayZ- and values are stored as "```#4054```"
                    // Coda query format: columnId:"value"
                    const CUSTOM_ID_COLUMN = 'c-DhElYWayZ-';

                    // Try different value formats (with/without backticks and hash)
                    const searchValues = [
                        `\`\`\`#${songId}\`\`\``,  // With backticks: ```#4054```
                        `#${songId}`,              // Just hash: #4054
                        songId,                    // Plain number: 4054
                    ];

                    for (const searchValue of searchValues) {
                        if (song) break;

                        // Format: columnId:"value"
                        const queryStr = `${CUSTOM_ID_COLUMN}:"${searchValue}"`;
                        const searchUrl = `https://coda.io/apis/v1/docs/${CODA_DOC_ID}/tables/${SONGS_TABLE_ID}/rows?query=${encodeURIComponent(queryStr)}&valueFormat=rich&limit=5`;

                        console.log(`Trying query: ${queryStr}`);

                        const codaResponse = await fetchFromCoda(searchUrl, {
                            method: 'GET',
                            headers: {
                                'Authorization': `Bearer ${env.CODA_API_KEY}`,
                                'Content-Type': 'application/json',
                            },
                        });

                        if (!codaResponse.ok) {
                            const errorText = await codaResponse.text();
                            console.log(`Search failed for "${searchValue}": ${codaResponse.status} - ${errorText.substring(0, 100)}`);
                            continue;
                        }

                        const data = await codaResponse.json();

                        if (data.items && data.items.length > 0) {
                            // Take first match - query should be precise enough
                            song = data.items[0];
                            console.log(`Found song with query: ${queryStr}`);
                        }
                    }

                    if (!song) {
                        console.log(`Song ${songId} not found after trying all query formats`);
                        return new Response(JSON.stringify({
                            error: 'Song not found',
                            songId: songId,
                            triedFormats: searchValues,
                        }), {
                            status: 404,
                            headers: { 'Content-Type': 'application/json', ...corsHeaders },
                        });
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

                return new Response(JSON.stringify(song), {
                    status: 200,
                    headers: {
                        'Content-Type': 'application/json',
                        'Cache-Control': 'public, max-age=300', // Cache for 5 minutes
                        ...corsHeaders,
                    },
                });
            }

            // ============================================
            // EXISTING PROXY LOGIC
            // ============================================
            const proxyPath = path.replace('/api/', '');
            const codaUrl = `https://coda.io/apis/v1/${proxyPath}${url.search}`;

            // Only cache GET requests
            if (request.method === 'GET') {
                const cacheKey = new Request(codaUrl, request);
                const cache = caches.default;

                // Check cache first
                let cachedResponse = await cache.match(cacheKey);

                if (cachedResponse) {
                    console.log('Cache HIT:', proxyPath);
                    const cachedData = await cachedResponse.text();
                    return new Response(cachedData, {
                        status: 200,
                        headers: {
                            'Content-Type': 'application/json',
                            'X-Cache': 'HIT',
                            ...corsHeaders,
                        },
                    });
                }

                console.log('Cache MISS:', proxyPath);

                // Fetch from Coda with timeout
                const codaResponse = await fetchFromCoda(codaUrl, {
                    method: 'GET',
                    headers: {
                        'Authorization': `Bearer ${env.CODA_API_KEY}`,
                        'Content-Type': 'application/json',
                    },
                });

                // Log non-OK responses
                if (!codaResponse.ok) {
                    const errorBody = await codaResponse.text();
                    console.error('Coda API error:', {
                        status: codaResponse.status,
                        statusText: codaResponse.statusText,
                        path: proxyPath,
                        body: errorBody.substring(0, 500), // Limit log size
                    });

                    return new Response(JSON.stringify({
                        error: 'Coda API error',
                        status: codaResponse.status,
                        statusText: codaResponse.statusText,
                        path: proxyPath,
                        details: errorBody.substring(0, 200),
                    }), {
                        status: codaResponse.status,
                        headers: { 'Content-Type': 'application/json', ...corsHeaders },
                    });
                }

                const data = await codaResponse.json();

                const response = new Response(JSON.stringify(data), {
                    status: codaResponse.status,
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Cache': 'MISS',
                        'Cache-Control': 'public, max-age=1800',
                        ...corsHeaders,
                    },
                });

                // Store in cache (non-blocking)
                const responseToCache = new Response(JSON.stringify(data), {
                    status: 200,
                    headers: {
                        'Content-Type': 'application/json',
                        'Cache-Control': 'public, max-age=1800',
                    },
                });
                ctx.waitUntil(cache.put(cacheKey, responseToCache));

                return response;
            }

            // For PUT requests - validate that only rating columns are being updated
            if (request.method === 'PUT') {
                const bodyText = await request.text();
                let body;

                try {
                    body = JSON.parse(bodyText);
                } catch (e) {
                    return new Response(JSON.stringify({ error: 'Invalid JSON body' }), {
                        status: 400,
                        headers: { 'Content-Type': 'application/json', ...corsHeaders },
                    });
                }

                // Validate the request - only allow rating column updates
                if (body.row && body.row.cells && Array.isArray(body.row.cells)) {
                    for (const cell of body.row.cells) {
                        if (!ALLOWED_RATING_COLUMNS.includes(cell.column)) {
                            return new Response(JSON.stringify({
                                error: 'Unauthorized: Only rating updates are allowed',
                                attemptedColumn: cell.column
                            }), {
                                status: 403,
                                headers: { 'Content-Type': 'application/json', ...corsHeaders },
                            });
                        }

                        const rating = parseInt(cell.value, 10);
                        if (isNaN(rating) || rating < 1 || rating > 5) {
                            return new Response(JSON.stringify({
                                error: 'Invalid rating value. Must be 1-5.',
                                value: cell.value
                            }), {
                                status: 400,
                                headers: { 'Content-Type': 'application/json', ...corsHeaders },
                            });
                        }
                    }
                }

                // Validated - proceed with the update
                const codaResponse = await fetchFromCoda(codaUrl, {
                    method: 'PUT',
                    headers: {
                        'Authorization': `Bearer ${env.CODA_API_KEY}`,
                        'Content-Type': 'application/json',
                    },
                    body: bodyText,
                });

                if (!codaResponse.ok) {
                    const errorBody = await codaResponse.text();
                    console.error('Coda PUT error:', {
                        status: codaResponse.status,
                        path: proxyPath,
                        body: errorBody.substring(0, 500),
                    });
                }

                const data = await codaResponse.json();

                return new Response(JSON.stringify(data), {
                    status: codaResponse.status,
                    headers: { 'Content-Type': 'application/json', ...corsHeaders },
                });
            }

            // For POST/PATCH requests
            let body = null;
            if (request.method === 'POST' || request.method === 'PATCH') {
                body = await request.text();
            }

            const codaResponse = await fetchFromCoda(codaUrl, {
                method: request.method,
                headers: {
                    'Authorization': `Bearer ${env.CODA_API_KEY}`,
                    'Content-Type': 'application/json',
                },
                body: body,
            });

            if (!codaResponse.ok) {
                const errorBody = await codaResponse.text();
                console.error(`Coda ${request.method} error:`, {
                    status: codaResponse.status,
                    path: proxyPath,
                    body: errorBody.substring(0, 500),
                });

                return new Response(JSON.stringify({
                    error: 'Coda API error',
                    status: codaResponse.status,
                    statusText: codaResponse.statusText,
                    path: proxyPath,
                    details: errorBody.substring(0, 200),
                }), {
                    status: codaResponse.status,
                    headers: { 'Content-Type': 'application/json', ...corsHeaders },
                });
            }

            const data = await codaResponse.json();

            return new Response(JSON.stringify(data), {
                status: codaResponse.status,
                headers: {
                    'Content-Type': 'application/json',
                    ...corsHeaders,
                },
            });

        } catch (error) {
            // Detailed error logging
            const errorInfo = {
                message: error.message,
                name: error.name,
                path: path,
                method: request.method,
                url: request.url,
            };

            console.error('Proxy error:', errorInfo);

            // Handle specific error types
            if (error.name === 'AbortError') {
                return new Response(JSON.stringify({
                    error: 'Request timeout',
                    message: 'Coda API did not respond in time. Please try again.',
                    path: path,
                }), {
                    status: 504,
                    headers: { 'Content-Type': 'application/json', ...corsHeaders },
                });
            }

            // Generic error response with details
            return new Response(JSON.stringify({
                error: error.message,
                type: error.name,
                path: path,
                method: request.method,
            }), {
                status: 500,
                headers: { 'Content-Type': 'application/json', ...corsHeaders },
            });
        }
    }
};
