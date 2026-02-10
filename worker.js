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
            'c-WqeOGhwKvu', // Recordings table rating column (רעיטינג)
        ];

        const url = new URL(request.url);
        const path = url.pathname;

        // Table IDs
        const CODA_DOC_ID = '6QcJRx7e13';
        const SONGS_TABLE_ID = 'grid-YycmDjJ8pS';
        const RECORDINGS_TABLE_ID = 'grid-jWWLn52ryG';
        const MECHABRIM_TABLE_ID = 'grid-bCbfhDmi82';
        const CHATZEROS_TABLE_ID = 'grid-4-KYWDJ5l9';
        const VERTER_TABLE_ID = 'grid-tLcBzyh_tz';
        const PIYUTIM_TABLE_ID = 'grid-318i50N4cK';
        const ZMANIM_TABLE_ID = 'grid-K2rOGXJwjk';
        const COLLECTIONS_TABLE_ID = 'grid-K7Mo-tilkg';
        const RESOURCES_TABLE_ID = 'grid-8Gfz8rXNUV';
        const DOCUMENTS_TABLE_ID = 'grid-3wKzDnkPg-';
        const ALBUMS_TABLE_ID = 'grid-bbTaa18Jhx';

        // Column IDs for songs table - MUST match frontend mappings exactly
        const SONG_COLUMNS = {
            name: 'c-6qqDF7NYmv',
            customId: 'c-DhElYWayZ-',
            mechaber: 'c-ujrTHeCJGo',
            chatzer: 'c-u0H9G6FHoJ',
            scale: 'c-wQZrHPVWog',
            ritem: 'c-OmWEZXO7Ys',
            verter: 'c-KAWfVfB4jq',
            collections: 'c-pCBFiGU8ex',    // FIXED (was c-kiHIam57Z0)
            pasigOif: 'c-mTtYp3FK9U',       // FIXED (was c-nF5sRFoMkJ) - פאסיג אויף / זמנים
            gezungen: 'c-kiHIam57Z0',        // FIXED (was c-0jFLHVYRL8) - געזונגען אויף
            maure: 'c-J3q8mQIXBh',          // FIXED (was c-6E02UJxxZW) - מאורע
            personalities: 'c-6E02UJxxZW',   // FIXED (was c-KBb2Bgi6nM) - פערזענליכקייט
            info: 'c-0jFLHVYRL8',           // ADDED - אינפארמאציע
            documents: 'c-nF5sRFoMkJ',      // ADDED - דאקומענטן
            albums: 'c-j8dujLHcnT',         // ADDED - אלבומס
            resources: 'c-KBb2Bgi6nM',      // ADDED - רעסורסן
            zugeleigt: 'c-iOlvD8H0n3',      // ADDED - צוגעלייגט אום
            siman: 'c-jSVbHezFAS',          // ADDED - סימן
            notn: 'c-hodtA6zYsy',           // ADDED - נאָטן
            recordingRefs: 'c-T2POJqNgIS',  // Relation to recordings table
        };

        // Column IDs for recordings table
        const RECORDING_COLUMNS = {
            nigunId: 'c-YRfBI9lv8C',
            file: 'c-5huefE57QG',
            personalities: 'c-g8h-pbWteL',
            details: 'c--CHvJUWn_f',
            rating: 'c-WqeOGhwKvu',
            album: 'c-jbH2d1qm6j',
        };

        // Helper function to fetch from Coda with timeout
        async function fetchFromCoda(codaUrl, options) {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 25000);

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

        // Helper to extract text from Coda values
        function extractText(value) {
            if (!value && value !== 0) return '';
            if (typeof value === 'number') return String(value);
            if (typeof value === 'string') return value.replace(/```/g, '').trim();
            if (Array.isArray(value)) {
                return value.map(v => extractText(v)).filter(Boolean).join(', ');
            }
            if (typeof value === 'object') {
                // Handle row references - extract name and rowId
                if (value.rowId || value.tableId) {
                    return value.name || '';
                }
                const text = value.name || value.display || value.value || '';
                return typeof text === 'string' ? text.replace(/```/g, '').trim() : String(text);
            }
            return '';
        }

        // Helper to extract ID from Coda row reference
        function extractRowId(value) {
            if (!value) return null;
            if (typeof value === 'object' && value.rowId) {
                return value.rowId;
            }
            if (Array.isArray(value) && value.length > 0 && value[0].rowId) {
                return value[0].rowId;
            }
            return null;
        }

        // Helper to extract customId from row reference name (e.g., "```#123```" -> "123")
        function extractCustomId(value) {
            if (!value) return null;
            let name = '';
            if (typeof value === 'object' && value.name) {
                name = value.name;
            } else if (Array.isArray(value) && value.length > 0 && value[0].name) {
                name = value[0].name;
            } else if (typeof value === 'string') {
                name = value;
            }
            // Remove backticks and # prefix
            return name.replace(/```/g, '').replace(/^#/, '').trim() || null;
        }

        // Helper to extract all names from a relation field (can be multiple)
        function extractAllNames(value) {
            if (!value) return [];
            if (Array.isArray(value)) {
                return value.map(v => extractText(v)).filter(Boolean);
            }
            const text = extractText(value);
            return text ? [text] : [];
        }

        // Fetch all rows from a table with pagination
        async function fetchAllRows(tableId) {
            let allRows = [];
            let pageToken = null;

            do {
                const fetchUrl = `https://coda.io/apis/v1/docs/${CODA_DOC_ID}/tables/${tableId}/rows?valueFormat=rich&limit=500${pageToken ? '&pageToken=' + pageToken : ''}`;
                const response = await fetchFromCoda(fetchUrl, {
                    method: 'GET',
                    headers: {
                        'Authorization': `Bearer ${env.CODA_API_KEY}`,
                        'Content-Type': 'application/json',
                    },
                });

                if (!response.ok) {
                    throw new Error(`Coda API error: ${response.status}`);
                }

                const data = await response.json();
                allRows = allRows.concat(data.items || []);
                pageToken = data.nextPageToken;
            } while (pageToken);

            return allRows;
        }

        try {
            // ============================================
            // FILE UPLOAD ENDPOINT - /api/upload
            // ============================================
            if (path === '/api/upload' && request.method === 'POST') {
                try {
                    const formData = await request.formData();
                    const file = formData.get('file');

                    if (!file) {
                        return new Response(JSON.stringify({
                            success: false,
                            error: 'קיין פייל ניט געפונען'
                        }), {
                            status: 400,
                            headers: { 'Content-Type': 'application/json', ...corsHeaders }
                        });
                    }

                    // לייענען די פייל ביטס
                    const fileData = await file.arrayBuffer();

                    // שפייכערן אין R2
                    const fileName = `${Date.now()}_${file.name}`;
                    await env.BUS.put(fileName, fileData, {
                        httpMetadata: {
                            contentType: file.type,
                        },
                    });

                    return new Response(JSON.stringify({
                        success: true,
                        fileName: file.name,
                        size: file.size,
                        storedAs: fileName,
                        type: file.type
                    }), {
                        status: 200,
                        headers: { 'Content-Type': 'application/json', ...corsHeaders }
                    });

                } catch (error) {
                    console.error('Upload error:', error);
                    return new Response(JSON.stringify({
                        success: false,
                        error: error.message
                    }), {
                        status: 500,
                        headers: { 'Content-Type': 'application/json', ...corsHeaders }
                    });
                }
            }

            // ============================================
            // CACHE METADATA ENDPOINTS
            // ============================================

            // GET /api/last-updated - Returns cache timestamp
            if (path === '/api/last-updated' && request.method === 'GET') {
                try {
                    const lastUpdated = await env.CACHE_KV.get('lastUpdated');
                    return new Response(JSON.stringify({
                        lastUpdated: lastUpdated || new Date().toISOString()
                    }), {
                        status: 200,
                        headers: {
                            'Content-Type': 'application/json',
                            'Cache-Control': 'no-cache',
                            ...corsHeaders
                        },
                    });
                } catch (error) {
                    return new Response(JSON.stringify({
                        error: 'Failed to get lastUpdated',
                        lastUpdated: new Date().toISOString()
                    }), {
                        status: 200,
                        headers: { 'Content-Type': 'application/json', ...corsHeaders },
                    });
                }
            }

            // POST /api/invalidate-cache - Called by Coda webhook
            if (path === '/api/invalidate-cache' && request.method === 'POST') {
                try {
                    const now = new Date().toISOString();
                    await env.CACHE_KV.put('lastUpdated', now);

                    // Purge edge cache for index endpoints
                    const cache = caches.default;
                    const urlBase = url.origin;
                    const cachePurgeUrls = [
                        `${urlBase}/api/song-index`,
                        `${urlBase}/api/recordings-index`,
                        `${urlBase}/api/category-index/mechabrim`,
                        `${urlBase}/api/category-index/chatzeros`,
                        `${urlBase}/api/category-index/albums`,
                        `${urlBase}/api/category-index/piyutim`,
                        `${urlBase}/api/category-index/zmanim`,
                        `${urlBase}/api/category-index/collections`,
                        `${urlBase}/api/category-index/verter`,
                        `${urlBase}/api/category-index/resources`,
                        `${urlBase}/api/category-index/documents`,
                    ];

                    for (const purgeUrl of cachePurgeUrls) {
                        try {
                            await cache.delete(new Request(purgeUrl));
                        } catch (e) {
                            console.log('Cache delete failed for:', purgeUrl);
                        }
                    }

                    return new Response(JSON.stringify({
                        success: true,
                        lastUpdated: now,
                        purgedUrls: cachePurgeUrls.length
                    }), {
                        status: 200,
                        headers: { 'Content-Type': 'application/json', ...corsHeaders },
                    });
                } catch (error) {
                    return new Response(JSON.stringify({
                        error: 'Failed to invalidate cache',
                        message: error.message
                    }), {
                        status: 500,
                        headers: { 'Content-Type': 'application/json', ...corsHeaders },
                    });
                }
            }

            // ============================================
            // SONG INDEX ENDPOINT - /api/song-index
            // ============================================
            if (path === '/api/song-index' && request.method === 'GET') {
                // Check edge cache first
                const cache = caches.default;
                const cacheKey = new Request(request.url);
                const cachedResponse = await cache.match(cacheKey);

                if (cachedResponse) {
                    const newResponse = new Response(cachedResponse.body, cachedResponse);
                    newResponse.headers.set('X-Cache', 'HIT');
                    newResponse.headers.set('Access-Control-Allow-Origin', '*');
                    return newResponse;
                }

                try {
                    console.log('Building song index...');

                    // Fetch songs and recordings in parallel
                    const [songsRows, recordingsRows] = await Promise.all([
                        fetchAllRows(SONGS_TABLE_ID),
                        fetchAllRows(RECORDINGS_TABLE_ID),
                    ]);

                    console.log(`Fetched ${songsRows.length} songs and ${recordingsRows.length} recordings`);

                    // Build recordings index by nigun ID
                    const recordingsByNigunId = {};
                    recordingsRows.forEach(item => {
                        const values = item.values;

                        // Get nigun ID reference
                        const nigunIdValue = values[RECORDING_COLUMNS.nigunId];
                        let nigunId = extractCustomId(nigunIdValue);
                        if (!nigunId) return;

                        // Get file attachment
                        const fileAttachment = values[RECORDING_COLUMNS.file];
                        if (!fileAttachment) return;

                        // Handle both array and single object
                        const files = Array.isArray(fileAttachment) ? fileAttachment : [fileAttachment];
                        if (files.length === 0 || !files[0].url) return;

                        // Parse rating
                        const ratingVal = extractText(values[RECORDING_COLUMNS.rating]);
                        let rating = 0;
                        if (ratingVal) {
                            const parsed = parseInt(ratingVal, 10);
                            if (!isNaN(parsed) && parsed >= 1 && parsed <= 5) {
                                rating = parsed;
                            }
                        }

                        // Store recording info
                        if (!recordingsByNigunId[nigunId]) {
                            recordingsByNigunId[nigunId] = [];
                        }

                        recordingsByNigunId[nigunId].push({
                            rowId: item.id,
                            url: files[0].url,
                            rating: rating,
                        });
                    });

                    // Sort recordings by rating and find best for each nigun
                    const bestRecordings = {};
                    Object.keys(recordingsByNigunId).forEach(nigunId => {
                        const recs = recordingsByNigunId[nigunId];
                        recs.sort((a, b) => (b.rating || 0) - (a.rating || 0));
                        bestRecordings[nigunId] = {
                            rowId: recs[0].rowId,
                            rating: recs[0].rating,
                            count: recs.length,
                        };
                    });

                    // Build song index
                    const songIndex = songsRows.map(item => {
                        const values = item.values;

                        // Extract customId
                        let customId = extractText(values[SONG_COLUMNS.customId]);
                        if (customId) {
                            customId = customId.replace(/^#/, '').trim();
                        }

                        // Extract name
                        const name = extractText(values[SONG_COLUMNS.name]);
                        if (!name) return null;

                        // Extract mechaber
                        const mechaberValue = values[SONG_COLUMNS.mechaber];
                        const mechaber = extractText(mechaberValue);
                        const mechaberRowId = extractRowId(mechaberValue);
                        const mechaberId = extractCustomId(mechaberValue) || mechaber;

                        // Extract chatzer (can be multiple)
                        const chatzerValue = values[SONG_COLUMNS.chatzer];
                        const chatzer = extractText(chatzerValue);
                        const firstChatzer = chatzer ? chatzer.split(',')[0].trim() : '';
                        let chatzerRowId = null;
                        let chatzerId = null;
                        if (Array.isArray(chatzerValue) && chatzerValue.length > 0) {
                            chatzerRowId = chatzerValue[0].rowId || null;
                            chatzerId = extractCustomId(chatzerValue[0]) || firstChatzer;
                        } else if (chatzerValue && typeof chatzerValue === 'object') {
                            chatzerRowId = chatzerValue.rowId || null;
                            chatzerId = extractCustomId(chatzerValue) || firstChatzer;
                        }

                        // Extract filter fields
                        const scale = extractText(values[SONG_COLUMNS.scale]);
                        const ritem = extractText(values[SONG_COLUMNS.ritem]);

                        // FIXED: Use correct column IDs
                        const collections = extractText(values[SONG_COLUMNS.collections]);  // c-pCBFiGU8ex
                        const pasigOif = extractText(values[SONG_COLUMNS.pasigOif]);        // c-mTtYp3FK9U
                        const gezungen = extractText(values[SONG_COLUMNS.gezungen]);        // c-kiHIam57Z0

                        // Verter - truncate to first 80 chars for search/display
                        let verter = extractText(values[SONG_COLUMNS.verter]);
                        if (verter && verter.length > 80) {
                            verter = verter.substring(0, 80) + '...';
                        }

                        // Get recording info
                        const recInfo = bestRecordings[customId] || null;

                        // SLIM INDEX: Essential fields for list/search/filter
                        // Full details (maure, info, documents, etc.) loaded via /api/song/:id
                        return {
                            id: customId || item.id,
                            rowId: item.id,
                            name: name,
                            mechaber: mechaber ? mechaber.split(',')[0].trim() : '',
                            mechaberId: mechaberId || null,
                            mechaberRowId: mechaberRowId || null,
                            chatzer: firstChatzer,
                            chatzerId: chatzerId || null,
                            chatzerRowId: chatzerRowId || null,
                            scale: scale || null,
                            ritem: ritem || null,
                            verter: verter || null,
                            collections: collections || null,    // FIXED: now from correct column
                            pasigOif: pasigOif || null,          // FIXED: now string from correct column
                            gezungen: gezungen || null,          // ADDED: for gezungen filter
                            hasRecordings: !!recInfo,
                            recordingCount: recInfo ? recInfo.count : 0,
                            bestRecordingRowId: recInfo ? recInfo.rowId : null,
                            bestRecordingRating: recInfo ? recInfo.rating : 0,
                        };
                    }).filter(Boolean);

                    // Get lastUpdated
                    let lastUpdated;
                    try {
                        lastUpdated = await env.CACHE_KV.get('lastUpdated');
                    } catch (e) {
                        lastUpdated = null;
                    }

                    const responseData = {
                        lastUpdated: lastUpdated || new Date().toISOString(),
                        count: songIndex.length,
                        songs: songIndex,
                    };

                    const response = new Response(JSON.stringify(responseData), {
                        status: 200,
                        headers: {
                            'Content-Type': 'application/json',
                            'Cache-Control': 'public, max-age=300',
                            'X-Cache': 'MISS',
                            ...corsHeaders,
                        },
                    });

                    // Cache on edge
                    ctx.waitUntil(cache.put(cacheKey, response.clone()));

                    return response;

                } catch (error) {
                    console.error('Error building song index:', error);
                    return new Response(JSON.stringify({
                        error: 'Failed to build song index',
                        message: error.message
                    }), {
                        status: 500,
                        headers: { 'Content-Type': 'application/json', ...corsHeaders },
                    });
                }
            }

            // ============================================
            // RECORDINGS INDEX ENDPOINT - /api/recordings-index
            // ============================================
            if (path === '/api/recordings-index' && request.method === 'GET') {
                // Check edge cache first
                const cache = caches.default;
                const cacheKey = new Request(request.url);
                const cachedResponse = await cache.match(cacheKey);

                if (cachedResponse) {
                    const newResponse = new Response(cachedResponse.body, cachedResponse);
                    newResponse.headers.set('X-Cache', 'HIT');
                    newResponse.headers.set('Access-Control-Allow-Origin', '*');
                    return newResponse;
                }

                try {
                    console.log('Building recordings index...');
                    const recordingsRows = await fetchAllRows(RECORDINGS_TABLE_ID);

                    // Build recordings index by nigun ID
                    const recordingsByNigunId = {};

                    recordingsRows.forEach(item => {
                        const values = item.values;

                        // Get nigun ID reference
                        const nigunIdValue = values[RECORDING_COLUMNS.nigunId];
                        let nigunId = extractCustomId(nigunIdValue);
                        if (!nigunId) return;

                        // Get file attachment
                        const fileAttachment = values[RECORDING_COLUMNS.file];
                        if (!fileAttachment) return;

                        const files = Array.isArray(fileAttachment) ? fileAttachment : [fileAttachment];
                        if (files.length === 0 || !files[0].url) return;

                        // Extract metadata
                        const personalities = extractText(values[RECORDING_COLUMNS.personalities]);
                        const details = extractText(values[RECORDING_COLUMNS.details]);
                        const album = extractText(values[RECORDING_COLUMNS.album]);
                        const ratingVal = extractText(values[RECORDING_COLUMNS.rating]);

                        let rating = 0;
                        if (ratingVal) {
                            const parsed = parseInt(ratingVal, 10);
                            if (!isNaN(parsed) && parsed >= 1 && parsed <= 5) {
                                rating = parsed;
                            }
                        }

                        // Process all files
                        files.forEach((file, fileIdx) => {
                            if (!file.url) return;

                            if (!recordingsByNigunId[nigunId]) {
                                recordingsByNigunId[nigunId] = [];
                            }

                            recordingsByNigunId[nigunId].push({
                                rowId: item.id,
                                url: file.url,
                                name: file.name || 'רעקארדינג',
                                details: details,
                                personalities: personalities,
                                album: album,
                                rating: rating,
                            });
                        });
                    });

                    // Sort all recordings by rating
                    Object.values(recordingsByNigunId).forEach(recs => {
                        recs.sort((a, b) => (b.rating || 0) - (a.rating || 0));
                        recs.forEach((rec, idx) => {
                            rec.recordingNumber = idx + 1;
                        });
                    });

                    let lastUpdated;
                    try {
                        lastUpdated = await env.CACHE_KV.get('lastUpdated');
                    } catch (e) {
                        lastUpdated = null;
                    }

                    const responseData = {
                        lastUpdated: lastUpdated || new Date().toISOString(),
                        nigunimCount: Object.keys(recordingsByNigunId).length,
                        recordings: recordingsByNigunId,
                    };

                    const response = new Response(JSON.stringify(responseData), {
                        status: 200,
                        headers: {
                            'Content-Type': 'application/json',
                            'Cache-Control': 'public, max-age=300',
                            'X-Cache': 'MISS',
                            ...corsHeaders,
                        },
                    });

                    ctx.waitUntil(cache.put(cacheKey, response.clone()));
                    return response;

                } catch (error) {
                    console.error('Error building recordings index:', error);
                    return new Response(JSON.stringify({
                        error: 'Failed to build recordings index',
                        message: error.message
                    }), {
                        status: 500,
                        headers: { 'Content-Type': 'application/json', ...corsHeaders },
                    });
                }
            }

            // ============================================
            // SINGLE RECORDING ENDPOINT - /api/recording/:rowId
            // ============================================
            const recordingMatch = path.match(/^\/api\/recording\/(.+)$/);
            if (recordingMatch && request.method === 'GET') {
                const rowId = recordingMatch[1];

                try {
                    const codaUrl = `https://coda.io/apis/v1/docs/${CODA_DOC_ID}/tables/${RECORDINGS_TABLE_ID}/rows/${rowId}?valueFormat=rich`;

                    const codaResponse = await fetchFromCoda(codaUrl, {
                        method: 'GET',
                        headers: {
                            'Authorization': `Bearer ${env.CODA_API_KEY}`,
                            'Content-Type': 'application/json',
                        },
                    });

                    if (!codaResponse.ok) {
                        return new Response(JSON.stringify({
                            error: 'Recording not found',
                            rowId: rowId,
                        }), {
                            status: 404,
                            headers: { 'Content-Type': 'application/json', ...corsHeaders },
                        });
                    }

                    const data = await codaResponse.json();
                    const values = data.values || {};

                    // Extract file URL
                    const fileAttachment = values[RECORDING_COLUMNS.file];
                    let url = null;
                    let name = 'רעקארדינג';

                    if (fileAttachment) {
                        const files = Array.isArray(fileAttachment) ? fileAttachment : [fileAttachment];
                        if (files.length > 0 && files[0].url) {
                            url = files[0].url;
                            name = files[0].name || name;
                        }
                    }

                    const recording = {
                        rowId: data.id,
                        url: url,
                        name: name,
                        details: extractText(values[RECORDING_COLUMNS.details]),
                        personalities: extractText(values[RECORDING_COLUMNS.personalities]),
                        album: extractText(values[RECORDING_COLUMNS.album]),
                        rating: parseInt(extractText(values[RECORDING_COLUMNS.rating])) || 0,
                    };

                    return new Response(JSON.stringify(recording), {
                        status: 200,
                        headers: {
                            'Content-Type': 'application/json',
                            'Cache-Control': 'public, max-age=300',
                            ...corsHeaders,
                        },
                    });

                } catch (error) {
                    return new Response(JSON.stringify({
                        error: 'Failed to fetch recording',
                        message: error.message
                    }), {
                        status: 500,
                        headers: { 'Content-Type': 'application/json', ...corsHeaders },
                    });
                }
            }

            // ============================================
            // CATEGORY INDEX ENDPOINT - /api/category-index/:category
            // ============================================
            const categoryIndexMatch = path.match(/^\/api\/category-index\/(mechabrim|chatzeros|albums|piyutim|zmanim|collections|verter|resources|documents)$/);
            if (categoryIndexMatch && request.method === 'GET') {
                const category = categoryIndexMatch[1];

                // Map category to table ID
                const tableMap = {
                    'mechabrim': MECHABRIM_TABLE_ID,
                    'chatzeros': CHATZEROS_TABLE_ID,
                    'verter': VERTER_TABLE_ID,
                    'piyutim': PIYUTIM_TABLE_ID,
                    'zmanim': ZMANIM_TABLE_ID,
                    'collections': COLLECTIONS_TABLE_ID,
                    'resources': RESOURCES_TABLE_ID,
                    'documents': DOCUMENTS_TABLE_ID,
                    'albums': ALBUMS_TABLE_ID,
                };

                const tableId = tableMap[category];

                // Check edge cache
                const cache = caches.default;
                const cacheKey = new Request(request.url);
                const cachedResponse = await cache.match(cacheKey);

                if (cachedResponse) {
                    const newResponse = new Response(cachedResponse.body, cachedResponse);
                    newResponse.headers.set('X-Cache', 'HIT');
                    newResponse.headers.set('Access-Control-Allow-Origin', '*');
                    return newResponse;
                }

                try {
                    console.log(`Building ${category} index...`);
                    const rows = await fetchAllRows(tableId);

                    // Build index - key is the name, value contains id, rowId, customId, and optionally image
                    const index = {};

                    rows.forEach(item => {
                        const values = item.values;

                        // Get name (usually first column or item.name)
                        const name = item.name || '';
                        if (!name) return;

                        // Try to get customId - look for a column with ```#123``` format
                        let customId = null;
                        for (const key of Object.keys(values)) {
                            const val = values[key];
                            if (typeof val === 'string' && val.includes('#')) {
                                const match = val.replace(/```/g, '').match(/#?(\d+)/);
                                if (match) {
                                    customId = match[1];
                                    break;
                                }
                            }
                        }

                        // Try to get image
                        let image = null;
                        for (const key of Object.keys(values)) {
                            const val = values[key];
                            if (val && typeof val === 'object' && val.url &&
                                (val.url.includes('codahosted.io') || val.url.includes('image'))) {
                                image = val.url;
                                break;
                            }
                            if (Array.isArray(val) && val.length > 0 && val[0].url &&
                                (val[0].url.includes('codahosted.io') || val[0].url.includes('image'))) {
                                image = val[0].url;
                                break;
                            }
                        }

                        // Try to get tagName for mechabrim
                        let tagName = null;
                        if (category === 'mechabrim') {
                            for (const key of Object.keys(values)) {
                                const val = extractText(values[key]);
                                if (val && val.length < name.length && val.length > 2) {
                                    tagName = val;
                                    break;
                                }
                            }
                        }

                        index[name] = {
                            id: item.id,           // rowId for URL linking
                            rowId: item.id,
                            customId: customId,    // Numeric ID if available
                            image: image,
                            ...(tagName && { tagName: tagName }),
                        };
                    });

                    let lastUpdated;
                    try {
                        lastUpdated = await env.CACHE_KV.get('lastUpdated');
                    } catch (e) {
                        lastUpdated = null;
                    }

                    const responseData = {
                        lastUpdated: lastUpdated || new Date().toISOString(),
                        category: category,
                        count: Object.keys(index).length,
                        index: index,
                    };

                    const response = new Response(JSON.stringify(responseData), {
                        status: 200,
                        headers: {
                            'Content-Type': 'application/json',
                            'Cache-Control': 'public, max-age=300',
                            'X-Cache': 'MISS',
                            ...corsHeaders,
                        },
                    });

                    ctx.waitUntil(cache.put(cacheKey, response.clone()));
                    return response;

                } catch (error) {
                    console.error(`Error building ${category} index:`, error);
                    return new Response(JSON.stringify({
                        error: `Failed to build ${category} index`,
                        message: error.message
                    }), {
                        status: 500,
                        headers: { 'Content-Type': 'application/json', ...corsHeaders },
                    });
                }
            }

            // ============================================
            // SINGLE SONG ENDPOINT - /api/song/:id
            // ============================================
            const singleSongMatch = path.match(/^\/api\/song\/(.+)$/);
            if (singleSongMatch && request.method === 'GET') {
                const songId = singleSongMatch[1];

                const isRowId = songId.startsWith('i-');

                let codaUrl;
                if (isRowId) {
                    codaUrl = `https://coda.io/apis/v1/docs/${CODA_DOC_ID}/tables/${SONGS_TABLE_ID}/rows/${songId}?valueFormat=rich`;
                } else {
                    const searchTerms = [
                        `#${songId}`,
                        songId,
                        `"#${songId}"`,
                    ];
                    const query = encodeURIComponent(searchTerms[0]);
                    codaUrl = `https://coda.io/apis/v1/docs/${CODA_DOC_ID}/tables/${SONGS_TABLE_ID}/rows?query=${query}&valueFormat=rich&limit=25`;
                }

                console.log(`Fetching single song: ${songId}, isRowId: ${isRowId}`);

                let song = null;

                if (isRowId) {
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
                    const CUSTOM_ID_COLUMN = 'c-DhElYWayZ-';

                    const searchValues = [
                        `\`\`\`#${songId}\`\`\``,
                        `#${songId}`,
                        songId,
                    ];

                    for (const searchValue of searchValues) {
                        if (song) break;

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
                        'Cache-Control': 'public, max-age=300',
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

                const codaResponse = await fetchFromCoda(codaUrl, {
                    method: 'GET',
                    headers: {
                        'Authorization': `Bearer ${env.CODA_API_KEY}`,
                        'Content-Type': 'application/json',
                    },
                });

                if (!codaResponse.ok) {
                    const errorBody = await codaResponse.text();
                    console.error('Coda API error:', {
                        status: codaResponse.status,
                        statusText: codaResponse.statusText,
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

                const response = new Response(JSON.stringify(data), {
                    status: codaResponse.status,
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Cache': 'MISS',
                        'Cache-Control': 'public, max-age=1800',
                        ...corsHeaders,
                    },
                });

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

            // For POST requests - check if it's to the reports table
            if (request.method === 'POST') {
                const bodyText = await request.text();

                // Allow POST to reports table and add-info table
                const isReportsTable = proxyPath.includes('grid-zxaGYhXG8S') ||
                    proxyPath.includes('_tuYhXG8S') ||
                    decodeURIComponent(proxyPath).includes('רעפארטס');

                // Allow POST to add-info table
                const isAddInfoTable = proxyPath.includes('grid-LutCbIuQvQ');

                if (!isReportsTable && !isAddInfoTable) {
                    return new Response(JSON.stringify({
                        error: 'Unauthorized: POST not allowed to this endpoint',
                        path: proxyPath
                    }), {
                        status: 403,
                        headers: { 'Content-Type': 'application/json', ...corsHeaders },
                    });
                }

                const codaResponse = await fetchFromCoda(codaUrl, {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${env.CODA_API_KEY}`,
                        'Content-Type': 'application/json',
                    },
                    body: bodyText,
                });

                const data = await codaResponse.json();
                return new Response(JSON.stringify(data), {
                    status: codaResponse.status,
                    headers: { 'Content-Type': 'application/json', ...corsHeaders },
                });
            }

            // For PATCH requests
            let body = null;
            if (request.method === 'PATCH') {
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
            const errorInfo = {
                message: error.message,
                name: error.name,
                path: path,
                method: request.method,
                url: request.url,
            };

            console.error('Proxy error:', errorInfo);

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
