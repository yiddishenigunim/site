        // API Configuration
        const WORKER_URL = 'https://api.oitzerhanigunim.org/api/';
        const DOC_ID = '6QcJRx7e13';
        const TABLE_ID = 'grid-YycmDjJ8pS'; // Songs table

        // Additional table IDs from Coda doc
        const MECHABRIM_TABLE_ID = 'grid-bCbfhDmi82'; // מחברים טעיבל
        const CHATZEROS_TABLE_ID = 'grid-4-KYWDJ5l9'; // חצרות טעיבל
        const VERTER_TABLE_ID = 'grid-tLcBzyh_tz'; // ווערטער טעיבל
        const PIYUTIM_TABLE_ID = 'grid-318i50N4cK'; // פיוטים טעיבל
        const ZMANIM_TABLE_ID = 'grid-K2rOGXJwjk'; // זמנים טעיבל
        const COLLECTIONS_TABLE_ID = 'grid-K7Mo-tilkg'; // קאלעקשנס טעיבל
        const RESOURCES_TABLE_ID = 'grid-8Gfz8rXNUV'; // רעסורסן טעיבל
        const DOCUMENTS_TABLE_ID = 'grid-3wKzDnkPg-'; // דאקומענטן טעיבל
        const ALBUMS_TABLE_ID = 'grid-bbTaa18Jhx'; // אלבומס טעיבל
        const STATS_TABLE_ID = 'table-CbrETqO5E9'; // סטעטיסטיקס טעיבל

        // Stats column IDs (exact IDs from Coda)
        const STATS_COLS = {
            niggunim: 'c-3wC2msD8Ll',
            mechabrim: 'c-5u6KHT3ZLF',
            verter: 'c-IadpFStSEK',
            chatzeros: 'c-i--tHnlwAu',
            zmanim: 'c-ZFj4le3Q0t',
            piyutim: 'c-0X_OHjkDDx',
            collections: 'c-Y9SNk0BOGV'
        };

        // Recording slots configuration (5 recording slots) - used for rating updates
        const RECORDING_SLOTS = [
            { file: 'c-XmTlDf3xfw', personalities: 'c-s8sYskJc4M', details: 'c-SuWz2qu9B1', rating: 'c-v5V7PENdFy', album: 'c-FuDbL2XiAj' },
            { file: 'c-oqB3imNNfk', personalities: 'c-jRuAfCbs28', details: 'c-_TOU9FXYYn', rating: 'c-YQ-aWW59ej', album: 'c-H0XAyaKkPl' },
            { file: 'c-oGF6C78GZl', personalities: 'c-4trFKY_CK8', details: 'c-dr-m1GnYty', rating: 'c-gmz2rln2dD', album: 'c-HKBBDews5N' },
            { file: 'c-pf9zW3wypK', personalities: 'c-x0WLb4j4wV', details: 'c-pbtldr4CXD', rating: 'c-_GlQy78PAc', album: 'c-imgRABw0tH' },
            { file: 'c-aqbocjDGuL', personalities: 'c-ilrb-DW4dV', details: 'c-2T9FWsVE9h', rating: 'c-yuKHpnbHfM', album: 'c-5FSrRNb-Uw' }
        ];

        // Cached statistics from stats table
        let cachedStats = {
            niggunim: 0,
            mechabrim: 0,
            verter: 0,
            chatzeros: 0,
            zmanim: 0,
            piyutim: 0,
            collections: 0
        };

        // Track which category data has been loaded
        let loadedCategories = {
            chatzeros: false,
            mechabrim: false,
            verter: false,
            zmanim: false,
            piyutim: false,
            collections: false,
            resources: false,
            documents: false,
            albums: false,
            songs: false
        };

        // Nigun modal state (defined early for event handlers)
        let nigunModalOpen = false;
        let savedScrollPosition = 0;
        let currentNigunModalIdx = null;

        // Helper to escape quotes for HTML attributes (converts " to &quot;)
        function escapeHtml(str) {
            if (!str) return '';
            return String(str).replace(/"/g, '&quot;');
        }

        // Global event delegation for handling clicks with special characters
        // This replaces the old escapeAttr/decodeAttr approach that had issues with quotes
        document.addEventListener('click', (e) => {
            // Handle navigate to detail
            const detailEl = e.target.closest('[data-action="detail"]');
            if (detailEl) {
                e.preventDefault();
                e.stopPropagation();
                // Close nigun modal if open before navigating
                if (nigunModalOpen) {
                    nigunModalOpen = false;
                    currentNigunModalIdx = null;
                    document.getElementById('nigunModalOverlay').classList.remove('active');
                    document.body.style.overflow = '';
                }
                navigateToDetail(detailEl.dataset.category, detailEl.dataset.name);
                return;
            }

            // Handle navigate with filter
            const filterEl = e.target.closest('[data-action="filter"]');
            if (filterEl) {
                e.stopPropagation();
                e.preventDefault();
                // Close nigun modal if open before navigating
                if (nigunModalOpen) {
                    nigunModalOpen = false;
                    currentNigunModalIdx = null;
                    document.getElementById('nigunModalOverlay').classList.remove('active');
                    document.body.style.overflow = '';
                }
                navigateToWithFilter(filterEl.dataset.page, filterEl.dataset.filterKey, filterEl.dataset.value);
                if (filterEl.dataset.closeModal === 'true') {
                    closeModal();
                }
                return;
            }

            // Handle navigate to zman detail
            const zmanEl = e.target.closest('[data-action="zman-detail"]');
            if (zmanEl) {
                e.stopPropagation();
                navigateToZmanDetail(zmanEl.dataset.name);
                return;
            }

            // Handle play collection
            const playColEl = e.target.closest('[data-action="play-collection"]');
            if (playColEl) {
                e.stopPropagation();
                playCollectionFromStart(playColEl.dataset.name);
                return;
            }

            // Handle play music category
            const playMusicEl = e.target.closest('[data-action="play-music"]');
            if (playMusicEl) {
                e.stopPropagation();
                playMusicCategory(playMusicEl.dataset.categoryType, playMusicEl.dataset.name);
                return;
            }

            // Handle show all scale songs
            const scaleMoreEl = e.target.closest('[data-action="scale-more"]');
            if (scaleMoreEl) {
                e.stopPropagation();
                showAllScaleSongs(scaleMoreEl.dataset.ritem, scaleMoreEl.dataset.scale);
                return;
            }
        });

        // Global event delegation for tooltips
        document.addEventListener('mouseenter', (e) => {
            if (!(e.target instanceof Element)) return;
            const tooltipEl = e.target.closest('[data-tooltip-value]');
            if (tooltipEl) {
                showTooltip(e, tooltipEl.dataset.tooltipValue, tooltipEl.dataset.tooltipType);
            }
        }, true);

        document.addEventListener('mouseleave', (e) => {
            if (!(e.target instanceof Element)) return;
            const tooltipEl = e.target.closest('[data-tooltip-value]');
            if (tooltipEl) {
                hideTooltip();
            }
        }, true);

        // Hide tooltip immediately when clicking on links or navigating
        document.addEventListener('click', (e) => {
            if (!(e.target instanceof Element)) return;
            const clickedElement = e.target.closest('[data-tooltip-value]');
            if (clickedElement) {
                forceHideTooltip();

                // Check if it's a piyut tooltip click - navigate to piyut detail page
                const tooltipType = clickedElement.getAttribute('data-tooltip-type');
                const tooltipValue = clickedElement.getAttribute('data-tooltip-value');

                if (tooltipType === 'zman' && tooltipValue && piyutimInfo[tooltipValue]) {
                    // This is a piyut, navigate to piyutim detail page
                    const piyutId = piyutimInfo[tooltipValue].customId || tooltipValue;
                    e.preventDefault();
                    e.stopPropagation();
                    window.location.hash = `#/piyutim/${encodeURIComponent(piyutId)}`;
                }
            }
        }, true);

        // Hide tooltip when page visibility changes or before unload
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                forceHideTooltip();
            }
        });

        window.addEventListener('beforeunload', () => {
            forceHideTooltip();
        });

        // --- Musical Notes Loader Helpers ---
        function getTinyLoaderHTML() {
            const notes = ['♪', '♫', '♬', '♩', '♭', '♮', '♯'];
            // Pick 3 random distinct notes
            const shuffled = [...notes].sort(() => 0.5 - Math.random());
            const selected = shuffled.slice(0, 3);

            return `
                <div class="loader-tiny">
                    <span class="note n1">${selected[0]}</span>
                    <span class="note n2">${selected[1]}</span>
                    <span class="note n3">${selected[2]}</span>
                </div>
            `;
        }

        function getMediumLoaderHTML() {
            const notes = ['♪', '♫', '♬', '♩', '♭', '♮', '♯'];
            const shuffled = [...notes].sort(() => 0.5 - Math.random());
            const selected = shuffled.slice(0, 3);

            return `
                <div class="loader-medium">
                    <span class="note n1" style="font-size: 28px;">${selected[0]}</span>
                    <span class="note n2" style="font-size: 34px;">${selected[1]}</span>
                    <span class="note n3" style="font-size: 30px;">${selected[2]}</span>
                </div>
            `;
        }

        // Continuous Symbol Randomization
        // Listen for the end of each animation cycle (when note is invisible)
        // and swap the text content to a new random note.
        document.addEventListener('animationiteration', (e) => {
            // Check if it's a note element
            if (e.target.classList && e.target.classList.contains('note')) {
                // 1. Random Symbol
                const notes = ['♪', '♫', '♬', '♩', '♭', '♮', '♯'];
                const randomNote = notes[Math.floor(Math.random() * notes.length)];
                e.target.innerText = randomNote;

                // 2. Random Trajectory
                const isMedium = e.target.closest('.loader-medium');
                const types = ['Left', 'Right', 'Center'];
                const randomType = types[Math.floor(Math.random() * types.length)];

                const prefix = isMedium ? 'floatMedium' : 'floatTiny';
                const newAnimName = prefix + randomType;

                // Update animation name to shift path
                e.target.style.animationName = newAnimName;
                // Critical: Prevent delay from re-applying on new animation
                e.target.style.animationDelay = '0s';
            }
        }, true);

        // State
        let allSongs = [];
        let filteredSongs = [];
        let currentPage = 1;
        const songsPerPage = 50; // 50 items per batch for infinite scroll
        let currentSongIndex = -1;

        // Lazy loading state for songs page
        let songsLazyState = {
            currentIndex: 0,
            batchSize: 50,
            isLoading: false,
            isInitialLoad: true  // Track if this is the first batch (for animations)
        };
        let currentPlaylist = []; // Track which songs are in current playlist
        let currentPlaylistPosition = -1; // Position within playlist
        let currentRecordingIndex = 0; // Track which recording of current song is playing
        let currentPlayingRecordingInfo = null; // Track current recording info for rating: { songIdx, recordingIdx, slotIndex, rowId }
        let isInRecordingsMode = false; // Are we navigating between recordings or songs?
        let playerVisible = false; // Track if player banner is visible
        let currentPageView = 'home';
        let searchQuery = ''; // Search query for songs

        let currentSortOrder = 'random'; // Sort order: 'random', 'alphabetical', 'recent'
        let currentDisplayMode = 'minimal'; // Display mode: 'minimal', 'recordings', 'full'
        let minQualityRating = 0; // Minimum quality rating (0-5, 0 = all)
        let recordingsCollapsed = false; // Collapse recordings in full mode (show only first)
        let songsFullyLoaded = false; // Track if ALL songs are loaded from API (not just first batch)
        let seenSongIds = new Set(); // Track song IDs from previous batches for FLIP animation
        let isFlipping = false; // Block loadMoreSongs during FLIP animation

        // Category data
        let categories = {
            chatzeros: {},
            mechabrim: {},
            verter: {},
            zmanim: {},
            collections: {}
        };

        // Extended info for all categories (from separate tables)
        let chatzerosInfo = {};
        let mechabrimInfo = {};
        let verterInfo = {};
        let zmaninInfo = {};
        let piyutimInfo = {};
        let collectionsInfo = {};
        let resourcesInfo = {};
        let documentsInfo = {};
        let albumsInfo = {};

        // Table configuration - consolidates all table metadata
        const TABLE_CONFIGS = {
            chatzeros: { id: CHATZEROS_TABLE_ID, store: () => chatzerosInfo, name: 'Chatzeros' },
            mechabrim: { id: MECHABRIM_TABLE_ID, store: () => mechabrimInfo, name: 'Mechabrim' },
            verter: { id: VERTER_TABLE_ID, store: () => verterInfo, name: 'Verter' },
            zmanim: { id: ZMANIM_TABLE_ID, store: () => zmaninInfo, name: 'Zmanim' },
            piyutim: { id: PIYUTIM_TABLE_ID, store: () => piyutimInfo, name: 'Piyutim' },
            collections: { id: COLLECTIONS_TABLE_ID, store: () => collectionsInfo, name: 'Collections' },
            resources: { id: RESOURCES_TABLE_ID, store: () => resourcesInfo, name: 'Resources' },
            documents: { id: DOCUMENTS_TABLE_ID, store: () => documentsInfo, name: 'Documents' },
            albums: { id: ALBUMS_TABLE_ID, store: () => albumsInfo, name: 'Albums' }
        };

        // Page data requirements - what data each page needs loaded
        const PAGE_DATA_REQUIREMENTS = {
            songs: { primary: null, needsSongs: 'wait' }, // Special case: load songs directly
            chatzeros: { primary: 'chatzeros', needsSongs: 'background' },
            mechabrim: { primary: 'mechabrim', needsSongs: 'background' },
            verter: { primary: 'verter', needsSongs: 'background' },
            zmanim: { primary: 'zmanim', additional: ['piyutim'], needsSongs: 'background' },
            collections: { primary: 'collections', needsSongs: 'background' },
            piyutim: { primary: 'piyutim', needsSongs: 'background' },
            resources: { primary: 'resources', needsSongs: false },
            documents: { primary: 'documents', needsSongs: false },
            albums: { primary: 'albums', needsSongs: false },
            search: { primary: null, loadAll: ['chatzeros', 'mechabrim', 'verter', 'albums', 'zmanim', 'piyutim', 'collections'] }
        };

        // --- Helper: Generate loader HTML (consolidated) ---
        function getLoaderHTML(size = 'medium') {
            const notes = ['♪', '♫', '♬', '♩', '♭', '♮', '♯'];
            const shuffled = [...notes].sort(() => 0.5 - Math.random());
            const selected = shuffled.slice(0, 3);

            if (size === 'tiny') {
                return `
                    <div class="loader-tiny">
                        <span class="note n1">${selected[0]}</span>
                        <span class="note n2">${selected[1]}</span>
                        <span class="note n3">${selected[2]}</span>
                    </div>
                `;
            }

            // Medium loader
            return `
                <div class="loader-medium">
                    <span class="note n1" style="font-size: 28px;">${selected[0]}</span>
                    <span class="note n2" style="font-size: 34px;">${selected[1]}</span>
                    <span class="note n3" style="font-size: 30px;">${selected[2]}</span>
                </div>
            `;
        }

        // Backwards compatibility aliases
        function getTinyLoaderHTML() {
            return getLoaderHTML('tiny');
        }

        function getMediumLoaderHTML() {
            return getLoaderHTML('medium');
        }

        // Active filters for all-songs view
        let activeFilters = {
            chatzer: [],
            mechaber: [],
            verter: [],
            zman: [],
            collection: [],
            scale: [],
            ritem: [],
            gezungen: [],
            maure: []
        };

        // ========================================
        // CACHE MANAGER - Minimize API calls
        // ========================================
        const CACHE_VERSION = 'v9'; // Increment when data structure changes
        const CACHE_DURATION = 24 * 60 * 60 * 1000; // 24 hours in milliseconds

        const CacheManager = {
            // Get cached data if valid
            get(key) {
                try {
                    const cacheKey = `${CACHE_VERSION}_${key}`;
                    const cached = localStorage.getItem(cacheKey);
                    if (!cached) return null;

                    const { data, timestamp } = JSON.parse(cached);
                    const now = Date.now();

                    // Check if cache expired
                    if (now - timestamp > CACHE_DURATION) {
                        console.log(`⏰ Cache expired for ${key}`);
                        localStorage.removeItem(cacheKey);
                        return null;
                    }

                    console.log(`✅ Using cached ${key} (age: ${Math.round((now - timestamp) / 1000 / 60)} minutes)`);
                    return data;
                } catch (error) {
                    console.error(`Cache read error for ${key}:`, error);
                    return null;
                }
            },

            // Save data to cache
            set(key, data) {
                try {
                    const cacheKey = `${CACHE_VERSION}_${key}`;
                    const cacheData = {
                        data: data,
                        timestamp: Date.now()
                    };
                    localStorage.setItem(cacheKey, JSON.stringify(cacheData));
                    console.log(`💾 Cached ${key}`);
                } catch (error) {
                    console.error(`Cache write error for ${key}:`, error);
                    // If localStorage is full, clear old caches
                    if (error.name === 'QuotaExceededError') {
                        this.clearOldCaches();
                        // Try again
                        try {
                            localStorage.setItem(cacheKey, JSON.stringify(cacheData));
                        } catch (e) {
                            console.error('Failed to cache even after cleanup');
                        }
                    }
                }
            },

            // Clear all caches (for testing or when structure changes)
            clearAll() {
                const keys = Object.keys(localStorage);
                keys.forEach(key => {
                    if (key.startsWith(CACHE_VERSION + '_')) {
                        localStorage.removeItem(key);
                    }
                });
                console.log('🗑️ All caches cleared');
            },

            // Clear old version caches
            clearOldCaches() {
                const keys = Object.keys(localStorage);
                keys.forEach(key => {
                    if (!key.startsWith(CACHE_VERSION + '_')) {
                        localStorage.removeItem(key);
                    }
                });
                console.log('🗑️ Old version caches cleared');
            }
        };

        // Clear old caches on load
        CacheManager.clearOldCaches();

        // Helper functions for cache (aliases for CacheManager)
        function getCachedData(key) {
            return CacheManager.get(key);
        }

        function setCachedData(key, data) {
            CacheManager.set(key, data);
        }

        // Animation Helper: Count Up Number
        function animateCountUp(element, endValue, duration = 1000, suffix = '') {
            if (!element) return;

            // Cancel running animation
            if (element._animationFrame) {
                cancelAnimationFrame(element._animationFrame);
            }

            // Determine start value from current text
            const currentText = element.textContent.replace(/,/g, '');
            const match = currentText.match(/\d+/);
            let startValue = match ? parseInt(match[0]) : 0;

            // If currently "loading...", startValue is 0
            if (element.classList.contains('loading')) {
                startValue = 0;
                element.classList.remove('loading');
            }

            // If start and end are same, just update text (ensure suffix is correct)
            if (startValue === endValue) {
                element.textContent = endValue + suffix;
                return;
            }

            const startTime = performance.now();

            function update(currentTime) {
                const elapsed = currentTime - startTime;
                const progress = Math.min(elapsed / duration, 1);

                // Ease out cubic
                const ease = 1 - Math.pow(1 - progress, 3);

                const current = Math.floor(startValue + (endValue - startValue) * ease);
                element.textContent = current + suffix;

                if (progress < 1) {
                    element._animationFrame = requestAnimationFrame(update);
                } else {
                    element.textContent = endValue + suffix;
                    element._animationFrame = null;
                }
            }

            element._animationFrame = requestAnimationFrame(update);
        }

        // Helper to set loading state on subtitle
        function setSubtitleLoading(subtitleElement) {
            if (subtitleElement) {
                subtitleElement.textContent = 'לאודט...'; // Placeholder text for screen readers
                subtitleElement.classList.add('loading');
            }
        }

        // Audio player
        const audioPlayer = document.getElementById('audioPlayer');

        // Helper function to extract text from Coda value
        function extractText(value) {
            if (!value && value !== 0) return '';
            if (typeof value === 'number') return String(value);
            if (typeof value === 'string') return value.replace(/```/g, '').trim();
            if (Array.isArray(value)) return value.map(v => extractText(v)).filter(Boolean).join(', ');
            if (typeof value === 'object') {
                const text = value.name || value.display || value.value || '';
                if (typeof text === 'number') return String(text);
                return typeof text === 'string' ? text.replace(/```/g, '').trim() : String(text);
            }
            return '';
        }

        // Helper function to extract image URL from Coda value
        function extractImageUrl(value) {
            if (!value) return null;

            // Direct string URL
            if (typeof value === 'string' && value.startsWith('http')) {
                return value;
            }

            // Array format (Coda's typical image format)
            if (Array.isArray(value) && value.length > 0) {
                const first = value[0];
                // ImageObject format: { "@type": "ImageObject", "url": "..." }
                if (first && first['@type'] === 'ImageObject' && first.url) {
                    return first.url;
                }
                // Simple object with url
                if (first && first.url) {
                    return first.url;
                }
                // Direct string in array
                if (typeof first === 'string' && first.startsWith('http')) {
                    return first;
                }
            }

            // Direct object with url
            if (value && typeof value === 'object' && value.url) {
                return value.url;
            }

            return null;
        }

        // Helper function to extract URL from various Coda formats (WebPage, ImageObject, etc.)
        function extractUrl(value) {
            if (!value) return null;

            // Direct string URL
            if (typeof value === 'string') {
                // Check if it's markdown link format [text](url)
                const markdownMatch = value.match(/\[.*?\]\((https?:\/\/[^\)]+)\)/);
                if (markdownMatch) {
                    return markdownMatch[1];
                }
                if (value.startsWith('http')) {
                    return value;
                }
                return null;
            }

            // WebPage format: { "@type": "WebPage", "url": "..." }
            if (value && typeof value === 'object' && value['@type'] === 'WebPage' && value.url) {
                return value.url;
            }

            // ImageObject format (for PDFs stored as images)
            if (value && typeof value === 'object' && value['@type'] === 'ImageObject' && value.url) {
                return value.url;
            }

            // Array of ImageObjects or WebPages
            if (Array.isArray(value) && value.length > 0) {
                const first = value[0];
                if (first && (first['@type'] === 'ImageObject' || first['@type'] === 'WebPage') && first.url) {
                    return first.url;
                }
                if (first && first.url) {
                    return first.url;
                }
            }

            // Direct object with url
            if (value && typeof value === 'object' && value.url) {
                return value.url;
            }

            return null;
        }

        // Generic function to fetch table info with pagination - with caching
        async function fetchTableInfo(tableId, infoObject, tableName) {
            try {
                // Check cache first
                const cacheKey = `table_${tableId}`;
                const cached = CacheManager.get(cacheKey);
                if (cached) {
                    Object.assign(infoObject, cached);
                    console.log(`✅ ${tableName} loaded from cache (${Object.keys(cached).length} items)`);
                    return;
                }

                console.log(`📡 Fetching ${tableName} from API...`);

                // Get columns first
                const columnsResponse = await fetch(`${WORKER_URL}docs/${DOC_ID}/tables/${tableId}/columns`);
                const columnsData = await columnsResponse.json();
                console.log(`${tableName} columns:`, columnsData.items?.map(c => ({ id: c.id, name: c.name, type: c.format?.type })));

                // Build column map by name (lowercase) and identify special columns
                const columnNames = {};
                let imageColId = null;
                let nameColId = null;
                let detailsColId = null; // Specific column for דעטאלן

                if (columnsData.items) {
                    columnsData.items.forEach((col, idx) => {
                        columnNames[col.id] = col.name || '';
                        const nameLower = (col.name || '').toLowerCase();

                        // Find image column by type or name
                        if (col.format?.type === 'image' || nameLower.includes('בילד') || nameLower.includes('image') || nameLower.includes('תמונה') || nameLower.includes('picture') || nameLower.includes('קאווער') || nameLower.includes('cover')) {
                            imageColId = col.id;
                        }

                        // Find details column specifically
                        if (nameLower.includes('דעטאלן') || nameLower === 'details' || nameLower === 'description') {
                            detailsColId = col.id;
                        }

                        // First column is usually the name
                        if (idx === 0) {
                            nameColId = col.id;
                        }
                    });
                }

                console.log(`${tableName} - Image column: ${imageColId}, Name column: ${nameColId}, Details column: ${detailsColId}`);

                // Fetch all rows with pagination
                let allRows = [];
                let pageToken = null;

                do {
                    const url = `${WORKER_URL}docs/${DOC_ID}/tables/${tableId}/rows?valueFormat=rich${pageToken ? '&pageToken=' + pageToken : ''}`;
                    const response = await fetch(url);
                    if (!response.ok) {
                        console.log(`${tableName} table not accessible:`, response.status);
                        return;
                    }
                    const data = await response.json();
                    allRows = allRows.concat(data.items || []);
                    pageToken = data.nextPageToken;
                } while (pageToken);

                console.log(`${tableName} - Fetched ${allRows.length} total rows`);

                allRows.forEach(item => {
                    const values = item.values;

                    // Get name from first column or name column (this is the tag name / short name)
                    const nameValue = nameColId ? values[nameColId] : values[Object.keys(values)[0]];
                    const tagName = extractText(nameValue);

                    // Use item.name as the full/display name (computed by Coda)
                    const name = item.name || tagName;

                    if (name) {
                        const info = { allData: {}, rowId: item.id };

                        // Always store tag name from first column (will compare with built full name later)
                        if (tagName) {
                            info.tagName = tagName;
                        }

                        // Extract image from identified image column
                        if (imageColId && values[imageColId]) {
                            const imgUrl = extractImageUrl(values[imageColId]);
                            if (imgUrl) {
                                info.image = imgUrl;
                                info.cover = imgUrl; // Also set as cover for albums
                            }
                        }

                        // Extract details/description from identified details column FIRST (priority)
                        if (detailsColId && values[detailsColId]) {
                            const detailsText = extractText(values[detailsColId]);
                            if (detailsText) {
                                info.description = detailsText;
                                console.log(`Set description from details column for ${name}: ${detailsText.substring(0, 50)}...`);
                            }
                        }

                        // Process all columns for other data
                        Object.keys(values).forEach(colId => {
                            if (colId === nameColId || colId === imageColId) return;

                            const value = values[colId];
                            const colName = columnNames[colId] || '';
                            const colNameLower = colName.toLowerCase();

                            // Extract text value for mechabrim column ID matching (works for any value type)
                            const textVal = extractText(value);

                            // Direct column ID matching for ID columns across all tables
                            // Check this BEFORE the textVal check in case the value format is different
                            const idColumnIds = [
                                'c-pDoyO3lhGs', // מחברים ID
                                'c-b0wqs6RvRL', // חצרות ID
                                'c-BUEMMqGeaq', // ווערטער ID
                                'c-u_op0cdc6C', // פיוטים ID
                                'c-Hnj-jcC9Fz', // זמנים ID
                                'c-qWlY1R6WOz', // קאלעקשנס ID
                                'c-ov8ZZ90jjL', // רעסורסן ID
                                'c-ABZxwZf5gG', // דאקומענטן ID
                                'c-6sbXQP4zbj'  // אלבומס ID
                            ];
                            if (idColumnIds.includes(colId)) {
                                // Try multiple ways to extract the ID value
                                let idValue = textVal;
                                if (!idValue && typeof value === 'number') {
                                    idValue = String(value);
                                }
                                if (!idValue && value && typeof value === 'object') {
                                    // Try getting value from object properties
                                    idValue = value.name || value.value || value.text || '';
                                }

                                if (idValue) {
                                    idValue = String(idValue).replace(/^#/, '').trim();
                                    if (idValue) {
                                        info.customId = idValue;
                                    }
                                }
                            }

                            if (textVal) {
                                // Direct column ID matching for mechabrim fields
                                if (colId === 'c-iSyYvX1S79') info.title = textVal;
                                if (colId === 'c-wgHtV5VfbM') info.firstName = textVal;
                                if (colId === 'c-a1I4bjzuvt') info.lastName = textVal;
                                if (colId === 'c-5hlKINjTSq') info.gerufen = textVal;
                                if (colId === 'c-qF9asVJ_z1') info.platz = textVal;
                                if (colId === 'c-2MdIjbFK44') info.suffix = textVal;
                            }

                            // Skip lookup/relation columns (they have complex objects)
                            if (Array.isArray(value) && value.length > 0 && value[0]['@type'] === 'StructuredValue') {
                                // Extract just the name from related records
                                const relatedNames = value.map(v => v.name).filter(Boolean);
                                if (relatedNames.length > 0) {
                                    info.allData[colName] = relatedNames.join(', ');
                                    if (colNameLower.includes('חצר')) {
                                        info.chatzer = relatedNames.join(', ');
                                    }
                                    // Store piyutim list for zmanim
                                    if (colNameLower === 'פיוטים' || colNameLower.includes('פיוט')) {
                                        info.piyutim = relatedNames;
                                    }
                                    // Store collections list for zmanim
                                    if (colNameLower === 'קאלעקשנס' || colNameLower.includes('קאלעקשן')) {
                                        info.collections = relatedNames;
                                    }
                                    // Store zmanim list for collections
                                    if (colNameLower === 'זמנים' || colNameLower.includes('זמן')) {
                                        info.zmanim = relatedNames;
                                    }
                                    // Mechaber birth/death years (lookup fields)
                                    if (colNameLower === 'שנת לידה') {
                                        info.birthYear = relatedNames.join(', ');
                                    }
                                    if (colNameLower === 'שנת פטירה') {
                                        info.deathYear = relatedNames.join(', ');
                                    }
                                }
                                return;
                            }

                            // Handle single StructuredValue (non-array lookup)
                            if (value && typeof value === 'object' && value['@type'] === 'StructuredValue') {
                                const relatedName = value.name;
                                if (relatedName) {
                                    info.allData[colName] = relatedName;
                                    if (colNameLower === 'שנת לידה') {
                                        info.birthYear = relatedName;
                                    }
                                    if (colNameLower === 'שנת פטירה') {
                                        info.deathYear = relatedName;
                                    }
                                    if (colNameLower.includes('חצר')) {
                                        info.chatzer = relatedName;
                                    }
                                }
                                return;
                            }

                            // Handle WebPage format for links
                            if (value && typeof value === 'object' && value['@type'] === 'WebPage') {
                                const url = value.url;
                                if (url) {
                                    info.allData[colName] = url;
                                    if (colNameLower === 'לינק / נאמבער' || colNameLower === 'לינק' || colNameLower === 'link') {
                                        info.link = url;
                                    }
                                    if (colNameLower === 'צו באקומען ביי' || colNameLower.includes('באקומען')) {
                                        info.whereToBuy = url;
                                    }
                                }
                                return;
                            }

                            // Handle ImageObject format (for PDFs/files)
                            const imgObjUrl = extractUrl(value);
                            if (imgObjUrl && (colNameLower === 'פייל' || colNameLower === 'file' || colNameLower === 'בוקלעט' || colNameLower === 'booklet')) {
                                info.allData[colName] = imgObjUrl;
                                if (colNameLower === 'פייל' || colNameLower === 'file') {
                                    info.file = imgObjUrl;
                                }
                                if (colNameLower === 'בוקלעט' || colNameLower === 'booklet') {
                                    info.booklet = imgObjUrl;
                                }
                                return;
                            }

                            // Handle select/array fields (like מקור)
                            let textValue = '';
                            if (Array.isArray(value)) {
                                textValue = value.map(v => {
                                    if (typeof v === 'string') return v.replace(/```/g, '').trim();
                                    if (v && v.name) return v.name;
                                    return '';
                                }).filter(Boolean).join(', ');
                            } else {
                                textValue = extractText(value);
                            }
                            if (textValue) {
                                info.allData[colName] = textValue;

                                // Identify specific fields by column name
                                // Only set description if not already set from details column
                                if (!info.description && (colNameLower.includes('דעטאלן') || colNameLower.includes('אינפארמאציע') || colNameLower.includes('באשרייבונג') || colNameLower.includes('description') || colNameLower.includes('info') || colNameLower.includes('details'))) {
                                    info.description = textValue;
                                    console.log(`Found description for ${name}: ${textValue.substring(0, 50)}...`);
                                }
                                if (colNameLower.includes('מקום') || colNameLower.includes('ארט') || colNameLower.includes('location')) {
                                    info.location = textValue;
                                }
                                if (colNameLower.includes('מקור') && !colNameLower.includes('מראה')) {
                                    info.makor = textValue;
                                }
                                if (colNameLower.includes('מראה מקום')) {
                                    info.marehMakom = textValue;
                                }
                                if (colNameLower.includes('פולע טעקסט') || colNameLower.includes('טעקסט')) {
                                    info.fullText = textValue;
                                }
                                if (colNameLower === 'זמן' || colNameLower === 'zman') {
                                    info.zman = textValue;
                                }
                                if (colNameLower.includes('מייסד') || colNameLower.includes('founder')) {
                                    info.founder = textValue;
                                }
                                if (colNameLower.includes('יאר') || colNameLower.includes('year')) {
                                    info.year = textValue;
                                }
                                if (colNameLower.includes('געבורט') || colNameLower.includes('birth') || colNameLower.includes('geboren')) {
                                    info.birthYear = textValue;
                                }
                                if (colNameLower.includes('פטירה') || colNameLower.includes('נפטר') || colNameLower.includes('death')) {
                                    info.deathYear = textValue;
                                }
                                if (colNameLower.includes('רבי') || colNameLower.includes('rebbe')) {
                                    info.currentRebbe = textValue;
                                }
                                if (colNameLower.includes('באוואוסט') || colNameLower.includes('famous') || colNameLower.includes('known')) {
                                    info.famous = textValue;
                                }
                                // Mechaber specific fields
                                if (colNameLower === 'טעג נאמען' || colNameLower.includes('טעג')) {
                                    info.tagName = textValue;
                                }
                                if (colNameLower === 'טיטל' || colNameLower === 'title') {
                                    info.title = textValue;
                                }
                                if (colNameLower === 'ערשטע נאמען' || colNameLower.includes('first name')) {
                                    info.firstName = textValue;
                                }
                                if (colNameLower === 'לעצטע נאמען' || colNameLower.includes('last name')) {
                                    info.lastName = textValue;
                                }
                                if (colNameLower === 'סופיקס' || colNameLower === 'suffix') {
                                    info.suffix = textValue;
                                }
                                if (colNameLower === 'גערופן' || colNameLower === 'called' || colNameLower === 'nickname') {
                                    info.gerufen = textValue;
                                }
                                if (colNameLower === 'פלאץ' || colNameLower === 'place') {
                                    info.platz = textValue;
                                }
                                if (colNameLower === 'יום לידה') {
                                    info.birthDay = textValue;
                                }
                                if (colNameLower === 'חודש לידה') {
                                    info.birthMonth = textValue;
                                }
                                if (colNameLower === 'שנת לידה') {
                                    info.birthYear = textValue;
                                }
                                if (colNameLower === 'יום פטירה') {
                                    info.deathDay = textValue;
                                }
                                if (colNameLower === 'חודש פטירה') {
                                    info.deathMonth = textValue;
                                }
                                if (colNameLower === 'שנת פטירה') {
                                    info.deathYear = textValue;
                                }
                                // Resources specific fields
                                if (colNameLower === 'לינק / נאמבער' || colNameLower === 'לינק' || colNameLower === 'link') {
                                    info.link = textValue;
                                }
                                if (colNameLower === 'סארט' || colNameLower === 'sort' || colNameLower === 'type') {
                                    info.sort = textValue;
                                }
                                if (colNameLower === 'הערות' || colNameLower === 'notes') {
                                    info.notes = textValue;
                                }
                                // Documents specific fields  
                                if (colNameLower === 'פייל' || colNameLower === 'file') {
                                    info.file = textValue;
                                }
                                if (colNameLower === 'סעריע' || colNameLower === 'serie' || colNameLower === 'series') {
                                    info.serie = textValue;
                                }
                                if (colNameLower === 'נאמען' || colNameLower === 'name') {
                                    info.docName = textValue;
                                }
                                // Albums specific fields
                                if (colNameLower === 'פראדוצירער' || colNameLower === 'producer') {
                                    info.producer = textValue;
                                }
                                if (colNameLower === 'בוקלעט' || colNameLower === 'booklet') {
                                    info.booklet = textValue;
                                }
                                if (colNameLower === 'צו באקומען ביי' || colNameLower.includes('באקומען')) {
                                    info.whereToBuy = textValue;
                                }
                                if (colNameLower === 'קאווער' || colNameLower === 'cover') {
                                    info.cover = textValue;
                                }
                                if (colNameLower === 'שנת' || colNameLower === 'year' || colNameLower === 'יאר') {
                                    info.year = textValue;
                                }
                                if (colNameLower === 'אלבום' || colNameLower === 'album') {
                                    info.albumName = textValue;
                                }
                            }
                        });

                        // Don't set a fallback description - only use actual details/description fields

                        // Trim the name to avoid whitespace issues
                        const trimmedName = name.trim();
                        infoObject[trimmedName] = info;
                    }
                });

                console.log(`Loaded ${tableName} info:`, Object.keys(infoObject).length, 'items');

                // Cache the results
                setCachedData(cacheKey, infoObject);
                console.log(`💾 ${tableName} cached`);

            } catch (error) {
                console.log(`Could not fetch ${tableName} info:`, error.message);
            }
        }

        // Consolidated fetch function - uses TABLE_CONFIGS
        async function fetchTableData(tableKey) {
            const config = TABLE_CONFIGS[tableKey];
            if (!config) {
                console.error(`Unknown table key: ${tableKey}`);
                return;
            }
            await fetchTableInfo(config.id, config.store(), config.name);
        }

        // Backwards compatibility wrappers (call consolidated function)
        async function fetchChatzerosInfo() { await fetchTableData('chatzeros'); }
        async function fetchMechabrimInfo() { await fetchTableData('mechabrim'); }
        async function fetchVerterInfo() { await fetchTableData('verter'); }
        async function fetchZmanimInfo() { await fetchTableData('zmanim'); }
        async function fetchPiyutimInfo() { await fetchTableData('piyutim'); }
        async function fetchCollectionsInfo() { await fetchTableData('collections'); }
        async function fetchResourcesInfo() { await fetchTableData('resources'); }
        async function fetchDocumentsInfo() { await fetchTableData('documents'); }
        async function fetchAlbumsInfo() { await fetchTableData('albums'); }

        // Fetch statistics from STATS table (only row 1) - with caching
        async function fetchStatistics() {
            try {
                // Check cache first
                const cacheKey = 'statistics';
                const cached = getCachedData(cacheKey);
                if (cached) {
                    Object.assign(cachedStats, cached);
                    console.log('✅ Statistics loaded from cache:', cachedStats);
                    return;
                }

                console.log('📡 Fetching statistics from API:', STATS_TABLE_ID);

                // Fetch stats row directly using known column IDs
                const url = `${WORKER_URL}docs/${DOC_ID}/tables/${STATS_TABLE_ID}/rows?valueFormat=simple&limit=1`;
                const response = await fetch(url);

                if (!response.ok) {
                    const errorText = await response.text();
                    console.error('Stats API error:', errorText);
                    throw new Error(`HTTP error! status: ${response.status}`);
                }

                const data = await response.json();

                if (data.items && data.items.length > 0) {
                    const row = data.items[0];
                    const values = row.values;

                    console.log('📊 Stats row values:', values);

                    // Helper to get number from value (handles both direct numbers and strings)
                    const getNum = (val) => {
                        if (val === null || val === undefined) return 0;
                        if (typeof val === 'number') return val;
                        if (typeof val === 'string') return parseInt(val) || 0;
                        return parseInt(extractText(val)) || 0;
                    };

                    // Extract statistics using exact column IDs
                    cachedStats.niggunim = getNum(values[STATS_COLS.niggunim]);
                    cachedStats.mechabrim = getNum(values[STATS_COLS.mechabrim]);
                    cachedStats.verter = getNum(values[STATS_COLS.verter]);
                    cachedStats.chatzeros = getNum(values[STATS_COLS.chatzeros]);
                    cachedStats.zmanim = getNum(values[STATS_COLS.zmanim]);
                    cachedStats.piyutim = getNum(values[STATS_COLS.piyutim]);
                    cachedStats.collections = getNum(values[STATS_COLS.collections]);

                    console.log('📊 Parsed stats:', cachedStats);

                    // Save to cache
                    setCachedData(cacheKey, { ...cachedStats });

                    console.log('✅ Statistics loaded from API and cached:', cachedStats);
                } else {
                    console.error('❌ No stats data in response');
                }
            } catch (error) {
                console.error('❌ Statistics fetch error:', error);
                throw error;
            }
        }

        // Fetch songs from API - OPTIMIZED: Only load stats, everything else is lazy loaded
        async function fetchSongs() {
            try {
                console.log('🚀 Starting initialization (optimized - lazy loading)...');

                // ONLY load statistics - no category info, no songs yet
                try {
                    await fetchStatistics();
                    console.log('✅ Statistics loaded:', cachedStats);
                } catch (statsError) {
                    console.error('⚠️ Statistics failed to load, using defaults:', statsError);
                    // Continue anyway with default 0 values
                }

                // Render home page NOW with statistics (or defaults)
                if (!window.location.hash || window.location.hash === '#/home' || window.location.hash === '#/') {
                    updateURL();
                    renderCurrentPage();
                    console.log('✅ Home page rendered');
                } else {
                    // Navigate based on URL - category data will be loaded on demand
                    handleURLChange();
                }

            } catch (error) {
                console.error('❌ Fatal error:', error);
                const content = document.getElementById('mainContent');
                if (content) {
                    content.innerHTML = `
                        <div class="empty-state">
                            <div class="empty-state-icon">❌</div>
                            <h2>גרייז לאָודינג</h2>
                            <p>${error.message}</p>
                            <button onclick="location.reload()" style="margin-top: 20px; padding: 10px 20px; background: var(--primary); color: white; border: none; border-radius: 8px; cursor: pointer;">פּרוּוו ווידער</button>
                        </div>
                    `;
                }
            }
        }

        // Ensure required data is loaded for a specific page
        // Consolidated data loading using PAGE_DATA_REQUIREMENTS
        async function ensureDataForPage(page, waitForAll = false) {
            const requirements = PAGE_DATA_REQUIREMENTS[page];
            if (!requirements) return; // Unknown page

            // Special case: songs page
            if (page === 'songs') {
                if (!loadedCategories.songs) {
                    await fetchSongsInBackground();
                }
                if (waitForAll && !songsFullyLoaded) {
                    await fetchSongsInBackground();
                }
                return;
            }

            // Special case: search - load all data
            if (requirements.loadAll) {
                const promises = [];
                requirements.loadAll.forEach(key => {
                    if (!loadedCategories[key]) {
                        promises.push(fetchTableData(key).then(() => loadedCategories[key] = true));
                    }
                });
                if (!loadedCategories.songs) {
                    promises.push(fetchSongsInBackground());
                }
                await Promise.all(promises);
                return;
            }

            // Standard case: load primary data
            if (requirements.primary) {
                const key = requirements.primary;
                if (!loadedCategories[key]) {
                    await fetchTableData(key);
                    loadedCategories[key] = true;
                }
            }

            // Load additional data if specified
            if (requirements.additional) {
                for (const key of requirements.additional) {
                    if (!loadedCategories[key]) {
                        await fetchTableData(key);
                        loadedCategories[key] = true;
                    }
                }
            }

            // Load songs in background if needed
            if (requirements.needsSongs === 'background' && !loadedCategories.songs) {
                fetchSongsInBackground(); // Don't await - load in background
            } else if (requirements.needsSongs === 'wait' && !loadedCategories.songs) {
                await fetchSongsInBackground();
            }
        }

        // Flag to prevent duplicate fetches
        let songsCurrentlyLoading = false;
        let songsLoadingPromise = null;

        // Fetch songs in background with pagination - OPTIMIZED: first 50 items initially
        async function fetchSongsInBackground() {
            // Prevent duplicate fetches - return existing promise if already loading
            if (songsCurrentlyLoading && songsLoadingPromise) {
                console.log('⏳ Songs already loading, returning existing promise');
                return songsLoadingPromise;
            }
            songsCurrentlyLoading = true;

            // Create a promise that will be resolved when songs are loaded
            songsLoadingPromise = (async () => {

                try {
                    // Check cache first
                    const cachedSongs = getCachedData('songs');
                    if (cachedSongs && cachedSongs.length > 0) {
                        console.log(`Using cached songs: ${cachedSongs.length} items`);
                        allSongs = cachedSongs;
                        loadedCategories.songs = true;
                        songsFullyLoaded = true; // Cached songs are complete
                        applyFiltersQuiet();  // Quiet version to avoid animation on load
                        buildCategoryIndices();
                        updateNavCounts();
                        if (currentPageView === 'songs') {
                            // Only update the songs grid, not the whole page
                            updateSongsCount();
                            const grid = document.getElementById('songsGrid');
                            if (grid && grid.children.length === 0) {
                                loadMoreSongs(true);
                            }
                        }
                        songsCurrentlyLoading = false;
                        songsLoadingPromise = null;
                        return;
                    }

                    let allRows = [];
                    let pageToken = null;
                    let isFirstBatch = true;

                    // Show loading wave bar
                    const waveBar = document.getElementById('loadingWaveBar');
                    if (waveBar) waveBar.classList.add('active');

                    do {
                        const cacheBuster = Date.now();
                        const url = `${WORKER_URL}docs/${DOC_ID}/tables/${TABLE_ID}/rows?valueFormat=rich&_cb=${cacheBuster}${pageToken ? '&pageToken=' + pageToken : ''}`;
                        const response = await fetch(url);
                        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
                        const data = await response.json();
                        allRows = allRows.concat(data.items || []);
                        pageToken = data.nextPageToken;

                        // Process songs so far and update counts
                        processSongsData(allRows);

                        // Update filteredSongs after each batch
                        // First batch: shuffle for random mode, skip sort for alphabetical (for FLIP)
                        // Subsequent batches: skip sort for shuffle (preserve order), sort for alphabetical
                        if (isFirstBatch) {
                            if (currentSortOrder === 'random') {
                                applyFiltersQuiet(false); // Shuffle for random mode
                                console.log('First batch: shuffling songs for random mode');
                            } else {
                                applyFiltersQuiet(true); // Skip sort for FLIP-friendly display
                                console.log('First batch: skipping sort for FLIP-friendly display');
                            }
                        } else if (currentSortOrder === 'random') {
                            // In shuffle mode: DON'T re-shuffle! Keep existing order
                            // Just filter and append new songs - FLIP will insert at random positions
                            applyFiltersQuiet(true); // Skip sort to preserve order
                            console.log('Subsequent batch: shuffle mode - preserving existing order');
                        } else {
                            applyFiltersQuiet(false); // Sort to put new songs in correct position
                        }

                        if (currentPageView === 'songs') {
                            updateSongsCount();
                        }

                        // After first batch, render and mark DISPLAYED songs as "seen"
                        if (isFirstBatch && allRows.length >= 50) {
                            isFirstBatch = false;
                            loadedCategories.songs = true;
                            buildCategoryIndices();

                            if (currentPageView === 'songs') {
                                const grid = document.getElementById('songsGrid');
                                if (grid) {
                                    // For sorted modes: display 1 song from middle position
                                    // This allows earlier songs to FLIP in before it
                                    if (currentSortOrder !== 'random') {
                                        // Sort first to know correct order
                                        applyFiltersQuiet(false);
                                        // Seed with song at position 49 (or last if less)
                                        const seedPosition = Math.min(49, filteredSongs.length - 1);
                                        const seedSong = filteredSongs[seedPosition];
                                        const seedIdx = allSongs.indexOf(seedSong);

                                        // Create the seed element
                                        const playlistIndices = filteredSongs.map(song => allSongs.indexOf(song));
                                        const seedHtml = renderSongItem(seedSong, seedIdx, seedPosition + 1, playlistIndices);
                                        grid.insertAdjacentHTML('beforeend', seedHtml);
                                        songsLazyState.currentIndex = 1;

                                        // Mark only this song as seen
                                        seenSongIds = new Set([seedSong.rowId || seedSong.name]);
                                        console.log(`First batch: seeded with song at position ${seedPosition} (alphabetical mode)`);
                                    } else {
                                        // Shuffle mode: display first song normally
                                        songsLazyState.batchSize = 1;
                                        loadMoreSongs(true);
                                        songsLazyState.batchSize = 50;

                                        const displayedSongs = filteredSongs.slice(0, 1);
                                        seenSongIds = new Set(displayedSongs.map(s => s.rowId || s.name));
                                        console.log(`First batch: marked ${seenSongIds.size} displayed song as seen (shuffle mode)`);
                                    }
                                } else {
                                    renderCurrentPage();
                                    seenSongIds = new Set(allSongs.map(s => s.rowId || s.name));
                                }
                            } else {
                                seenSongIds = new Set(allSongs.map(s => s.rowId || s.name));
                            }
                        } else if (!isFirstBatch) {
                            // Subsequent batches: identify new songs (not in displayed set)
                            const newSongs = allSongs.filter(s => {
                                const id = s.rowId || s.name;
                                return !seenSongIds.has(id);
                            });

                            // Only mark songs in VISIBLE RANGE as seen (not all)
                            // This allows later batches to FLIP into positions 0-49
                            const displayedCount = document.getElementById('songsGrid')?.children.length || 50;
                            const visibleRange = Math.max(displayedCount + 20, 50);
                            newSongs.forEach(s => {
                                const posInFiltered = filteredSongs.indexOf(s);
                                if (posInFiltered < visibleRange) {
                                    seenSongIds.add(s.rowId || s.name);
                                }
                            });

                            console.log(`FLIP check: newSongs=${newSongs.length}, currentSortOrder=${currentSortOrder}`);

                            // FLIP for ALL sort modes - show songs popping in
                            if (currentPageView === 'songs' && newSongs.length > 0) {
                                const grid = document.getElementById('songsGrid');
                                if (grid && grid.children.length > 0) {
                                    updateSongsListWithNewSongs(newSongs);
                                }
                            }

                            // Update category page incrementally as songs load
                            const categoryPagesForUpdate = ['chatzeros', 'mechabrim', 'verter', 'zmanim', 'collections'];
                            if (categoryPagesForUpdate.includes(currentPageView)) {
                                buildCategoryIndices();
                                updateCategoryPageCards();
                            }
                        }

                        console.log(`Loaded ${allRows.length} songs...`);
                    } while (pageToken);

                    console.log(`Finished loading ${allRows.length} total songs`);
                    loadedCategories.songs = true;
                    songsFullyLoaded = true; // All songs are now loaded

                    // Cache the processed songs
                    setCachedData('songs', allSongs);

                    // Final update - just refresh filters and count (no animation for background data)
                    applyFiltersQuiet();  // Update filteredSongs without animation
                    buildCategoryIndices();

                    // Update category page if user is viewing one (categories grew as songs loaded)
                    const categoryPagesList = ['chatzeros', 'mechabrim', 'verter', 'zmanim', 'collections'];
                    if (categoryPagesList.includes(currentPageView)) {
                        // Final update with all categories
                        updateCategoryPageCards();
                        // Make sure wave bar is hidden now that all songs are loaded
                        const categoryLoader = document.getElementById('categoryLoader');
                        if (categoryLoader) categoryLoader.classList.remove('active');
                    }

                    // Hide loading wave bar and update subtitle (fade out ball)
                    const waveBarToHide = document.getElementById('loadingWaveBar');
                    if (waveBarToHide) waveBarToHide.classList.remove('active');

                    // Update count display to fade out loading ball
                    if (currentPageView === 'songs') {
                        updateSongsCount();
                    }

                    songsCurrentlyLoading = false;
                    songsLoadingPromise = null;

                } catch (error) {
                    console.error('Error loading songs:', error);
                    songsCurrentlyLoading = false;
                    songsLoadingPromise = null;
                    throw error;
                }
            })();

            return songsLoadingPromise;
        }

        // Process songs data array
        function processSongsData(rows) {
            allSongs = rows.map((item, index) => {
                const values = item.values;

                // Find customId from any column ending with "ID"
                let customId = null;
                for (const [colId, value] of Object.entries(values)) {
                    const textVal = extractText(value);
                    if (textVal && /^#?\d+$/.test(textVal.trim())) {
                        // This looks like an ID (e.g. "#781" or "781")
                        customId = textVal.replace(/^#/, '').trim();
                        break;
                    }
                }

                // Helper to check file type
                const isAudioFile = (filename) => {
                    if (!filename) return false;
                    const ext = filename.toLowerCase().split('.').pop();
                    return ['mp3', 'wav', 'ogg', 'm4a', 'aac', 'flac', 'wma'].includes(ext);
                };

                const isImageFile = (filename) => {
                    if (!filename) return false;
                    const ext = filename.toLowerCase().split('.').pop();
                    return ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'svg'].includes(ext);
                };

                const isPdfFile = (filename) => {
                    if (!filename) return false;
                    const ext = filename.toLowerCase().split('.').pop();
                    return ext === 'pdf';
                };

                const isVideoFile = (filename) => {
                    if (!filename) return false;
                    const ext = filename.toLowerCase().split('.').pop();
                    return ['mp4', 'webm', 'mov', 'avi', 'mkv', 'm4v'].includes(ext);
                };

                // Recording slots configuration (5 recording slots)
                const recordingSlots = [
                    { file: 'c-XmTlDf3xfw', personalities: 'c-s8sYskJc4M', details: 'c-SuWz2qu9B1', rating: 'c-v5V7PENdFy', album: 'c-FuDbL2XiAj' },
                    { file: 'c-oqB3imNNfk', personalities: 'c-jRuAfCbs28', details: 'c-_TOU9FXYYn', rating: 'c-YQ-aWW59ej', album: 'c-H0XAyaKkPl' },
                    { file: 'c-oGF6C78GZl', personalities: 'c-4trFKY_CK8', details: 'c-dr-m1GnYty', rating: 'c-gmz2rln2dD', album: 'c-HKBBDews5N' },
                    { file: 'c-pf9zW3wypK', personalities: 'c-x0WLb4j4wV', details: 'c-pbtldr4CXD', rating: 'c-_GlQy78PAc', album: 'c-imgRABw0tH' },
                    { file: 'c-aqbocjDGuL', personalities: 'c-ilrb-DW4dV', details: 'c-2T9FWsVE9h', rating: 'c-yuKHpnbHfM', album: 'c-5FSrRNb-Uw' }
                ];

                // Extract recordings with full metadata
                const recordings = [];
                let recordingCounter = 1;

                recordingSlots.forEach((slot, slotIndex) => {
                    let fileAttachment = values[slot.file];

                    // Handle both array and single object formats from Coda
                    if (!fileAttachment) return;

                    // If it's a single object (not an array), wrap it in an array
                    if (!Array.isArray(fileAttachment)) {
                        if (fileAttachment.url && fileAttachment.name) {
                            fileAttachment = [fileAttachment];
                        } else {
                            return; // Not a valid file attachment
                        }
                    }

                    if (fileAttachment.length === 0) return;

                    const personalities = extractText(values[slot.personalities]) || '';
                    const details = extractText(values[slot.details]) || '';
                    const ratingVal = extractText(values[slot.rating]) || '';
                    const album = extractText(values[slot.album]) || '';

                    // Parse rating (1-5 stars)
                    let rating = 0;
                    if (ratingVal) {
                        const parsed = parseInt(ratingVal, 10);
                        if (!isNaN(parsed) && parsed >= 1 && parsed <= 5) {
                            rating = parsed;
                        }
                    }

                    // Process all files in this attachment field (can have multiple)
                    fileAttachment.forEach((file, fileIdx) => {
                        if (!file.url || !isAudioFile(file.name)) return;

                        const suffix = fileAttachment.length > 1 ? ` (${fileIdx + 1})` : '';
                        recordings.push({
                            url: file.url,
                            name: file.name || `רעקארדינג ${recordingCounter}`,
                            details: details,
                            personalities: personalities,
                            album: album,
                            rating: rating,
                            displayName: details || personalities || album || `רעקארדינג ${recordingCounter}${suffix}`,
                            slotIndex: slotIndex,
                            fileIndex: fileIdx
                        });
                        recordingCounter++;
                    });
                });

                // Sort recordings by rating (highest first), then those without rating
                recordings.sort((a, b) => {
                    if (a.rating && b.rating) return b.rating - a.rating;
                    if (a.rating && !b.rating) return -1;
                    if (!a.rating && b.rating) return 1;
                    return 0;
                });

                // Get main audio URL (first recording)
                let mainAudioUrl = recordings.length > 0 ? recordings[0].url : null;

                // 6th attachment slot - for other media (images, videos, PDFs, additional audio)
                const otherMedia = {
                    text: extractText(values['c-l9TdqiWUNg']) || '',
                    audio: [],
                    files: [],
                    images: [],
                    videos: [],
                    pdfs: []
                };

                const attachment6 = values['c-_q44nnca44'];
                if (Array.isArray(attachment6)) {
                    attachment6.forEach((file, idx) => {
                        if (!file.url) return;
                        const fileName = file.name || `קובץ ${idx + 1}`;

                        if (isAudioFile(fileName)) {
                            otherMedia.audio.push({ url: file.url, name: fileName });
                        } else if (isImageFile(fileName)) {
                            otherMedia.images.push({ url: file.url, name: fileName });
                        } else if (isVideoFile(fileName)) {
                            otherMedia.videos.push({ url: file.url, name: fileName });
                        } else if (isPdfFile(fileName)) {
                            otherMedia.pdfs.push({ url: file.url, name: fileName });
                        } else {
                            otherMedia.files.push({ url: file.url, name: fileName });
                        }
                    });
                }

                // Legacy arrays for backward compatibility
                const images = otherMedia.images;
                const pdfs = otherMedia.pdfs;

                // Find personality field (פערזענליכקייט) - column c-mxsPhpZeVM
                const songName = extractText(values['c-6qqDF7NYmv']) || '';

                // Extract personality from the known column
                const personalityNames = extractText(values['c-6E02UJxxZW']) || ''; // פערזענליכקייט

                // Also try the specific ID column if customId not found
                if (!customId) {
                    const customIdVal = extractText(values['c-DhElYWayZ-']);
                    if (customIdVal) {
                        customId = customIdVal.replace(/^#/, '').trim();
                    }
                }

                return {
                    rowId: item.id,
                    customId: customId,
                    name: songName || `ליד ${index + 1}`,
                    author: extractText(values['c-ujrTHeCJGo']),
                    category: extractText(values['c-u0H9G6FHoJ']), // חצר
                    verter: extractText(values['c-KAWfVfB4jq']),
                    mechaber: extractText(values['c-ujrTHeCJGo']),
                    personality: personalityNames, // פערזענליכקייט
                    scale: extractText(values['c-wQZrHPVWog']),
                    ritem: extractText(values['c-OmWEZXO7Ys']),
                    gezungen: extractText(values['c-kiHIam57Z0']),
                    collections: extractText(values['c-pCBFiGU8ex']),
                    zugeleigt: extractText(values['c-iOlvD8H0n3']), // צוגעלייגט אום 
                    pasigOif: extractText(values['c-mTtYp3FK9U']), // פאסיג אויף / זמנים
                    maure: extractText(values['c-J3q8mQIXBh']), // מאורע
                    info: extractText(values['c-0jFLHVYRL8']),
                    siman: extractText(values['c-jSVbHezFAS']),
                    notn: extractText(values['c-hodtA6zYsy']),
                    documents: extractText(values['c-nF5sRFoMkJ']), // דאקומענטן
                    albums: extractText(values['c-j8dujLHcnT']), // אלבומס
                    resources: extractText(values['c-KBb2Bgi6nM']), // רעסורסן
                    audioUrl: mainAudioUrl,
                    recordings: recordings,
                    images: images,
                    pdfs: pdfs,
                    otherMedia: otherMedia
                };
            });

            // NOTE: Do NOT sort allSongs here! 
            // allSongs must keep stable indices since data-song-idx attributes reference them.
            // Sorting is done on filteredSongs via applyFiltersQuiet() instead.

            // Apply initial filters quietly (no animation during background loading)
            applyFiltersQuiet();

            // Build category indices
            buildCategoryIndices();

            // Update nav counts
            updateNavCounts();
        }

        // Build category indices
        function buildCategoryIndices() {
            // Reset
            categories = {
                chatzeros: {},
                mechabrim: {},
                verter: {},
                zmanim: {},
                collections: {},
                gezungen: {},
                scale: {},
                ritem: {}
            };

            allSongs.forEach((song, idx) => {
                // חצרות
                if (song.category) {
                    song.category.split(',').forEach(c => {
                        const key = c.trim();
                        if (key) {
                            if (!categories.chatzeros[key]) categories.chatzeros[key] = [];
                            categories.chatzeros[key].push(idx);
                        }
                    });
                }

                // מחברים
                if (song.mechaber) {
                    song.mechaber.split(',').forEach(m => {
                        const key = m.trim();
                        if (key) {
                            if (!categories.mechabrim[key]) categories.mechabrim[key] = [];
                            categories.mechabrim[key].push(idx);
                        }
                    });
                }

                // ווערטער
                if (song.verter) {
                    song.verter.split(',').forEach(v => {
                        const key = v.trim();
                        if (key) {
                            if (!categories.verter[key]) categories.verter[key] = [];
                            categories.verter[key].push(idx);
                        }
                    });
                }

                // זמנים/פיוטים (from פאסיג אויף)
                if (song.pasigOif) {
                    song.pasigOif.split(',').forEach(z => {
                        const key = z.trim();
                        if (key) {
                            if (!categories.zmanim[key]) categories.zmanim[key] = [];
                            categories.zmanim[key].push(idx);
                        }
                    });
                }

                // קאלעקשאנס
                if (song.collections) {
                    song.collections.split(',').forEach(col => {
                        const key = col.trim();
                        if (key) {
                            if (!categories.collections[key]) categories.collections[key] = [];
                            categories.collections[key].push(idx);
                        }
                    });
                }

                // געזונגען אויף
                if (song.gezungen) {
                    song.gezungen.split(',').forEach(g => {
                        const key = g.trim();
                        if (key) {
                            if (!categories.gezungen[key]) categories.gezungen[key] = [];
                            categories.gezungen[key].push(idx);
                        }
                    });
                }

                // סקעיל
                if (song.scale) {
                    song.scale.split(',').forEach(s => {
                        const key = s.trim();
                        if (key) {
                            if (!categories.scale[key]) categories.scale[key] = [];
                            categories.scale[key].push(idx);
                        }
                    });
                }

                // ריטעם
                if (song.ritem) {
                    song.ritem.split(',').forEach(r => {
                        const key = r.trim();
                        if (key) {
                            if (!categories.ritem[key]) categories.ritem[key] = [];
                            categories.ritem[key].push(idx);
                        }
                    });
                }
            });
        }

        // Update navigation counts
        function updateNavCounts() {
            // Nav counts removed - function kept for compatibility
        }

        // Navigation - with lazy loading
        // Generate skeleton loader HTML for a page
        function getSkeletonForPage(page) {
            // Using consolidated utility classes: 'page-theme theme-{name}'
            const pageThemes = {
                'songs': 'page-theme theme-nigun',
                'chatzeros': 'page-theme theme-chatzer',
                'mechabrim': 'page-theme theme-mechaber',
                'verter': 'page-theme theme-verter',
                'zmanim': 'page-theme theme-zman',
                'collections': 'page-theme theme-collection',
                'resources': 'page-theme theme-resource',
                'documents': 'page-theme theme-document',
                'albums': 'page-theme theme-album',
                'music': 'page-theme theme-music'
            };

            // Page titles and subtitles for real headers
            const pageTitles = {
                'songs': { title: 'ניגונים', subtitle: '', countLabel: 'ניגונים' },
                'chatzeros': { title: 'חצרות', subtitle: 'חצרות', countLabel: 'חצרות' },
                'mechabrim': { title: 'מחברים', subtitle: 'מחברים', countLabel: 'מחברים' },
                'verter': { title: 'ווערטער', subtitle: 'ווערטער', countLabel: 'ווערטער' },
                'zmanim': { title: 'זמנים', subtitle: 'זמנים', countLabel: 'זמנים' },
                'collections': { title: 'קאלעקשאנס', subtitle: 'קאלעקשאנס', countLabel: 'קאלעקשאנס' },
                'resources': { title: 'רעסורסן', subtitle: 'רעסורסן', countLabel: 'רעסורסן' },
                'documents': { title: 'דאקומענטן', subtitle: 'דאקומענטן', countLabel: 'דאקומענטן' },
                'albums': { title: 'אלבומס', subtitle: 'אלבומס', countLabel: 'אלבומס' },
                'music': { title: 'מוזיק', subtitle: 'מוזיק', countLabel: 'מוזיק' }
            };

            const themeClass = pageThemes[page] || 'page-theme theme-nigun';
            const pageInfo = pageTitles[page] || { title: '', subtitle: '', countLabel: '' };

            // Songs page has a different layout with sidebar
            if (page === 'songs') {
                return `
                    <div class="${themeClass}">
                        <div class="page-title">
                            <div class="page-title-bar">${pageInfo.title}</div>
                            <div class="page-title-content">
                                <div class="subtitle"><span class="count-number">0</span> ${pageInfo.countLabel}</div>
                                <div class="loading-wave-bar active" id="loadingWaveBar"></div>
                            </div>
                        </div>
                        <div class="skeleton-songs-layout">
                            <div class="skeleton-songs-main">
                                <div class="skeleton-song-row"></div>
                                <div class="skeleton-song-row"></div>
                                <div class="skeleton-song-row"></div>
                                <div class="skeleton-song-row"></div>
                                <div class="skeleton-song-row"></div>
                                <div class="skeleton-song-row"></div>
                                <div class="skeleton-song-row"></div>
                                <div class="skeleton-song-row"></div>
                            </div>
                            <div class="skeleton-songs-sidebar">
                                <div class="skeleton skeleton-search-box"></div>
                                <div class="skeleton skeleton-filter"></div>
                                <div class="skeleton skeleton-filter"></div>
                                <div class="skeleton skeleton-filter"></div>
                                <div class="skeleton skeleton-filter"></div>
                            </div>
                        </div>
                    </div>
                `;
            }

            // Category pages (chatzeros, mechabrim, etc.) with grid - REAL header, skeleton cards
            return `
                <div class="${themeClass}">
                    <div class="page-title">
                        <div class="page-title-bar">${pageInfo.title}</div>
                        <div class="page-title-content">
                            <div class="subtitle"><span class="subtitle-count">0</span> ${pageInfo.countLabel}</div>
                            <div class="loading-wave-bar active" id="categoryLoader"></div>
                        </div>
                    </div>
                    <div class="skeleton-grid">
                        <div class="skeleton-card"></div>
                        <div class="skeleton-card"></div>
                        <div class="skeleton-card"></div>
                        <div class="skeleton-card"></div>
                        <div class="skeleton-card"></div>
                        <div class="skeleton-card"></div>
                        <div class="skeleton-card"></div>
                        <div class="skeleton-card"></div>
                        <div class="skeleton-card"></div>
                    </div>
                </div>
            `;
        }

        // Info pages list for special transitions
        const infoPages = ['about', 'instructions', 'stats', 'credits', 'thanks', 'terms', 'contribute'];

        // Page order for determining navigation direction
        const pageOrder = ['home', 'songs', 'mechabrim', 'chatzeros', 'zmanim', 'collections', 'verter', 'music', 'resources', 'documents', 'albums'];

        // Get navigation direction based on page order
        function getNavigationDirection(fromPage, toPage) {
            const fromIdx = pageOrder.indexOf(fromPage);
            const toIdx = pageOrder.indexOf(toPage);
            // If one of the pages is not in the list (info pages, etc.), default to forward
            if (fromIdx === -1 || toIdx === -1) return 'forward';
            return toIdx > fromIdx ? 'forward' : 'backward';
        }

        async function navigateTo(page) {
            hideAllDropdowns();
            const content = document.getElementById('mainContent');
            const isHomePage = page === 'home';
            const wasHomePage = currentPageView === 'home';
            const isInfoPage = infoPages.includes(page);
            const wasInfoPage = infoPages.includes(currentPageView);

            // Map URL page names to internal page names
            const internalPage = page === 'nigunim' ? 'songs' : page;
            const direction = getNavigationDirection(currentPageView, internalPage);

            // Determine exit animation class
            let exitClass;
            if (wasHomePage) {
                exitClass = 'page-exit-home'; // Home always exits to right
            } else if (wasInfoPage) {
                exitClass = 'page-exit-info'; // Info pages just fade out
            } else {
                // Content pages: direction-aware
                exitClass = direction === 'forward' ? 'page-exit' : 'page-exit-back';
            }

            // Add exit animation
            if (content && content.children.length > 0) {
                content.classList.add(exitClass);
                await new Promise(r => setTimeout(r, wasInfoPage ? 150 : 200));
                content.classList.remove(exitClass);
            }

            currentPageView = internalPage;
            currentPage = 1;
            currentDetailView = null; // Clear any detail view

            updateURL();

            // Track if we showed skeleton (data wasn't loaded)
            const hadSkeleton = internalPage !== 'home' && !isInfoPage && !isDataLoadedForPage(internalPage);

            // Show skeleton loader for pages that need data
            if (hadSkeleton) {
                if (content) {
                    content.innerHTML = getSkeletonForPage(internalPage);
                }
                // Load required data for this page
                await ensureDataForPage(internalPage);
            }

            renderCurrentPage();

            // Determine enter animation class
            // Skip animation if we showed skeleton (header is already visible)
            let enterClass;
            if (hadSkeleton) {
                enterClass = null; // Coming from skeleton - no page animation needed
            } else if (isHomePage) {
                enterClass = null; // Home page uses its own stagger animation, skip page transition
            } else if (isInfoPage) {
                enterClass = 'page-enter-info'; // Info pages just fade in
            } else {
                // Content pages (including songs): direction-aware
                enterClass = direction === 'forward' ? 'page-enter' : 'page-enter-back';
            }

            // Add enter animation
            if (content && enterClass) {
                content.classList.add(enterClass);
                setTimeout(() => content.classList.remove(enterClass), 350);
            }

            // Scroll to top
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }

        // Check if data is already loaded for a page
        function isDataLoadedForPage(page) {
            switch (page) {
                case 'songs': return loadedCategories.songs && allSongs.length > 0;
                case 'chatzeros': return loadedCategories.chatzeros && Object.keys(chatzerosInfo).length > 0;
                case 'mechabrim': return loadedCategories.mechabrim && Object.keys(mechabrimInfo).length > 0;
                case 'verter': return loadedCategories.verter && Object.keys(verterInfo).length > 0;
                case 'zmanim': return loadedCategories.zmanim && Object.keys(zmaninInfo).length > 0;
                case 'collections': return loadedCategories.collections && Object.keys(collectionsInfo).length > 0;
                case 'piyutim': return loadedCategories.piyutim && Object.keys(piyutimInfo).length > 0;
                case 'resources': return loadedCategories.resources && Object.keys(resourcesInfo).length > 0;
                case 'documents': return loadedCategories.documents && Object.keys(documentsInfo).length > 0;
                case 'albums': return loadedCategories.albums && Object.keys(albumsInfo).length > 0;
                case 'search':
                    // Search needs all data loaded
                    return loadedCategories.songs && allSongs.length > 0 &&
                        loadedCategories.chatzeros && Object.keys(chatzerosInfo).length > 0 &&
                        loadedCategories.mechabrim && Object.keys(mechabrimInfo).length > 0 &&
                        loadedCategories.verter && Object.keys(verterInfo).length > 0 &&
                        loadedCategories.zmanim && Object.keys(zmaninInfo).length > 0 &&
                        loadedCategories.collections && Object.keys(collectionsInfo).length > 0 &&
                        loadedCategories.albums && Object.keys(albumsInfo).length > 0;
                default: return true;
            }
        }

        // Scroll to section on home page
        function scrollToSection(sectionId) {
            hideAllDropdowns();
            navigateTo('home');
            setTimeout(() => {
                const section = document.getElementById(sectionId);
                if (section) {
                    section.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }
            }, 150);
        }

        // Open add modal (placeholder for future)
        function openAddModal() {
            hideAllDropdowns();
            alert('די אפציע צו צולייגן א ניגון קומט באלד!');
        }

        // Hide all dropdowns
        function hideAllDropdowns() {
            document.querySelectorAll('.nav-dropdown-content').forEach(d => {
                d.classList.add('hiding');
            });
            setTimeout(() => {
                document.querySelectorAll('.nav-dropdown-content').forEach(d => {
                    d.classList.remove('hiding');
                });
            }, 300);
        }

        // Close dropdowns when clicking on items
        document.addEventListener('click', function (e) {
            if (e.target.closest('.nav-dropdown-item')) {
                hideAllDropdowns();
            }
        });

        // Render current page
        function renderCurrentPage() {
            // Check if we're in a detail view
            if (currentDetailView) {
                renderDetailPage();
                return;
            }

            const content = document.getElementById('mainContent');

            switch (currentPageView) {
                case 'home':
                    renderHomePage(content);
                    break;
                case 'search':
                    renderSearchResultsPage(content);
                    break;
                case 'songs':
                    renderAllSongsPage(content);
                    break;
                case 'chatzeros':
                    renderCategoryPage(content, 'chatzeros', 'חצרות', '', 'אלע חסידישע חצרות און זייערע ניגונים');
                    break;
                case 'mechabrim':
                    renderCategoryPage(content, 'mechabrim', 'מחברים', '<div class="mechaber-icon"></div>', 'די מחברים פון די ניגונים');
                    break;
                case 'verter':
                    renderCategoryPage(content, 'verter', 'ווערטער', '', 'ניגונים לויט די ווערטער');
                    break;
                case 'zmanim':
                    renderZmanimPage(content);
                    break;
                case 'music':
                    renderMusicPage(content);
                    break;
                case 'collections':
                    renderCollectionsPage(content);
                    break;
                case 'piyutim':
                    renderPiyutimPage(content);
                    break;
                case 'resources':
                    renderResourcesPage(content);
                    break;
                case 'documents':
                    renderDocumentsPage(content);
                    break;
                case 'albums':
                    renderAlbumsPage(content);
                    break;
                case 'about':
                    renderAboutPage(content);
                    break;
                case 'instructions':
                    renderInstructionsPage(content);
                    break;
                case 'stats':
                    renderStatsPage(content);
                    break;
                case 'credits':
                    renderCreditsPage(content);
                    break;
                case 'thanks':
                    renderThanksPage(content);
                    break;
                case 'terms':
                    renderTermsPage(content);
                    break;
                case 'contribute':
                    renderContributePage(content);
                    break;
            }
        }

        // Render home page
        function renderHomePage(container) {
            // Use cached stats from the stats table (fast, no counting needed)
            const totalSongs = cachedStats.niggunim || 0;
            const totalChatzeros = cachedStats.chatzeros || 0;
            const totalMechabrim = cachedStats.mechabrim || 0;
            const totalVerter = cachedStats.verter || 0;
            const totalZmanim = cachedStats.zmanim || 0;
            const totalCollections = cachedStats.collections || 0;
            const totalPiyutim = cachedStats.piyutim || 0;

            container.innerHTML = `
                <div class="home-page">
                    <!-- Hero Banner -->
                    <div class="home-hero">
                        <div class="hero-musical-bg" id="heroMusicalBg">
                            <!-- Bars will be generated by JS -->
                        </div>
                        <div class="home-hero-content">
                            <div class="home-logo"></div>
                            <h1 class="home-title">אוצר הניגונים</h1>
                            <p class="home-subtitle">א דאטאבעיס פון חסידישע ניגונים</p>
                        </div>
                    </div>

                    <!-- Search Box -->
                    <div class="home-search-section">
                        <div class="home-search-box">
                            <input type="text"
                                   id="homeSearchInput"
                                   class="home-search-input"
                                   placeholder="זוך א ניגון, מחבר, חצר..."
                                   onkeyup="handleHomeSearch(event)">
                            <button class="home-search-btn" onclick="executeHomeSearch()">זוך</button>
                        </div>
                    </div>

                    <!-- Statistics - 2 Stack Layout -->
                    <div class="home-stats-grid">
                        <a class="home-stat-hero" href="#/nigunim">
                            <div class="home-stat-number">${totalSongs}</div>
                            <div class="home-stat-label">ניגונים</div>
                            <div class="home-stat-subtitle">אינעם דאטאבעיס</div>
                        </a>
                        <div class="home-stats-right">
                            <div class="home-stats-row">
                                <a class="home-stat-card stat-themed theme-mechaber" href="#/mechabrim">
                                    <div class="home-stat-number">${totalMechabrim}</div>
                                    <div class="home-stat-label">מחברים</div>
                                </a>
                                <a class="home-stat-card stat-themed theme-chatzer" href="#/chatzeros">
                                    <div class="home-stat-number">${totalChatzeros}</div>
                                    <div class="home-stat-label">חצרות</div>
                                </a>
                            </div>
                            <div class="home-stats-row">
                                <a class="home-stat-card stat-themed theme-verter" href="#/verter">
                                    <div class="home-stat-number">${totalVerter}</div>
                                    <div class="home-stat-label">ווערטער</div>
                                </a>
                                <a class="home-stat-card stat-themed theme-zman" href="#/zmanim">
                                    <div class="home-stat-number">${totalZmanim}</div>
                                    <div class="home-stat-label">זמנים</div>
                                </a>
                                <a class="home-stat-card stat-themed theme-collection" href="#/collections">
                                    <div class="home-stat-number">${totalCollections}</div>
                                    <div class="home-stat-label">קאלעקשאנס</div>
                                </a>
                            </div>
                        </div>
                    </div>

                    <!-- Statistics Link -->
                    <a class="home-stats-link" href="#/stats">
                        <div class="home-stats-link-line left"></div>
                        <span class="home-stats-link-text">סטאטיסטיקס</span>
                        <div class="home-stats-link-line"></div>
                    </a>

                    <!-- About Us Section -->
                    <div class="home-about-section">
                        <a class="home-about-header" href="#/about">
                            <span class="home-about-title">איבער אונז</span>
                            <span class="home-about-arrow">←</span>
                        </a>
                        <div class="home-about-links">
                            <a class="home-about-link" href="#/about">אונזער ציל</a>
                            <a class="home-about-link" href="#/instructions">אנווייזונגען</a>
                            <a class="home-about-link" href="#/credits">קרעדיטס</a>
                            <a class="home-about-link" href="#/thanks">התודה והברכה</a>
                        </div>
                    </div>

                    <!-- Terms Link -->
                    <a class="home-terms-link" href="#/terms">
                        Terms of Use
                    </a>
                </div>
            `;

            // Apply staggered animations to all home page elements
            requestAnimationFrame(() => {
                const elements = container.querySelectorAll(
                    '.home-hero, .home-search-section, .home-stat-hero, .home-stat-card, .home-stats-link, .home-section-title, .home-link-card, .home-section, .home-about-section, .home-terms-link'
                );

                elements.forEach((el, i) => {
                    // Start with opacity 0 (handled by class, but ensure immediate effect)
                    // el.style.opacity = '0'; // Not strictly needed if class is added immediately, but safer?
                    // actually the class .stagger-fade-up has opacity: 0.

                    // delay
                    el.style.animationDelay = `${i * 0.08}s`;
                    el.classList.add('stagger-fade-up');

                    // Remove animation class after it's done to allow hover transforms to work cleanly
                    el.addEventListener('animationend', () => {
                        el.classList.remove('stagger-fade-up');
                        // Ensure final state is visible and clean for interactions
                        el.style.opacity = '1';
                        el.style.transform = '';
                        el.style.animationDelay = '';
                    }, { once: true });
                });

                // Animate count-up for stat numbers
                const statNumbers = container.querySelectorAll('.home-stat-number');
                statNumbers.forEach((el, i) => {
                    const finalValue = parseInt(el.textContent) || 0;
                    if (finalValue > 0) {
                        el.textContent = '0';
                        el.classList.add('number-animate');
                        el.style.animationDelay = `${i * 0.08}s`;

                        // Count up animation
                        setTimeout(() => {
                            const duration = 600;
                            const startTime = performance.now();
                            const animate = (currentTime) => {
                                const elapsed = currentTime - startTime;
                                const progress = Math.min(elapsed / duration, 1);
                                // Easing function for smooth count
                                const eased = 1 - Math.pow(1 - progress, 3);
                                el.textContent = Math.floor(eased * finalValue);
                                if (progress < 1) {
                                    requestAnimationFrame(animate);
                                } else {
                                    el.textContent = finalValue;
                                }
                            };
                            requestAnimationFrame(animate);
                        }, i * 80 + 200);
                    }
                });
            });

            // Generate floating musical notes (Starfield / Warp Effect)
            const musicContainer = document.getElementById('heroMusicalBg');
            if (musicContainer) {
                // User requested: "Slower", "More", "Bigger pieces", "More in middle"
                const noteCount = 150; // Increased count significantly to fill the middle
                const symbols = ['♪', '♫', '♩', '♬', '♭', '♮', '♯'];
                let html = '';

                // Radius must be large enough to reach corners of a wide screen
                const maxRadius = 1500;

                for (let i = 0; i < noteCount; i++) {
                    const symbol = symbols[Math.floor(Math.random() * symbols.length)];

                    // Random angle
                    const angle = Math.random() * Math.PI * 2;

                    // Calculate target X and Y based on angle and radius
                    const tx = Math.cos(angle) * maxRadius;
                    const ty = Math.sin(angle) * maxRadius;

                    // Random rotation for the note itself
                    const rot = (Math.random() * 360 - 180) + 'deg';

                    // Slower duration: 25s to 45s (User requested slower)
                    const duration = 25 + Math.random() * 20;
                    // Spread delay across the entire duration so they are strictly uniform
                    const delay = Math.random() * duration;

                    // Bigger sizes: 50px to 90px
                    const size = 50 + Math.random() * 40;

                    // Decide on a specific wander animation pattern (1, 2, or 3)
                    const wanderClass = 'wander-' + (Math.floor(Math.random() * 3) + 1);

                    // Note: animation-duration applies to the FLIGHT (wrapper).
                    // We can let the inner wander animation inherit it or have its own speed. 
                    // To keep them synced (one full drift per flight), we'll use the same duration variable.

                    html += `
                        <div class="note-wrapper" style="--tx: ${tx}px; --ty: ${ty}px; animation-duration: ${duration}s; animation-delay: -${delay}s;">
                            <span class="musical-note ${wanderClass}" style="--rot: ${rot}; font-size: ${size}px; animation-duration: ${duration}s;">${symbol}</span>
                        </div>
                    `;
                }
                musicContainer.innerHTML = html;
            }
        }

        // Helper function to generate info pages sub-navigation
        function getInfoNavHTML(activePage) {
            const pages = [
                { id: 'about', label: 'אונזער ציל' },
                { id: 'instructions', label: 'אנווייזונגען' },
                { id: 'contribute', label: 'לייג צו', featured: true },
                { id: 'credits', label: 'קרעדיטס' },
                { id: 'thanks', label: 'התודה והברכה' }
            ];

            return `
                <nav class="info-nav">
                    ${pages.map((page, index) => `
                        <a class="info-nav-item ${activePage === page.id ? 'active' : ''} ${page.featured ? 'info-nav-featured' : ''}"
                             href="#/${page.id}">
                            ${page.label}
                        </a>
                    `).join('')}
                </nav>
            `;
        }

        // Render About Page (אונזער ציל)
        function renderAboutPage(container) {
            container.innerHTML = `
                <div class="info-page">
                    ${getInfoNavHTML('about')}

                    <div class="info-page-header">
                        <h1 class="info-page-title">אונזער ציל</h1>
                        <p class="info-page-subtitle">די מעגליכקייטן און בענעפיטן פונעם דאטאבעיס</p>
                    </div>

                    <div class="info-page-content">
                        <div class="goals-bricks">
                            <div class="goals-brick-row">
                                <div class="goal-brick">
                                    <div class="goal-brick-text"><strong>אלע חסידישע ניגונים</strong> וואס ווערן געזינגען בחצרות הקודש <strong>צאמגענומען אויף איין פלאץ</strong></div>
                                </div>
                            </div>

                            <div class="goals-brick-row">
                                <div class="goal-brick">
                                    <div class="goal-brick-text"><strong>א סעירטש קנעפל</strong><br>צו טרעפן סיי וועלכן ניגון</div>
                                </div>
                                <div class="goal-brick">
                                    <div class="goal-brick-text"><strong>דעטאלירטע דאקומענטאציע</strong><br>פון מקורות פון אלטע ניגונים</div>
                                </div>
                                <div class="goal-brick">
                                    <div class="goal-brick-text"><strong>היסטארישע רעקארדירונגען</strong><br>מיט די ארגינעלע קנייטשן</div>
                                </div>
                            </div>

                            <div class="goals-brick-row">
                                <div class="goal-brick">
                                    <div class="goal-brick-text"><strong>געשריבענע נאטן</strong><br>אויף ניגונים וואס פארמאגן נישט קיין ווערטער</div>
                                </div>
                                <div class="goal-brick">
                                    <div class="goal-brick-text"><strong>א רשימה מיט גוטע סימנים</strong><br>אויף ניגונים וואס פארמאגן נישט קיין ווערטער</div>
                                </div>
                            </div>

                            <div class="goals-brick-row">
                                <div class="goal-brick">
                                    <div class="goal-brick-text"><strong>העכסט פראפעסינאלע רעקארדירונגען</strong><br>פון אלע חסידישע ניגונים</div>
                                </div>
                                <div class="goal-brick">
                                    <div class="goal-brick-text">חסידישע ניגונים ערשטמאליג אויסגעשטעלט<br><strong>לויט די ריטעם און סקעיל</strong></div>
                                </div>
                                <div class="goal-brick">
                                    <div class="goal-brick-text"><strong>קאלעקשנס פון ניגונים</strong><br>וואס זענען פאסיג צו ווערן געזינגען ביי סיי וועלכן מאורע</div>
                                </div>
                            </div>

                            <div class="goals-brick-row">
                                <div class="goal-brick">
                                    <div class="goal-brick-text">פאסיגע געדאנקען פון ניגונים וואס קענען ווערן געזינגען<br><strong>אויף פיוטים ביים דאווענען</strong></div>
                                </div>
                                <div class="goal-brick">
                                    <div class="goal-brick-text">פאסיגע געדאנקען פון ניגונים וואס קענען ווערן געזינגען<br><strong>אויף זמירות ביי סעודות שבת ויו״ט</strong></div>
                                </div>
                            </div>
                        </div>

                        <div class="goals-warning">
                            <div class="goals-warning-title">אכטונג!</div>
                            <div class="goals-warning-text"><strong>דער דאטאבעיס איז געמאכט אויסדרוקלעך נאר פאר חסידישע ניגונים וואס ווערן געזינגען בחצרות הקודש</strong></div>
                        </div>

                        <div class="goals-contact">
                            <div class="goals-contact-title">הערות והארות</div>
                            <div class="goals-contact-text">פאר סיי וועלכע הערות אדער זיך צו פארבינדן מיט מערכת אידישע ניגונים</div>
                            <a href="mailto:yiddishenigunim@gmail.com" class="goals-contact-email">yiddishenigunim@gmail.com</a>
                        </div>
                    </div>
                </div>
            `;
        }

        // Render Instructions Page (אנווייזונגען)
        function renderInstructionsPage(container) {
            container.innerHTML = `
                <div class="info-page">
                    ${getInfoNavHTML('instructions')}

                    <div class="info-page-header">
                        <h1 class="info-page-title">אנווייזונגען</h1>
                        <p class="info-page-subtitle">ווי אזוי צו נוצן דעם אוצר הניגונים</p>
                    </div>

                    <div class="info-page-content">
                        <div class="instructions-page-grid">
                            <div class="instruction-page-item">
                                <div class="instruction-page-number">1</div>
                                <div class="instruction-page-text">
                                    <strong>...</strong>
                                    ...
                                </div>
                            </div>
                            <div class="instruction-page-item">
                                <div class="instruction-page-number">2</div>
                                <div class="instruction-page-text">
                                    <strong>...</strong>
                                    ...
                                </div>
                            </div>
                            <div class="instruction-page-item">
                                <div class="instruction-page-number">3</div>
                                <div class="instruction-page-text">
                                    <strong>...</strong>
                                    ...
                                </div>
                            </div>
                            <div class="instruction-page-item">
                                <div class="instruction-page-number">4</div>
                                <div class="instruction-page-text">
                                    <strong>...</strong>
                                    ...
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }

        // Render Stats Page (סטאטיסטיקס)
        function renderStatsPage(container) {
            const totalSongs = cachedStats.niggunim || 0;
            const totalChatzeros = cachedStats.chatzeros || 0;
            const totalMechabrim = cachedStats.mechabrim || 0;
            const totalVerter = cachedStats.verter || 0;
            const totalZmanim = cachedStats.zmanim || 0;
            const totalCollections = cachedStats.collections || 0;

            container.innerHTML = `
                <div class="info-page">
                    ${getInfoNavHTML('stats')}

                    <div class="info-page-header">
                        <h1 class="info-page-title">סטאטיסטיקס</h1>
                        <p class="info-page-subtitle">די צאלן פון אונזער זאמלונג</p>
                    </div>

                    <div class="info-page-content">
                        <div class="stats-page-grid">
                            <a class="stats-page-card" href="#/nigunim">
                                <div class="stats-page-number">${totalSongs}</div>
                                <div class="stats-page-label">ניגונים</div>
                            </a>
                            <a class="stats-page-card" href="#/chatzeros">
                                <div class="stats-page-number">${totalChatzeros}</div>
                                <div class="stats-page-label">חצרות</div>
                            </a>
                            <a class="stats-page-card" href="#/mechabrim">
                                <div class="stats-page-number">${totalMechabrim}</div>
                                <div class="stats-page-label">מחברים</div>
                            </a>
                            <a class="stats-page-card" href="#/verter">
                                <div class="stats-page-number">${totalVerter}</div>
                                <div class="stats-page-label">ווערטער</div>
                            </a>
                            <a class="stats-page-card" href="#/zmanim">
                                <div class="stats-page-number">${totalZmanim}</div>
                                <div class="stats-page-label">זמנים</div>
                            </a>
                            <a class="stats-page-card" href="#/collections">
                                <div class="stats-page-number">${totalCollections}</div>
                                <div class="stats-page-label">קאלעקשאנס</div>
                            </a>
                        </div>
                    </div>
                </div>
                </div>
            `;

            // Animate numbers
            const statNumbers = container.querySelectorAll('.stats-page-number');
            statNumbers.forEach((el, index) => {
                const value = parseInt(el.textContent.replace(/,/g, ''));
                if (!isNaN(value)) {
                    // Reset to 0 so animation starts from scratch
                    el.textContent = '0';
                    // Stagger animation slightly
                    setTimeout(() => {
                        animateCountUp(el, value);
                    }, index * 100);
                }
            });
        }

        // Render Credits Page (קרעדיטס)
        function renderCreditsPage(container) {
            container.innerHTML = `
                <div class="info-page">
                    ${getInfoNavHTML('credits')}

                    <div class="info-page-header">
                        <h1 class="info-page-title">קרעדיטס</h1>
                        <p class="info-page-subtitle">די מענטשן הינטער דעם פראיעקט</p>
                    </div>

                    <div class="info-page-content">
                        <p>דער פראיעקט איז געבויט געווארן דורך פרייוויליגע וואס האבן א ליבשאפט צו חסידישע ניגונים.</p>

                        <div class="credits-page-list">
                            <div class="credit-page-item">
                                <strong>דאטאבאנק:</strong> געזאמלט און ארגאניזירט מיט גרויס מי
                            </div>
                            <div class="credit-page-item">
                                <strong>טעכנאלאגיע:</strong> געבויט מיט מאדערנע וועב טעכנאלאגיעס
                            </div>
                            <div class="credit-page-item">
                                <strong>אוידיא:</strong> רעקארדירונגען פון פארשידענע מקורות
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }

        // Render Thanks Page (התודה והברכה)
        function renderThanksPage(container) {
            container.innerHTML = `
                <div class="info-page">
                    ${getInfoNavHTML('thanks')}

                    <div class="info-page-header">
                        <h1 class="info-page-title">התודה והברכה</h1>
                        <p class="info-page-subtitle">א יישר כח צו אלע וואס האבן ביישטייערט</p>
                    </div>

                    <div class="info-page-content">
                        <p>א גרויסן יישר כח צו אלע וואס האבן ביישטייערט צו דעם פראיעקט:</p>

                        <ul class="thanks-page-list">
                            <li>די וואס האבן געטיילט ניגונים און רעקארדירונגען</li>
                            <li>די וואס האבן געהאלפן מיט אינפארמאציע וועגן די ניגונים</li>
                            <li>די וואס האבן געגעבן פידבעק און פארשלאגן</li>
                            <li>אלע וואס נוצן און פארשפרייטן דעם אוצר</li>
                        </ul>

                        <div class="blessing-text">
                            יהי רצון שנזכה להמשיך לשמוע ולשיר ניגונים עד ביאת גואל צדק במהרה בימינו אמן.
                        </div>
                    </div>
                </div>
            `;
        }

        // Render Terms Page (Terms of Use)
        function renderTermsPage(container) {
            container.innerHTML = `
                <div class="info-page">
                    ${getInfoNavHTML('terms')}

                    <div class="info-page-header">
                        <h1 class="info-page-title">Terms of Use</h1>
                        <p class="info-page-subtitle">באדינגונגען פאר נוצן די וועבזייטל</p>
                    </div>

                    <div class="info-page-content terms-page-content">
                        <p class="terms-last-updated">Last Updated: 12/1/2025</p>

                        <p class="terms-intro">Welcome to OITZERHANIGUNIM.ORG. By accessing or using this website, you agree to the following Terms of Use. Please read them carefully. If you do not agree with any part of these Terms, you may not use the site.</p>

                        <hr class="terms-divider">

                        <div class="terms-section">
                            <h2 class="terms-section-header">1. Overview</h2>
                            <p>OITZERHANIGUNIM.ORG provides a song database where users may view information and optionally upload audio recordings. All audio content is uploaded directly by users.</p>
                            <p>OITZERHANIGUNIM.ORG does not host, create, or verify the content uploaded by users.</p>
                        </div>

                        <hr class="terms-divider">

                        <div class="terms-section">
                            <h2 class="terms-section-header">2. Eligibility</h2>
                            <p>To upload files or submit content, you must log in through an approved authentication method (such as Google Login).</p>
                            <p>You are responsible for maintaining the security of your login account.</p>
                        </div>

                        <hr class="terms-divider">

                        <div class="terms-section">
                            <h2 class="terms-section-header">3. User Responsibilities</h2>
                            <p>By using this site, you agree that you will:</p>
                            <ul>
                                <li>Provide accurate information</li>
                                <li>Use the site only for lawful purposes</li>
                                <li>Not harm, interfere with, or attempt to bypass any site security</li>
                                <li>Not upload harmful, malicious, or corrupted files</li>
                                <li>Follow all applicable copyright and intellectual-property laws</li>
                            </ul>
                            <p>Any misuse of the site may result in suspension or blocking of access.</p>
                        </div>

                        <hr class="terms-divider">

                        <div class="terms-section">
                            <h2 class="terms-section-header">4. User-Uploaded Content</h2>
                            <p>When you upload audio files or other materials, you agree that:</p>
                            <ol>
                                <li>You are the owner of the recording or have full permission to upload it</li>
                                <li>The content does not violate anyone's copyright or rights</li>
                                <li>You grant OITZERHANIGUNIM.ORG permission to display and store the uploaded file within the platform</li>
                                <li>You accept full responsibility for any issues or claims arising from your upload</li>
                                <li>You will not upload illegal, harmful, infringing, or abusive material</li>
                            </ol>
                            <p>OITZERHANIGUNIM.ORG is not responsible for user-uploaded content and does not guarantee legality, accuracy, or rights of any material provided by users.</p>
                        </div>

                        <hr class="terms-divider">

                        <div class="terms-section">
                            <h2 class="terms-section-header">5. Content Removal</h2>
                            <p>We reserve the right to:</p>
                            <ul>
                                <li>Remove or disable access to any content</li>
                                <li>Block or suspend users who violate these Terms</li>
                                <li>Remove content upon receiving a proper complaint</li>
                            </ul>
                            <p>These actions may be taken with or without notice.</p>
                        </div>

                        <hr class="terms-divider">

                        <div class="terms-section">
                            <h2 class="terms-section-header">6. Copyright Complaints & Takedown Policy</h2>
                            <p>If you believe your copyrighted material has been uploaded without permission, please contact us at:</p>
                            <p><span class="terms-email">yiddishenigunim@gmail.com</span></p>
                            <p>Include:</p>
                            <ul>
                                <li>The URL or link to the infringing content</li>
                                <li>A description of your work</li>
                                <li>Your contact information</li>
                                <li>A statement confirming that you believe the use is unauthorized</li>
                            </ul>
                            <p>We will remove the content promptly.</p>
                        </div>

                        <hr class="terms-divider">

                        <div class="terms-section">
                            <h2 class="terms-section-header">7. Site Availability & Updates</h2>
                            <p>The site is provided on an "as-is" and "as-available" basis.</p>
                            <p>We may update, modify, or discontinue any part of the site at any time.</p>
                        </div>

                        <hr class="terms-divider">

                        <div class="terms-section">
                            <h2 class="terms-section-header">8. No Warranty</h2>
                            <p>We do not guarantee:</p>
                            <ul>
                                <li>The accuracy or completeness of the content</li>
                                <li>That the site will always be available or error-free</li>
                                <li>That user uploads are legally permitted</li>
                            </ul>
                            <p>All use is at your own risk.</p>
                        </div>

                        <hr class="terms-divider">

                        <div class="terms-section">
                            <h2 class="terms-section-header">9. Limitation of Liability</h2>
                            <p>To the fullest extent permitted by law:</p>
                            <p>OITZERHANIGUNIM.ORG is not liable for any damages, losses, claims, or issues arising from:</p>
                            <ul>
                                <li>User-uploaded content</li>
                                <li>Copyright disputes</li>
                                <li>Technical problems or downtime</li>
                                <li>Data loss</li>
                                <li>Misuse of the site by others</li>
                            </ul>
                            <p>You agree to hold OITZERHANIGUNIM.ORG, its administrators, and affiliates harmless from any such claims.</p>
                        </div>

                        <hr class="terms-divider">

                        <div class="terms-section">
                            <h2 class="terms-section-header">10. Changes to These Terms</h2>
                            <p>We may update or revise these Terms from time to time.</p>
                            <p>The updated version will take effect when posted on the site.</p>
                            <p>Your continued use of the site means you accept the revised Terms.</p>
                        </div>

                        <hr class="terms-divider">

                        <div class="terms-section">
                            <h2 class="terms-section-header">11. Contact</h2>
                            <p>For questions or concerns, please contact:</p>
                            <p><span class="terms-email">yiddishenigunim@gmail.com</span></p>
                        </div>
                    </div>
                </div>
            `;
        }

        // Render Contribute Page (צולייגן אינפֿאָרמאַציע)
        function renderContributePage(container) {
            container.innerHTML = `
                <div class="info-page contribute-page">
                    ${getInfoNavHTML('contribute')}

                    <div class="contribute-hero">
                        <div class="contribute-hero-content">
                            <h1 class="contribute-hero-title">ביישטייערט צום אוצר</h1>
                            <p class="contribute-hero-subtitle">העלפט אונז פארגרעסערן און פארבעסערן דעם דאטאבעיס</p>
                        </div>
                    </div>

                    <div class="info-page-content contribute-content">
                        <!-- Main Nigun Button -->
                        <a href="https://coda.io/form/_deegJpdfVC5" class="contribute-main-btn contribute-btn themed theme-nigun" target="_blank" rel="noopener">
                            <span class="contribute-main-text">לייג צו / אפדעיט א <strong>ניגון</strong></span>
                            <span class="btn-external">↗</span>
                        </a>

                        <!-- Category Buttons Grid -->
                        <div class="contribute-grid">
                            <a href="https://coda.io/form/_dpAUcVRsKjH" class="contribute-btn themed theme-mechaber" target="_blank" rel="noopener">
                                <span class="contribute-btn-text">לייג צו / אפדעיט א <strong>מחבר</strong></span>
                                <span class="btn-external">↗</span>
                            </a>

                            <a href="https://coda.io/form/_dctTYOyS0RX" class="contribute-btn themed theme-chatzer" target="_blank" rel="noopener">
                                <span class="contribute-btn-text">לייג צו / אפדעיט א <strong>חצר</strong></span>
                                <span class="btn-external">↗</span>
                            </a>

                            <a href="https://coda.io/form/_dgAvntb_zik" class="contribute-btn themed theme-verter" target="_blank" rel="noopener">
                                <span class="contribute-btn-text">לייג צו / אפדעיט <strong>ווערטער</strong></span>
                                <span class="btn-external">↗</span>
                            </a>

                            <a href="https://coda.io/form/_d5bHKMFR3lD" class="contribute-btn themed theme-zman" target="_blank" rel="noopener">
                                <span class="contribute-btn-text">לייג צו / אפדעיט א <strong>זמן</strong></span>
                                <span class="btn-external">↗</span>
                            </a>

                            <a href="https://coda.io/form/_dZD54k66986" class="contribute-btn themed theme-piyut" target="_blank" rel="noopener">
                                <span class="contribute-btn-text">לייג צו / אפדעיט א <strong>פיוט</strong></span>
                                <span class="btn-external">↗</span>
                            </a>

                            <a href="https://coda.io/form/_d6Apy3XTmnZ" class="contribute-btn themed theme-collection" target="_blank" rel="noopener">
                                <span class="contribute-btn-text">לייג צו / אפדעיט א <strong>קאלעקשן</strong></span>
                                <span class="btn-external">↗</span>
                            </a>
                        </div>

                        <!-- Extra Resources Section -->
                        <div class="contribute-extra-section">
                            <h3 class="contribute-section-title">לייג צו נאך רעסורן</h3>
                            <div class="contribute-extra-grid">
                                <a href="https://coda.io/form/_dMIXJIsbjLg" class="contribute-btn themed theme-resource" target="_blank" rel="noopener">
                                    <span class="contribute-btn-text">לייג צו א <strong>רעסורס</strong></span>
                                    <span class="btn-external">↗</span>
                                </a>

                                <a href="https://coda.io/form/_d9KgrPJTikk" class="contribute-btn themed theme-document" target="_blank" rel="noopener">
                                    <span class="contribute-btn-text">לייג צו א <strong>דאקומענט</strong></span>
                                    <span class="btn-external">↗</span>
                                </a>

                                <a href="https://coda.io/form/_dVpYtnuj24f" class="contribute-btn themed theme-album" target="_blank" rel="noopener">
                                    <span class="contribute-btn-text">לייג צו אן <strong>אלבום</strong></span>
                                    <span class="btn-external">↗</span>
                                </a>
                            </div>
                        </div>
                    </div>

                    <!-- Contact Section (outside white container) -->
                    <div class="goals-contact">
                        <div class="goals-contact-title">הערות והארות</div>
                        <div class="goals-contact-text">פאר סיי וועלכע הערות אדער זיך צו פארבינדן מיט מערכת אידישע ניגונים</div>
                        <a href="mailto:yiddishenigunim@gmail.com" class="goals-contact-email">yiddishenigunim@gmail.com</a>
                    </div>

                    <!-- Terms Link -->
                    <div class="contribute-terms">
                        <a href="#/terms" class="contribute-terms-link">Terms of Use</a>
                    </div>
                </div>
            `;
        }

        // Handle home search
        function handleHomeSearch(event) {
            if (event.key === 'Enter') {
                executeHomeSearch();
            }
        }

        function executeHomeSearch() {
            const query = document.getElementById('homeSearchInput').value.trim();
            if (query) {
                searchQuery = query;
                filterSongs();
                navigateTo('nigunim');
                // Set the search input in songs page
                setTimeout(() => {
                    const searchInput = document.getElementById('searchInput');
                    if (searchInput) {
                        searchInput.value = query;
                    }
                }, 100);
            }
        }

        // Render all songs page
        function renderAllSongsPage(container) {
            // NOTE: Always build full page structure so songsGrid exists
            // If no songs yet, grid will show skeleton rows inside it

            // Reset lazy loading state
            songsLazyState = {
                currentIndex: 0,
                batchSize: 50,
                isLoading: false,
                isInitialLoad: true
            };

            container.innerHTML = `
                <div class="page-theme theme-nigun">
                    <div class="page-title">
                        <div class="page-title-bar">ניגונים</div>
                        <div class="page-title-content">
                            <div class="subtitle ${!songsFullyLoaded ? 'loading-state' : ''}">${!songsFullyLoaded ? getTinyLoaderHTML() + ' ' : ''}<span class="count-number">${filteredSongs.length}</span> ניגונים</div>
                            <div class="loading-wave-bar" id="loadingWaveBar"></div>
                        </div>
                    </div>

                    <div class="songs-page-layout">
                        <div class="songs-main-content">
                            <div class="songs-card">
                                <div class="all-songs-grid" id="songsGrid">
                                    ${filteredSongs.length === 0 && !songsFullyLoaded ? `
                                        <div class="skeleton-song-row"></div>
                                        <div class="skeleton-song-row"></div>
                                        <div class="skeleton-song-row"></div>
                                        <div class="skeleton-song-row"></div>
                                        <div class="skeleton-song-row"></div>
                                        <div class="skeleton-song-row"></div>
                                        <div class="skeleton-song-row"></div>
                                        <div class="skeleton-song-row"></div>
                                    ` : ''}
                                </div>
                                <div id="songsLoadMoreIndicator" style="height: 1px;"></div>
                            </div>
                        </div>

                        <div class="sidebar-wrapper">
                        <div class="songs-sidebar">
                            <div class="sidebar-search-box">
                                <input type="text"
                                       id="songsSearchInput"
                                       class="sidebar-search-input"
                                       placeholder="זוך א ניגון..."
                                       value="${searchQuery || ''}"
                                       onkeypress="if(event.key==='Enter')handleSongsSearch(this.value)">
                                ${searchQuery ? '<button class="sidebar-search-clear" onclick="clearSearch()">×</button>' : ''}
                                <button class="sidebar-search-btn" onclick="handleSongsSearch(document.getElementById('songsSearchInput').value)">🔍</button>
                            </div>

                            <div class="sidebar-sort-section">
                                <label class="sidebar-sort-label" style="color: var(--color-nigun-dark);">סארטיר:</label>
                                <div class="sort-toggle-container" id="sortToggle">
                                    <div class="sort-toggle-slider"></div>
                                    <div class="sort-toggle-option ${currentSortOrder === 'random' ? 'active' : ''}" data-sort="random" onclick="changeSortOrder('random')">שאָפל</div>
                                    <div class="sort-toggle-option ${currentSortOrder === 'alphabetical' ? 'active' : ''}" data-sort="alphabetical" onclick="changeSortOrder('alphabetical')">א"ב</div>
                                    <div class="sort-toggle-option ${currentSortOrder === 'recent' ? 'active' : ''}" data-sort="recent" onclick="changeSortOrder('recent')">לעצט צוגעלייגט</div>
                                </div>
                            </div>

                            <div class="sidebar-sort-section">
                                <label class="sidebar-sort-label" style="color: var(--color-nigun-dark);">ווייז:</label>
                                <div class="sort-toggle-container" id="displayToggle">
                                    <div class="sort-toggle-slider"></div>
                                    <div class="sort-toggle-option ${currentDisplayMode === 'minimal' ? 'active' : ''}" data-mode="minimal" onclick="changeDisplayMode('minimal')">קעפל</div>
                                    <div class="sort-toggle-option ${currentDisplayMode === 'recordings' ? 'active' : ''}" data-mode="recordings" onclick="changeDisplayMode('recordings')">רעקארדירונגען</div>
                                    <div class="sort-toggle-option ${currentDisplayMode === 'full' ? 'active' : ''}" data-mode="full" onclick="changeDisplayMode('full')">אלעס</div>
                                </div>
                            </div>

                            <div class="sidebar-sort-section">
                                <label class="sidebar-sort-label" style="color: var(--color-nigun-dark);">אודיאו קוואליטעט:</label>
                                <div class="quality-stars-filter" id="qualityStarsFilter">
                                    <span class="quality-star ${minQualityRating >= 1 ? 'active' : ''}" 
                                          data-rating="1" 
                                          onclick="setMinQualityRating(1)"
                                          data-tooltip="א מענטש זינגט פאר זיך">★</span>
                                    <span class="quality-star ${minQualityRating >= 2 ? 'active' : ''}" 
                                          data-rating="2" 
                                          onclick="setMinQualityRating(2)"
                                          data-tooltip="אראפגעזינגען אן מוזיק">★</span>
                                    <span class="quality-star ${minQualityRating >= 3 ? 'active' : ''}" 
                                          data-rating="3" 
                                          onclick="setMinQualityRating(3)"
                                          data-tooltip="אלטע מוזיק">★</span>
                                    <span class="quality-star ${minQualityRating >= 4 ? 'active' : ''}" 
                                          data-rating="4" 
                                          onclick="setMinQualityRating(4)"
                                          data-tooltip="שיין און ניי">★</span>
                                    <span class="quality-star ${minQualityRating >= 5 ? 'active' : ''}" 
                                          data-rating="5" 
                                          onclick="setMinQualityRating(5)"
                                          data-tooltip="שיין, ניי און געשמאק">★</span>
                                    <span class="quality-clear ${minQualityRating === 0 ? 'hidden' : ''}" 
                                          onclick="setMinQualityRating(0)" 
                                          data-tooltip="באהאלט">×</span>
                                </div>
                            </div>

                            <div class="sidebar-section">
                                <div class="sidebar-section-title">פילטערס:</div>
                            </div>

                            ${renderSidebarFilter('collection', 'קאלעקשאנס:', categories.collections)}
                            ${renderSidebarFilter('chatzer', 'חצר:', categories.chatzeros)}
                            ${renderSidebarFilter('mechaber', 'מחבר:', categories.mechabrim)}
                            ${renderSidebarFilter('verter', 'ווערטער:', categories.verter)}
                            ${renderSidebarFilter('gezungen', 'געזינגען אויף:', categories.gezungen)}
                            ${renderSidebarFilter('ritem', 'ריטעם:', categories.ritem)}
                            ${renderSidebarFilter('scale', 'סקעיל:', categories.scale)}

                            <div class="active-filters" id="activeFilterTags"></div>
                        </div>
                        </div><!-- sidebar-wrapper -->
                    </div>
                </div>
            `;

            // Show loading wave bar (will be hidden when all songs are loaded)
            const waveBar = document.getElementById('loadingWaveBar');
            if (waveBar) {
                // Show wave bar if songs are still loading from API
                if (!songsFullyLoaded) {
                    waveBar.classList.add('active');
                }
            }

            // Load first batch of songs
            loadMoreSongs();

            // Setup infinite scroll observer
            const observer = new IntersectionObserver((entries) => {
                console.log(`IntersectionObserver: isIntersecting=${entries[0].isIntersecting}, songsFullyLoaded=${songsFullyLoaded}`);
                // Only load more if songs are fully loaded from API and user is scrolling
                if (entries[0].isIntersecting && songsFullyLoaded) {
                    loadMoreSongs(true); // true = animate new items
                }
            }, { rootMargin: '200px' });

            const indicator = document.getElementById('songsLoadMoreIndicator');
            if (indicator) observer.observe(indicator);

            renderActiveFilterTags();

            // Initialize sort toggle slider position
            initSortToggleSlider();
        }

        // Load more songs (for infinite scroll)
        // animate parameter controls if new items should pop in (only for user scroll)
        function loadMoreSongs(animate = false) {
            console.log(`loadMoreSongs called: isFlipping=${isFlipping}, isLoading=${songsLazyState.isLoading}, currentIndex=${songsLazyState.currentIndex}, filteredLength=${filteredSongs.length}`);
            if (isFlipping) return; // Don't interfere with FLIP animation
            if (songsLazyState.isLoading) return;
            if (songsLazyState.currentIndex >= filteredSongs.length) return;

            songsLazyState.isLoading = true;
            const grid = document.getElementById('songsGrid');
            if (!grid) {
                songsLazyState.isLoading = false;
                return;
            }

            // Clear skeleton rows if this is the first real data
            if (songsLazyState.isInitialLoad) {
                const skeletonRows = grid.querySelectorAll('.skeleton-song-row');
                skeletonRows.forEach(row => row.remove());
            }

            // Count AFTER skeleton removal
            const existingCount = grid.children.length;

            const endIndex = Math.min(
                songsLazyState.currentIndex + songsLazyState.batchSize,
                filteredSongs.length
            );

            const batch = filteredSongs.slice(songsLazyState.currentIndex, endIndex);
            const playlistIndices = filteredSongs.map(song => allSongs.indexOf(song));

            const html = batch.map((song, idx) => {
                const globalIdx = allSongs.indexOf(song);
                const displayNum = songsLazyState.currentIndex + idx + 1;
                return renderSongItem(song, globalIdx, displayNum, playlistIndices);
            }).join('');

            grid.insertAdjacentHTML('beforeend', html);
            songsLazyState.currentIndex = endIndex;
            songsLazyState.isLoading = false;

            // Animation for scroll-loaded items (slide up one after another)
            if (animate && existingCount > 0) {
                const newItems = Array.from(grid.children).slice(existingCount);
                newItems.forEach((item, i) => {
                    // Start below and invisible
                    item.style.opacity = '0';
                    item.style.transform = 'translateY(25px)';
                    // Slide up one after another
                    setTimeout(() => {
                        item.style.transition = 'opacity 0.25s ease-out, transform 0.3s ease-out';
                        item.style.opacity = '1';
                        item.style.transform = 'translateY(0)';
                    }, i * 35);
                });
            }

            // Animation for first batch - same pop-in style as FLIP new elements
            if (songsLazyState.isInitialLoad && existingCount === 0) {
                const newItems = Array.from(grid.children);
                newItems.forEach((item, i) => {
                    // Start hidden with scale - same as FLIP
                    item.style.opacity = '0';
                    item.style.transform = 'scale(0.9)';
                    // Pop in with elastic effect - same as FLIP
                    setTimeout(() => {
                        item.style.transition = 'opacity 0.3s ease-out, transform 0.35s cubic-bezier(0.34, 1.56, 0.64, 1)';
                        item.style.opacity = '1';
                        item.style.transform = 'scale(1)';
                    }, 50 + i * 25); // Faster stagger for first batch
                });
            }

            // Mark initial load as complete after first batch
            if (songsLazyState.isInitialLoad) {
                songsLazyState.isInitialLoad = false;
            }

            console.log(`Loaded ${batch.length} songs (${songsLazyState.currentIndex}/${filteredSongs.length})`);
        }

        // Render sidebar filter dropdown
        function renderSidebarFilter(key, label, data) {
            if (!data) data = {};
            const options = Object.keys(data).sort((a, b) => a.localeCompare(b, 'he'));
            const selectedCount = activeFilters[key]?.length || 0;
            const selectedValue = selectedCount > 0 ? `(${selectedCount})` : '(אלע)';

            return `
                <div class="sidebar-filter" data-filter="${key}">
                    <div class="sidebar-filter-dropdown">
                        <button class="sidebar-filter-btn ${selectedCount > 0 ? 'active' : ''}" onclick="toggleSidebarDropdown('${key}')">
                            <span class="sidebar-filter-label">${label}</span>
                            <span class="sidebar-filter-value">${selectedValue}</span>
                            <span class="dropdown-arrow">▼</span>
                        </button>
                        <div class="sidebar-dropdown-content" id="sidebar-dropdown-${key}">
                            <div class="dropdown-search-box">
                                <input type="text" 
                                       class="dropdown-search-input" 
                                       placeholder="זוך..."
                                       onclick="event.stopPropagation()"
                                       oninput="filterDropdownOptions('${key}', this.value)">
                            </div>
                            <div class="dropdown-options" id="dropdown-options-${key}">
                                ${options.map(opt => {
                const escapedOpt = opt.replace(/'/g, "\\'").replace(/"/g, '\\"');
                return `
                                    <label class="sidebar-filter-option" data-value="${opt.toLowerCase()}" onclick="event.stopPropagation()">
                                        <input type="checkbox" 
                                            value="${opt.replace(/"/g, '&quot;')}" 
                                            ${activeFilters[key]?.includes(opt) ? 'checked' : ''}
                                            onchange="handleFilterChange('${key}', '${escapedOpt}', this.checked)">
                                        ${opt}
                                    </label>
                                `}).join('')}
                            </div>
                        </div>
                    </div>
                </div>
            `;
        }

        // Filter dropdown options based on search
        function filterDropdownOptions(key, searchTerm) {
            const container = document.getElementById('dropdown-options-' + key);
            const options = container.querySelectorAll('.sidebar-filter-option[data-value]');
            const term = searchTerm.toLowerCase();

            options.forEach(option => {
                const value = option.getAttribute('data-value');
                if (value.includes(term)) {
                    option.style.display = 'flex';
                } else {
                    option.style.display = 'none';
                }
            });
        }

        // Toggle sidebar dropdown
        function toggleSidebarDropdown(key) {
            const dropdown = document.getElementById('sidebar-dropdown-' + key);
            const allDropdowns = document.querySelectorAll('.sidebar-dropdown-content');
            const wasOpen = dropdown.classList.contains('show');

            allDropdowns.forEach(d => {
                d.classList.remove('show');
            });

            if (!wasOpen) {
                dropdown.classList.add('show');
            }
        }

        // Clear specific filter type
        function clearFilterType(key) {
            activeFilters[key] = [];

            // Uncheck all checkboxes in this dropdown
            const dropdown = document.getElementById('dropdown-options-' + key);
            if (dropdown) {
                dropdown.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                    cb.checked = false;
                });
            }

            applyFilters();
            renderActiveFilterTags();
        }

        // Render filter dropdown
        function renderFilterDropdown(key, label, data) {
            const options = Object.keys(data).sort((a, b) => a.localeCompare(b, 'he'));
            const selectedCount = activeFilters[key]?.length || 0;

            return `
                <div class="filter-dropdown">
                    <button class="filter-btn ${selectedCount > 0 ? 'active' : ''}" onclick="toggleFilterDropdown('${key}')">
                        ${label}
                        <span>${selectedCount > 0 ? `(${selectedCount})` : '▼'}</span>
                    </button>
                    <div class="filter-dropdown-content" id="dropdown-${key}">
                        ${options.map(opt => `
                            <label class="filter-option">
                                <input type="checkbox" 
                                    value="${opt}" 
                                    ${activeFilters[key]?.includes(opt) ? 'checked' : ''}
                                    onchange="handleFilterChange('${key}', '${opt}', this.checked)">
                                ${opt} (${data[opt].length})
                            </label>
                        `).join('')}
                    </div>
                </div>
            `;
        }

        // Toggle filter dropdown
        function toggleFilterDropdown(key) {
            const dropdown = document.getElementById(`dropdown-${key}`);
            const allDropdowns = document.querySelectorAll('.filter-dropdown-content');

            allDropdowns.forEach(d => {
                if (d.id !== `dropdown-${key}`) d.classList.remove('show');
            });

            dropdown.classList.toggle('show');
        }

        // Handle filter change (user clicks filter option)
        function handleFilterChange(key, value, checked) {
            if (checked) {
                if (!activeFilters[key].includes(value)) {
                    activeFilters[key].push(value);
                }
            } else {
                activeFilters[key] = activeFilters[key].filter(v => v !== value);
            }

            applyFilters();
        }

        // Sort filteredSongs based on current sort order
        function sortFilteredSongs() {
            switch (currentSortOrder) {
                case 'alphabetical':
                    filteredSongs.sort((a, b) => (a.name || '').localeCompare(b.name || '', 'he'));
                    break;
                case 'recent':
                    // Sort by zugeleigt (added date) - most recent first
                    // If zugeleigt is empty, put at the end
                    filteredSongs.sort((a, b) => {
                        const dateA = a.zugeleigt || '';
                        const dateB = b.zugeleigt || '';
                        // Empty dates go to the end
                        if (!dateA && !dateB) return (a.name || '').localeCompare(b.name || '', 'he');
                        if (!dateA) return 1;
                        if (!dateB) return -1;
                        // Compare dates (assuming format is consistent and comparable as strings)
                        // Reverse order for most recent first
                        return dateB.localeCompare(dateA);
                    });
                    break;
                case 'random':
                    // Fisher-Yates shuffle
                    for (let i = filteredSongs.length - 1; i > 0; i--) {
                        const j = Math.floor(Math.random() * (i + 1));
                        [filteredSongs[i], filteredSongs[j]] = [filteredSongs[j], filteredSongs[i]];
                    }
                    break;
            }
        }

        // Change sort order and re-apply
        function changeSortOrder(order) {
            currentSortOrder = order;
            sortFilteredSongs();
            currentPage = 1;

            // Update toggle UI
            updateSortToggleSlider(order);

            // Update display with animation
            if (currentPageView === 'songs') {
                updateSongsListWithAnimation();
            } else {
                renderCurrentPage();
            }
        }

        // Update sort toggle slider position dynamically
        function updateSortToggleSlider(order) {
            const toggle = document.getElementById('sortToggle');
            if (!toggle) return;

            const slider = toggle.querySelector('.sort-toggle-slider');
            const activeOption = toggle.querySelector(`.sort-toggle-option[data-sort="${order}"]`);

            if (slider && activeOption) {
                // Get the container's bounding rect
                const containerRect = toggle.getBoundingClientRect();
                const optionRect = activeOption.getBoundingClientRect();

                // Calculate position relative to container (RTL: use right position)
                const rightOffset = containerRect.right - optionRect.right;
                const width = optionRect.width;

                slider.style.right = rightOffset + 'px';
                slider.style.width = width + 'px';
            }

            // Update active class on options
            toggle.querySelectorAll('.sort-toggle-option').forEach(opt => {
                opt.classList.toggle('active', opt.dataset.sort === order);
            });
        }

        // Initialize sort toggle slider position on page load
        function initSortToggleSlider() {
            setTimeout(() => {
                updateSortToggleSlider(currentSortOrder);
                updateDisplayToggleSlider(currentDisplayMode);
                setupQualityStarsHover();
                initStickySidebar();
            }, 50);
        }

        // Initialize sticky sidebar - CSS sticky handles this now
        function initStickySidebar() {
            const sidebar = document.querySelector('.songs-sidebar');
            const wrapper = document.querySelector('.sidebar-wrapper');
            if (!sidebar || !wrapper) return;

            const header = document.querySelector('.main-header');
            const headerHeight = header ? header.offsetHeight : 80;
            const topOffset = 0;

            // Get initial position
            const wrapperRect = wrapper.getBoundingClientRect();
            const initialTop = wrapperRect.top + window.scrollY;
            const sidebarWidth = sidebar.offsetWidth;

            let isFixed = false;

            function handleScroll() {
                const currentSidebar = document.querySelector('.songs-sidebar');
                if (!currentSidebar) {
                    window.removeEventListener('scroll', handleScroll);
                    return;
                }

                const scrollY = window.scrollY;
                const mainContent = document.querySelector('.songs-main-content');
                if (!mainContent) return;

                const mainRect = mainContent.getBoundingClientRect();
                const shouldBeFixed = scrollY > initialTop - topOffset;
                const maxBottom = mainRect.bottom + scrollY - currentSidebar.offsetHeight - 20;

                if (shouldBeFixed && scrollY < maxBottom) {
                    if (!isFixed) {
                        currentSidebar.style.position = 'fixed';
                        currentSidebar.style.top = topOffset + 'px';
                        currentSidebar.style.width = sidebarWidth + 'px';
                        isFixed = true;
                    }
                } else if (!shouldBeFixed) {
                    if (isFixed) {
                        currentSidebar.style.position = '';
                        currentSidebar.style.top = '';
                        currentSidebar.style.width = '';
                        isFixed = false;
                    }
                }
            }

            window.addEventListener('scroll', handleScroll, { passive: true });
        }

        // Change display mode and re-render
        function changeDisplayMode(mode) {
            currentDisplayMode = mode;

            // Update toggle UI
            updateDisplayToggleSlider(mode);

            // Update display with animation
            if (currentPageView === 'songs') {
                updateSongsListWithAnimation();
            } else {
                renderCurrentPage();
            }
        }

        // Set minimum quality rating filter
        function setMinQualityRating(rating) {
            // Toggle off if clicking the same rating
            if (minQualityRating === rating) {
                minQualityRating = 0;
            } else {
                minQualityRating = rating;
            }

            // Update the stars UI
            updateQualityStarsUI();

            // Re-render songs list
            if (currentPageView === 'songs') {
                updateSongsListWithAnimation();
            }
        }

        // Toggle recordings collapse in full mode
        function toggleRecordingsCollapse() {
            recordingsCollapsed = !recordingsCollapsed;
            // Re-render songs list
            if (currentPageView === 'songs') {
                updateSongsListWithAnimation();
            }
        }

        // Update quality stars UI
        function updateQualityStarsUI() {
            const container = document.getElementById('qualityStarsFilter');
            if (!container) return;

            const stars = container.querySelectorAll('.quality-star');
            stars.forEach((star, idx) => {
                const starRating = idx + 1;
                if (minQualityRating >= starRating) {
                    star.classList.add('active');
                } else {
                    star.classList.remove('active');
                }
            });

            const clearBtn = container.querySelector('.quality-clear');
            if (clearBtn) {
                if (minQualityRating === 0) {
                    clearBtn.classList.add('hidden');
                } else {
                    clearBtn.classList.remove('hidden');
                }
            }
        }

        // Setup hover handlers for quality stars (call after rendering)
        function setupQualityStarsHover() {
            const container = document.getElementById('qualityStarsFilter');
            if (!container) return;

            const stars = container.querySelectorAll('.quality-star');

            stars.forEach((star, idx) => {
                star.addEventListener('mouseenter', () => {
                    container.classList.add('hovering');
                    // Highlight all stars up to and including this one
                    stars.forEach((s, i) => {
                        if (i <= idx) {
                            s.classList.add('hover-active');
                        } else {
                            s.classList.remove('hover-active');
                        }
                    });
                });
            });

            container.addEventListener('mouseleave', () => {
                container.classList.remove('hovering');
                stars.forEach(s => s.classList.remove('hover-active'));
            });
        }

        // Check if a recording meets the minimum quality threshold
        // Unrated recordings (rating=0 or undefined) always pass so they can be discovered
        function recordingMeetsQuality(recording) {
            if (minQualityRating === 0) return true; // No filter
            const rating = recording.audioRating || recording.rating || 0;
            // Unrated recordings (0) always pass - otherwise no one will ever hear them
            if (rating === 0) return true;
            return rating >= minQualityRating;
        }

        // Check if a song has any recording that meets the quality threshold
        function songHasQualityAudio(song) {
            if (minQualityRating === 0) {
                // No quality filter, just check if has audio
                return song.audioUrl && song.audioUrl.startsWith('http');
            }
            if (!song.recordings || song.recordings.length === 0) return false;
            return song.recordings.some(rec => recordingMeetsQuality(rec));
        }

        // Get the best quality recording for minimal mode playback
        // Picks the highest-rated recording, randomizes among ties
        function getBestQualityRecording(song) {
            if (!song.recordings || song.recordings.length === 0) return null;

            // Filter to quality recordings first
            let qualityRecs = song.recordings.filter(rec => recordingMeetsQuality(rec));
            if (qualityRecs.length === 0) return null;
            if (qualityRecs.length === 1) return qualityRecs[0];

            // Find the highest rating among quality recordings
            let maxRating = 0;
            qualityRecs.forEach(rec => {
                const rating = rec.audioRating || rec.rating || 0;
                if (rating > maxRating) maxRating = rating;
            });

            // Get all recordings with the max rating (including unrated if maxRating is 0)
            let topRecs = qualityRecs.filter(rec => {
                const rating = rec.audioRating || rec.rating || 0;
                return rating === maxRating;
            });

            // Randomly pick one from the top-rated recordings
            if (topRecs.length === 1) return topRecs[0];
            const randomIdx = Math.floor(Math.random() * topRecs.length);
            return topRecs[randomIdx];
        }

        // Alias for backwards compatibility
        function getFirstQualityRecording(song) {
            return getBestQualityRecording(song);
        }

        // Update display toggle slider position dynamically
        function updateDisplayToggleSlider(mode) {
            const toggle = document.getElementById('displayToggle');
            if (!toggle) return;

            const slider = toggle.querySelector('.sort-toggle-slider');
            const activeOption = toggle.querySelector(`.sort-toggle-option[data-mode="${mode}"]`);

            if (slider && activeOption) {
                // Get the container's bounding rect
                const containerRect = toggle.getBoundingClientRect();
                const optionRect = activeOption.getBoundingClientRect();

                // Calculate position relative to container (RTL: use right position)
                const rightOffset = containerRect.right - optionRect.right;
                const width = optionRect.width;

                slider.style.right = rightOffset + 'px';
                slider.style.width = width + 'px';
            }

            // Update active class on options
            toggle.querySelectorAll('.sort-toggle-option').forEach(opt => {
                opt.classList.toggle('active', opt.dataset.mode === mode);
            });
        }

        // Apply filters without updating display (for use before animated update)
        // skipSort: if true, don't re-sort - used during API loading to preserve order
        function applyFiltersQuiet(skipSort = false) {
            filteredSongs = allSongs.filter(song => {


                // Check search query
                if (searchQuery) {
                    const query = searchQuery.toLowerCase();
                    const matchName = song.name?.toLowerCase().includes(query);
                    const matchMechaber = song.mechaber?.some(m => m.toLowerCase().includes(query));
                    const matchCollection = song.collections?.some(c => c.toLowerCase().includes(query));
                    if (!matchName && !matchMechaber && !matchCollection) return false;
                }

                // Check all active filters
                if (activeFilters.mechaber.length > 0) {
                    if (!activeFilters.mechaber.some(f => song.mechaber?.includes(f))) return false;
                }
                if (activeFilters.collection.length > 0) {
                    if (!activeFilters.collection.some(f => song.collections?.includes(f))) return false;
                }
                if (activeFilters.zman.length > 0) {
                    if (!activeFilters.zman.some(f => song.pasigOif?.includes(f))) return false;
                }
                if (activeFilters.scale.length > 0) {
                    if (!activeFilters.scale.some(f => song.scale?.includes(f))) return false;
                }
                if (activeFilters.ritem.length > 0) {
                    if (!activeFilters.ritem.some(f => song.ritem?.includes(f))) return false;
                }
                if (activeFilters.gezungen.length > 0) {
                    if (!activeFilters.gezungen.some(f => song.gezungen?.includes(f))) return false;
                }
                if (activeFilters.maure.length > 0) {
                    if (!activeFilters.maure.some(f => song.maure?.includes(f))) return false;
                }
                if (activeFilters.verter.length > 0) {
                    if (!activeFilters.verter.some(f => song.verter?.includes(f))) return false;
                }
                if (activeFilters.chatzer.length > 0) {
                    if (!activeFilters.chatzer.some(f => song.chatzer?.includes(f))) return false;
                }
                return true;
            });
            if (!skipSort) {
                sortFilteredSongs();
            }
            currentPage = 1;
        }

        // Apply filters
        function applyFilters() {
            filteredSongs = allSongs.filter(song => {


                // Check search query
                if (searchQuery) {
                    const query = searchQuery.toLowerCase();
                    const searchableText = [
                        song.name,
                        song.mechaber,
                        song.category,
                        song.verter,
                        song.collections,
                        song.pasigOif,
                        song.info,
                        song.siman
                    ].filter(Boolean).join(' ').toLowerCase();
                    if (!searchableText.includes(query)) return false;
                }
                // Check chatzer
                if (activeFilters.chatzer.length > 0) {
                    if (!activeFilters.chatzer.some(f => song.category?.includes(f))) return false;
                }
                // Check mechaber
                if (activeFilters.mechaber.length > 0) {
                    if (!activeFilters.mechaber.some(f => song.mechaber?.includes(f))) return false;
                }
                // Check collection
                if (activeFilters.collection.length > 0) {
                    if (!activeFilters.collection.some(f => song.collections?.includes(f))) return false;
                }
                // Check zman
                if (activeFilters.zman.length > 0) {
                    if (!activeFilters.zman.some(f => song.pasigOif?.includes(f))) return false;
                }
                // Check scale
                if (activeFilters.scale.length > 0) {
                    if (!activeFilters.scale.some(f => song.scale?.includes(f))) return false;
                }
                // Check ritem
                if (activeFilters.ritem.length > 0) {
                    if (!activeFilters.ritem.some(f => song.ritem?.includes(f))) return false;
                }
                // Check gezungen
                if (activeFilters.gezungen.length > 0) {
                    if (!activeFilters.gezungen.some(f => song.gezungen?.includes(f))) return false;
                }
                // Check maure
                if (activeFilters.maure.length > 0) {
                    if (!activeFilters.maure.some(f => song.maure?.includes(f))) return false;
                }
                return true;
            });

            sortFilteredSongs();
            currentPage = 1;

            // If we're on songs page, update the list with fade animation
            if (currentPageView === 'songs') {
                updateSongsListWithAnimation(); // Fade out/in for filters
                updateSongsCount();
                renderActiveFilterTags();
                updateSidebarFilterValues();
            } else {
                renderCurrentPage();
            }
        }

        // Update songs list with animation specifically for filter changes
        function updateSongsListWithAnimation() {
            const grid = document.getElementById('songsGrid');
            if (!grid) return;

            const existingItems = Array.from(grid.children);

            // If no existing items, just load without animation
            if (existingItems.length === 0) {
                songsLazyState = {
                    currentIndex: 0,
                    batchSize: 50,
                    isLoading: false,
                    isInitialLoad: true
                };
                loadMoreSongs();
                return;
            }

            // Fade out existing items
            existingItems.forEach((item, i) => {
                item.style.transition = 'opacity 0.18s ease-out, transform 0.18s ease-out';
                item.style.transitionDelay = `${Math.min(i * 5, 100)}ms`;
                item.style.opacity = '0';
                item.style.transform = 'scale(0.97)';
            });

            // After fade out, reload with slide-up animation
            setTimeout(() => {
                grid.innerHTML = '';

                // For sorted modes: seed with song at position 0 to allow smooth FLIP
                // For shuffle: just load first song normally
                if (currentSortOrder !== 'random' && filteredSongs.length > 0) {
                    const seedPosition = 0; // Seed with first song, loadMoreSongs starts at 1
                    const seedSong = filteredSongs[seedPosition];
                    const seedIdx = allSongs.indexOf(seedSong);
                    const playlistIndices = filteredSongs.map(song => allSongs.indexOf(song));

                    // Create the seed element
                    const seedHtml = renderSongItem(seedSong, seedIdx, seedPosition + 1, playlistIndices);
                    grid.insertAdjacentHTML('beforeend', seedHtml);
                    songsLazyState = {
                        currentIndex: 1, // Start AFTER the seed
                        batchSize: 50,
                        isLoading: false,
                        isInitialLoad: false
                    };

                    // Mark only this song as seen - ALL others will FLIP in
                    seenSongIds = new Set([seedSong.rowId || seedSong.name]);
                    console.log(`Sort change: seeded with song at position ${seedPosition} for FLIP`);

                    // Animate the seed song, then trigger FLIP
                    requestAnimationFrame(() => {
                        const item = grid.children[0];
                        if (item) {
                            item.style.opacity = '0';
                            item.style.transform = 'translateY(20px)';
                            setTimeout(() => {
                                item.style.transition = 'opacity 0.25s ease-out, transform 0.3s ease-out';
                                item.style.opacity = '1';
                                item.style.transform = 'translateY(0)';
                            }, 50);
                        }

                        // Manually trigger FLIP with all other songs
                        // This ensures songs are inserted at correct positions
                        setTimeout(() => {
                            const allOtherSongs = allSongs.filter(s => {
                                const id = s.rowId || s.name;
                                return !seenSongIds.has(id);
                            });
                            if (allOtherSongs.length > 0 && grid.children.length > 0) {
                                console.log(`Sort change: triggering FLIP for ${allOtherSongs.length} songs`);
                                updateSongsListWithNewSongs(allOtherSongs);
                            }
                        }, 100);
                    });
                } else {
                    // Shuffle mode or small list - load normally
                    songsLazyState = {
                        currentIndex: 0,
                        batchSize: 1,
                        isLoading: false,
                        isInitialLoad: true
                    };
                    loadMoreSongs();
                    songsLazyState.batchSize = 50;

                    // Mark only the displayed song as seen
                    const displayedSongs = filteredSongs.slice(0, 1);
                    seenSongIds = new Set(displayedSongs.map(s => s.rowId || s.name));
                    console.log(`Sort change: marked ${seenSongIds.size} displayed song as seen (shuffle mode)`);

                    // Animate new items sliding up from below
                    requestAnimationFrame(() => {
                        const newItems = Array.from(grid.children);
                        newItems.forEach((item, i) => {
                            item.style.opacity = '0';
                            item.style.transform = 'translateY(20px)';
                            setTimeout(() => {
                                item.style.transition = 'opacity 0.25s ease-out, transform 0.3s ease-out';
                                item.style.opacity = '1';
                                item.style.transform = 'translateY(0)';
                            }, i * 20);
                        });
                    });
                }
            }, 250);
        }

        // Update songs list with FLIP animation for incremental data updates
        // Songs that need to make room slide down, new songs pop in at their sorted position
        function updateSongsList() {
            const grid = document.getElementById('songsGrid');
            if (!grid) return;

            const existingItems = Array.from(grid.children);

            // If no existing items or empty grid, just load normally
            if (existingItems.length === 0) {
                songsLazyState = {
                    currentIndex: 0,
                    batchSize: 50,
                    isLoading: false,
                    isInitialLoad: true
                };
                loadMoreSongs();
                updateSongsCount();
                return;
            }

            // Get currently displayed song indices
            const displayedSongIdxs = new Set();
            existingItems.forEach(item => {
                const idx = item.getAttribute('data-song-idx');
                if (idx) displayedSongIdxs.add(parseInt(idx));
            });

            // Get the first batch of filtered songs worth displaying
            const maxToShow = Math.max(existingItems.length, 50);
            const newFilteredBatch = filteredSongs.slice(0, maxToShow);
            const newSongIdxs = new Set(newFilteredBatch.map(song => allSongs.indexOf(song)));

            // Find songs to add (in newFilteredBatch but not displayed)
            const songsToAdd = newFilteredBatch.filter(song => {
                const idx = allSongs.indexOf(song);
                return !displayedSongIdxs.has(idx);
            });

            console.log(`FLIP: ${songsToAdd.length} songs to add out of ${newFilteredBatch.length}`);

            // If no songs to add, nothing to do - just return
            if (songsToAdd.length === 0) {
                console.log('FLIP: No new songs in visible range, skipping');
                return;
            }

            // If too many changes, just reload quietly (no animation)
            if (songsToAdd.length > 300) {
                console.log('FLIP: Too many songs to animate, reloading quietly');
                songsLazyState = {
                    currentIndex: 0,
                    batchSize: 50,
                    isLoading: false,
                    isInitialLoad: false  // Not initial - no animation
                };
                grid.innerHTML = '';
                loadMoreSongs(false);  // false = no animation
                updateSongsCount();
                return;
            }

            // FLIP Animation
            // Step 1 (First): Record positions of existing items
            const firstPositions = new Map();
            existingItems.forEach(item => {
                const idx = item.getAttribute('data-song-idx');
                firstPositions.set(idx, item.getBoundingClientRect());
            });

            // Step 2: Insert new songs at correct sorted positions
            const playlistIndices = filteredSongs.map(song => allSongs.indexOf(song));
            const newElements = [];

            songsToAdd.forEach(song => {
                const globalIdx = allSongs.indexOf(song);

                // Skip if song already exists in grid (prevent duplicates)
                if (grid.querySelector(`[data-song-idx="${globalIdx}"]`)) {
                    return;
                }

                const displayNum = playlistIndices.indexOf(globalIdx) + 1;
                const html = renderSongItem(song, globalIdx, displayNum, playlistIndices);

                const template = document.createElement('template');
                template.innerHTML = html.trim();
                const newElement = template.content.firstChild;

                // Find insertion point based on song name (Hebrew alphabetical)
                const songName = song.name || '';
                let insertBefore = null;

                for (const existingItem of grid.children) {
                    const existingSongName = existingItem.querySelector('.song-name')?.textContent || '';
                    if (songName.localeCompare(existingSongName, 'he') < 0) {
                        insertBefore = existingItem;
                        break;
                    }
                }

                if (insertBefore) {
                    grid.insertBefore(newElement, insertBefore);
                } else {
                    grid.appendChild(newElement);
                }

                newElements.push(newElement);
            });

            // Step 3 (Last + Invert + Play): Animate
            requestAnimationFrame(() => {
                // Animate existing items that moved down
                existingItems.forEach(item => {
                    if (!item.isConnected) return;

                    const idx = item.getAttribute('data-song-idx');
                    const first = firstPositions.get(idx);
                    if (!first) return;

                    const last = item.getBoundingClientRect();
                    const deltaY = first.top - last.top;

                    if (Math.abs(deltaY) > 1) {
                        // Item moved - animate from old position to new
                        item.style.transition = 'none';
                        item.style.transform = `translateY(${deltaY}px)`;

                        requestAnimationFrame(() => {
                            item.style.transition = 'transform 0.4s cubic-bezier(0.4, 0, 0.2, 1)';
                            item.style.transform = 'translateY(0)';
                        });
                    }
                });

                // Animate new items with pop-in effect
                newElements.forEach((item, i) => {
                    item.style.opacity = '0';
                    item.style.transform = 'scale(0.9)';

                    setTimeout(() => {
                        item.style.transition = 'opacity 0.3s ease-out, transform 0.35s cubic-bezier(0.34, 1.56, 0.64, 1)';
                        item.style.opacity = '1';
                        item.style.transform = 'scale(1)';
                    }, 100 + i * 50);
                });
            });

            // Update lazy state
            songsLazyState.currentIndex = grid.children.length;
            updateSongsCount();
        }

        // FLIP animation for specific new songs from API batch
        // Ensures grid is ALWAYS in correct order, with new songs animating in
        function updateSongsListWithNewSongs(newSongs) {
            isFlipping = true;


            const grid = document.getElementById('songsGrid');
            if (!grid) {

                isFlipping = false;
                return;
            }

            const existingItems = Array.from(grid.children);


            if (existingItems.length === 0) {

                isFlipping = false;
                return;
            }

            // Build SET of existing song indices (allSongs index) in grid
            const existingIdxSet = new Set();
            existingItems.forEach(item => {
                const idx = parseInt(item.getAttribute('data-song-idx'));
                if (!isNaN(idx)) existingIdxSet.add(idx);
            });


            // Determine how many items to show
            const targetCount = Math.min(existingItems.length + 20, 100);


            // Get the correct order from filteredSongs
            const correctOrder = filteredSongs.slice(0, targetCount);




            // In shuffle mode: use newSongs from API, not correctOrder
            const isShuffleModeForInsert = currentSortOrder === 'random';

            // Find which songs to insert
            const newToInsert = [];

            if (isShuffleModeForInsert && newSongs && newSongs.length > 0) {
                // SHUFFLE MODE: Pick random songs from the NEW API batch to insert
                // Scale insertions based on grid size - less insertions as grid grows
                const gridSize = existingItems.length;
                let maxInsert;
                if (gridSize < 50) {
                    maxInsert = 15;  // Early batches: more visible action
                } else if (gridSize < 100) {
                    maxInsert = 8;   // Medium: moderate
                } else {
                    maxInsert = 5;   // Large grid: subtle additions
                }
                maxInsert = Math.min(maxInsert, newSongs.length);

                // Shuffle newSongs and take first maxInsert that aren't in grid
                const shuffledNew = [...newSongs];
                for (let i = shuffledNew.length - 1; i > 0; i--) {
                    const j = Math.floor(Math.random() * (i + 1));
                    [shuffledNew[i], shuffledNew[j]] = [shuffledNew[j], shuffledNew[i]];
                }

                let insertCount = 0;
                for (const song of shuffledNew) {
                    if (insertCount >= maxInsert) break;
                    const idx = allSongs.indexOf(song);
                    if (idx >= 0 && !existingIdxSet.has(idx)) {
                        newToInsert.push({ song, idx, targetPos: insertCount });
                        insertCount++;
                    }
                }

            } else {
                // SORTED MODE: Use correctOrder to find missing songs
                correctOrder.forEach((song, pos) => {
                    const idx = allSongs.indexOf(song);
                    if (idx >= 0 && !existingIdxSet.has(idx)) {
                        newToInsert.push({ song, idx, targetPos: pos });
                    }
                });
            }



            if (newToInsert.length === 0) {
                isFlipping = false;
                return;
            }

            // Step 1 (First): Record positions of all existing items
            const firstPositions = new Map();
            existingItems.forEach(item => {
                firstPositions.set(item, item.getBoundingClientRect());
            });

            // Step 2: Insert new songs at correct positions (no clearing!)
            const playlistIndices = filteredSongs.map(song => allSongs.indexOf(song));
            const newElements = [];

            // In shuffle mode: shuffle the new songs first, then insert at random positions
            const isShuffleMode = currentSortOrder === 'random';
            if (isShuffleMode) {
                // Shuffle the newToInsert array
                for (let i = newToInsert.length - 1; i > 0; i--) {
                    const j = Math.floor(Math.random() * (i + 1));
                    [newToInsert[i], newToInsert[j]] = [newToInsert[j], newToInsert[i]];
                }

            }

            newToInsert.forEach(({ song, idx, targetPos }) => {
                // Double-check not already in grid
                if (grid.querySelector(`[data-song-idx="${idx}"]`)) {

                    return;
                }

                const posInFiltered = filteredSongs.indexOf(song);
                const displayNum = posInFiltered + 1;
                const html = renderSongItem(song, idx, displayNum, playlistIndices);

                const template = document.createElement('template');
                template.innerHTML = html.trim();
                const newElement = template.content.firstChild;

                let insertBefore = null;
                let insertBeforePos = -1;

                if (isShuffleMode) {
                    // SHUFFLE MODE: Insert at random position, SLIGHTLY BIASED TOWARDS BEGINNING
                    const gridItems = Array.from(grid.children);
                    // Expand visible range to first 70 items
                    const maxPos = Math.min(gridItems.length, 70);
                    // Use Math.pow(random, 1.3) for MILD bias towards beginning
                    // This spreads inserts more evenly but still slightly favors visible area
                    const weightedRandom = Math.pow(Math.random(), 1.3);
                    const randomPos = Math.floor(weightedRandom * maxPos);
                    if (randomPos < gridItems.length) {
                        insertBefore = gridItems[randomPos];
                        insertBeforePos = randomPos;
                    }
                } else {
                    // SORTED MODE: Find correct insertion point by position
                    for (const existingItem of grid.children) {
                        const existingIdx = parseInt(existingItem.getAttribute('data-song-idx'));
                        const existingSong = allSongs[existingIdx];
                        if (existingSong) {
                            const existingPos = filteredSongs.indexOf(existingSong);
                            if (posInFiltered < existingPos) {
                                insertBefore = existingItem;
                                insertBeforePos = existingPos;
                                break;
                            }
                        }
                    }
                }

                if (insertBefore) {

                    grid.insertBefore(newElement, insertBefore);
                } else {

                    grid.appendChild(newElement);
                }

                newElements.push(newElement);
                existingIdxSet.add(idx); // Mark as now in grid
            });



            // Step 3a: In SORTED mode, reorder ALL grid items to match correctOrder
            if (!isShuffleMode) {
                // Recalculate correctOrder to cover ALL items now in grid
                const actualGridSize = grid.children.length;
                const fullCorrectOrder = filteredSongs.slice(0, actualGridSize);


                // FIRST: Remove any duplicate idx elements (keep first occurrence)
                const seenIdxForDedup = new Set();
                const duplicatesToRemove = [];
                Array.from(grid.children).forEach(item => {
                    const idx = parseInt(item.getAttribute('data-song-idx'));
                    if (!isNaN(idx)) {
                        if (seenIdxForDedup.has(idx)) {
                            duplicatesToRemove.push(item);

                        } else {
                            seenIdxForDedup.add(idx);
                        }
                    }
                });
                duplicatesToRemove.forEach(item => item.remove());

                // Build a map of song idx to element (now no duplicates)
                const elemByIdx = new Map();
                Array.from(grid.children).forEach(item => {
                    const idx = parseInt(item.getAttribute('data-song-idx'));
                    if (!isNaN(idx)) elemByIdx.set(idx, item);
                });



                // Reorder: for each song in fullCorrectOrder, append to grid (moves to end in order)
                fullCorrectOrder.forEach(song => {
                    const idx = allSongs.indexOf(song);
                    const elem = elemByIdx.get(idx);
                    if (elem) {
                        grid.appendChild(elem); // Moves to end
                    }
                });
            }

            // Step 3b: Update display numbers for ALL items in grid
            Array.from(grid.children).forEach((item, gridPos) => {
                const numEl = item.querySelector('.song-number');
                if (numEl) {
                    if (isShuffleMode) {
                        // SHUFFLE MODE: Use grid position as display number
                        numEl.textContent = gridPos + 1;
                    } else {
                        // SORTED MODE: Use filteredSongs position
                        const idx = parseInt(item.getAttribute('data-song-idx'));
                        const song = allSongs[idx];
                        if (song) {
                            const correctPos = filteredSongs.indexOf(song) + 1;
                            numEl.textContent = correctPos;
                        }
                    }
                }
            });



            // Step 4 (Last + Invert + Play): Animate
            requestAnimationFrame(() => {
                // Animate existing items that moved
                existingItems.forEach(item => {
                    if (!item.isConnected) return;

                    const first = firstPositions.get(item);
                    if (!first) return;

                    const last = item.getBoundingClientRect();
                    const deltaX = first.left - last.left;
                    const deltaY = first.top - last.top;

                    if (Math.abs(deltaY) > 1 || Math.abs(deltaX) > 1) {
                        item.style.transition = 'none';
                        item.style.transform = `translate(${deltaX}px, ${deltaY}px)`;

                        requestAnimationFrame(() => {
                            item.style.transition = 'transform 0.4s cubic-bezier(0.4, 0, 0.2, 1)';
                            item.style.transform = 'translate(0, 0)';
                        });
                    }
                });

                // Animate new items with pop-in effect
                newElements.forEach((item, i) => {
                    item.style.opacity = '0';
                    item.style.transform = 'scale(0.8)';

                    setTimeout(() => {
                        item.style.transition = 'opacity 0.3s ease-out, transform 0.35s cubic-bezier(0.34, 1.56, 0.64, 1)';
                        item.style.opacity = '1';
                        item.style.transform = 'scale(1)';
                    }, 50 + i * 30);
                });
            });

            // Update lazy state
            songsLazyState.currentIndex = grid.children.length;

            isFlipping = false;
        }

        // Update songs count display
        function updateSongsCount() {
            const subtitle = document.querySelector('.page-title-content .subtitle');
            if (!subtitle) return;

            const count = filteredSongs.length;

            // Get or create count span
            let countSpan = subtitle.querySelector('.count-number');
            if (!countSpan) {
                countSpan = document.createElement('span');
                countSpan.className = 'count-number';
            }

            // If still loading from API
            if (!songsFullyLoaded) {
                // Show loading state with musical notes + count up
                let loader = subtitle.querySelector('.loader-tiny');
                if (!loader) {
                    subtitle.innerHTML = `${getTinyLoaderHTML()} <span class="count-number">0</span> ניגונים`;
                    countSpan = subtitle.querySelector('.count-number');
                }
                subtitle.classList.add('loading-state');
                // Animate count up
                animateCountUp(countSpan, count, 800, '');
            } else {
                // Loading complete - fade out loader, keep counting
                if (subtitle.classList.contains('loading-state')) {
                    const loader = subtitle.querySelector('.loader-tiny');
                    if (loader) {
                        loader.style.transition = 'opacity 0.3s ease-out';
                        loader.style.opacity = '0';
                        setTimeout(() => loader.remove(), 300);
                    }
                    subtitle.classList.remove('loading-state');
                }
                // Continue count animation
                if (!subtitle.querySelector('.count-number')) {
                    subtitle.innerHTML = `<span class="count-number">0</span> ניגונים`;
                    countSpan = subtitle.querySelector('.count-number');
                }
                animateCountUp(subtitle.querySelector('.count-number'), count, 800, '');
            }
        }

        // Update sidebar filter button values without re-rendering
        function updateSidebarFilterValues() {
            const filterKeys = ['collection', 'chatzer', 'mechaber', 'verter', 'gezungen', 'ritem', 'scale'];
            filterKeys.forEach(key => {
                const btn = document.querySelector(`#sidebar-dropdown-${key}`)?.previousElementSibling;
                if (btn) {
                    const selectedCount = activeFilters[key]?.length || 0;
                    const selectedValue = selectedCount > 0 ? `(${selectedCount})` : '(אלע)';
                    const valueSpan = btn.querySelector('.sidebar-filter-value');
                    if (valueSpan) {
                        valueSpan.textContent = selectedValue;
                    }
                    if (selectedCount > 0) {
                        btn.classList.add('active');
                    } else {
                        btn.classList.remove('active');
                    }
                }
            });
        }

        // Render active filter tags
        function renderActiveFilterTags() {
            const container = document.getElementById('activeFilterTags');
            if (!container) return;

            let html = '';
            let hasFilters = false;
            const labels = { chatzer: 'חצר', mechaber: 'מחבר', collection: 'קאלעקשן', verter: 'ווערטער', zman: 'זמן', scale: 'סקעיל', ritem: 'ריטעם', gezungen: 'געזונגען אויף', maure: 'מאורע' };

            for (let [key, values] of Object.entries(activeFilters)) {
                if (!Array.isArray(values)) {
                    // Handle single value (from navigateToWithFilter)
                    if (values) {
                        hasFilters = true;
                        const escapedValues = values.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                        html += `
                            <div class="filter-tag">
                                ${labels[key]}: ${values}
                                <span class="filter-tag-close" onclick="removeFilter('${key}', '${escapedValues}')">&times;</span>
                            </div>
                        `;
                    }
                } else {
                    values.forEach(value => {
                        hasFilters = true;
                        const escapedValue = value.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                        html += `
                            <div class="filter-tag">
                                ${labels[key]}: ${value}
                                <span class="filter-tag-close" onclick="removeFilter('${key}', '${escapedValue}')">&times;</span>
                            </div>
                        `;
                    });
                }
            }

            // Add clear all button if there are filters
            if (hasFilters) {
                html += `<button class="clear-filters-btn" onclick="clearAllFilters()">🗑️ נעם אראפ אלעס</button>`;
            }

            container.innerHTML = html;
        }

        // Remove filter
        function removeFilter(key, value) {
            activeFilters[key] = activeFilters[key].filter(v => v !== value);
            applyFilters();
            renderActiveFilterTags();
        }

        // Clear all filters
        function clearAllFilters() {
            activeFilters = {
                chatzer: [],
                mechaber: [],
                verter: [],
                zman: [],
                collection: [],
                scale: [],
                ritem: [],
                gezungen: [],
                maure: []
            };
            searchQuery = '';

            // Uncheck all filter checkboxes
            document.querySelectorAll('.sidebar-dropdown-content input[type="checkbox"]').forEach(cb => {
                cb.checked = false;
            });

            // Check all "אלע" radio buttons
            document.querySelectorAll('.sidebar-dropdown-content input[type="radio"]').forEach(rb => {
                rb.checked = true;
            });

            applyFilters();
            renderActiveFilterTags();
        }

        // Check if any filters are active
        function hasAnyFilters() {
            if (searchQuery) return true;
            return Object.values(activeFilters).some(arr => arr.length > 0);
        }

        // Handle songs page search
        function handleSongsSearch(value) {
            searchQuery = value.trim();
            currentPage = 1;
            applyFilters();
        }

        // Clear search only
        function clearSearch() {
            searchQuery = '';
            currentPage = 1;
            // Clear the input box
            const searchInput = document.getElementById('songsSearchInput');
            if (searchInput) searchInput.value = '';
            applyFilters();
            // Re-render to remove the X button
            renderCurrentPage();
        }





        // Current detail view
        let currentDetailView = null; // { type: 'mechaber', name: 'xxx' }

        // URL Routing
        function updateURL() {
            let hash = '';

            if (currentDetailView) {
                // Detail page URL: #/mechabrim/123
                // Prefer customId (simple number), then name (no rowId in URLs)
                const id = currentDetailView.customId || currentDetailView.name;
                hash = `#/${currentDetailView.type}/${encodeURIComponent(id)}`;
            } else if (currentPageView === 'search' && searchQuery) {
                // Search page URL: #/search/קדיש
                hash = `#/search/${encodeURIComponent(searchQuery)}`;
            } else {
                // Category page URL: #/mechabrim or #/home
                // Map internal 'songs' to external 'nigunim' URL
                const urlPage = currentPageView === 'songs' ? 'nigunim' : currentPageView;
                hash = `#/${urlPage}`;
            }

            // Update URL without triggering hashchange
            if (window.location.hash !== hash) {
                history.pushState(null, '', hash);
            }
        }

        // Helper function to find item by ID or name
        function findItemByIdOrName(type, idOrName) {
            // Get the appropriate info object
            let infoObj = {};
            switch (type) {
                case 'chatzeros': infoObj = chatzerosInfo; break;
                case 'mechabrim': infoObj = mechabrimInfo; break;
                case 'verter': infoObj = verterInfo; break;
                case 'zmanim':
                case 'zman': infoObj = Object.assign({}, zmaninInfo, piyutimInfo); break;
                case 'collections': infoObj = collectionsInfo; break;
                case 'albums': infoObj = albumsInfo; break;
                case 'documents': infoObj = documentsInfo; break;
                case 'resources': infoObj = resourcesInfo; break;
                case 'nigunim':
                    // For nigunim, search in allSongs
                    const song = allSongs.find(s => s.customId === idOrName || s.rowId === idOrName || s.name === idOrName);
                    return song ? { name: song.name, id: song.rowId, customId: song.customId } : null;
            }

            // First try to find by customId (simple number)
            for (const [name, info] of Object.entries(infoObj)) {
                if (info.customId === idOrName) {
                    return { name, id: info.rowId, customId: info.customId };
                }
            }

            // Then try to find by rowId
            for (const [name, info] of Object.entries(infoObj)) {
                if (info.rowId === idOrName) {
                    return { name, id: info.rowId, customId: info.customId };
                }
            }

            // Fallback to finding by name (for backwards compatibility)
            if (infoObj[idOrName]) {
                return { name: idOrName, id: infoObj[idOrName].rowId, customId: infoObj[idOrName].customId };
            }

            return null;
        }

        // Track last handled hash to prevent duplicate processing
        let lastHandledHash = '';
        let lastHandledTime = 0;

        // Parse URL and navigate - with lazy loading
        async function handleURLChange() {
            const hash = window.location.hash || '#/home';
            const now = Date.now();

            // Debounce: skip if same hash was handled within 500ms
            if (hash === lastHandledHash && (now - lastHandledTime) < 500) {
                console.log('⏭️ Skipping duplicate handleURLChange for:', hash);
                return;
            }
            lastHandledHash = hash;
            lastHandledTime = now;

            console.log('handleURLChange called with hash:', hash);
            const parts = hash.substring(2).split('/'); // Remove #/ and split

            const page = parts[0] || 'home';
            const idOrName = parts[1] ? decodeURIComponent(parts[1]) : null;
            console.log('Parsed URL - page:', page, 'idOrName:', idOrName);

            // Valid pages (including info pages)
            const validPages = ['home', 'nigunim', 'chatzeros', 'mechabrim', 'verter', 'zmanim', 'piyutim', 'collections', 'albums', 'documents', 'resources', 'zman', 'search', 'music', 'about', 'instructions', 'credits', 'thanks', 'terms', 'stats', 'contribute'];

            // Info pages that don't need data loading
            const infoPagesList = ['about', 'instructions', 'credits', 'thanks', 'terms', 'stats', 'contribute'];

            if (validPages.includes(page)) {
                const content = document.getElementById('mainContent');
                const previousPage = currentPageView;
                const wasHomePage = previousPage === 'home';
                const wasInfoPage = infoPagesList.includes(previousPage);
                const isHomePage = page === 'home';
                const isInfoPage = infoPagesList.includes(page);

                // Map to internal page name for direction calculation
                const targetPage = page === 'nigunim' ? 'songs' : (page === 'zman' ? 'zmanim' : page);
                const direction = getNavigationDirection(previousPage, targetPage);

                // Determine exit animation class
                let exitClass;
                if (wasHomePage) {
                    exitClass = 'page-exit-home';
                } else if (wasInfoPage) {
                    exitClass = 'page-exit-info';
                } else {
                    exitClass = direction === 'forward' ? 'page-exit' : 'page-exit-back';
                }

                // Apply exit animation (only if there's existing content)
                if (content && content.children.length > 0 && previousPage !== targetPage) {
                    content.classList.add(exitClass);
                    await new Promise(r => setTimeout(r, wasInfoPage ? 150 : 200));
                    content.classList.remove(exitClass);
                }

                // Handle info pages - they don't need data loading
                if (isInfoPage) {
                    currentPageView = page;
                    currentDetailView = null;
                    renderCurrentPage();

                    // Enter animation for info pages
                    if (content) {
                        content.classList.add('page-enter-info');
                        setTimeout(() => content.classList.remove('page-enter-info'), 300);
                    }

                    // Scroll to top
                    window.scrollTo({ top: 0, behavior: 'smooth' });
                    return;
                }

                // Track if we're showing skeleton (data isn't loaded yet)
                const hadSkeleton = targetPage !== 'home' && !isDataLoadedForPage(targetPage);

                // Show skeleton loader if data not loaded
                // For songs: don't await, let background fetch populate the grid
                if (hadSkeleton) {
                    if (targetPage === 'songs') {
                        // Songs: start loading in background, don't await
                        // The page will show skeleton, and fetchSongsInBackground will update the grid
                        ensureDataForPage(targetPage); // No await!
                    } else {
                        // Other pages: show skeleton and await
                        if (content) {
                            content.innerHTML = getSkeletonForPage(targetPage);
                        }
                        await ensureDataForPage(targetPage);
                    }
                }

                if (page === 'search' && idOrName) {
                    // Search results page
                    searchQuery = idOrName;
                    currentPageView = 'search';
                    currentDetailView = null;

                    // Update search input
                    const searchInput = document.getElementById('globalSearch');
                    if (searchInput) searchInput.value = idOrName;

                    // Show loading indicator and ensure data is loaded for search
                    if (!isDataLoadedForPage('search')) {
                        const content = document.getElementById('mainContent');
                        if (content) {
                            content.innerHTML = `
                                <div class="detail-loading-container" style="display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 400px; gap: 20px;">
                                    <div class="loading-wave-bar"></div>
                                    <p style="color: var(--text-secondary); font-size: 14px;">לאָדן...</p>
                                </div>
                            `;
                        }
                        await ensureDataForPage('search');
                    }

                    renderCurrentPage();
                } else if (idOrName) {
                    // Detail pages that need data loaded
                    const detailPages = ['mechabrim', 'chatzeros', 'verter', 'zmanim', 'piyutim', 'zman', 'collections', 'albums', 'documents', 'resources', 'nigunim'];
                    const dataPage = page === 'zman' ? 'zmanim' : (page === 'piyutim' ? 'piyutim' : (page === 'nigunim' ? 'songs' : page));

                    // Show loading indicator and ensure data is loaded for detail pages
                    // For nigunim, need to check songsFullyLoaded, not just loadedCategories
                    const needsLoading = page === 'nigunim' ? !songsFullyLoaded : !isDataLoadedForPage(dataPage);
                    if (detailPages.includes(page) && needsLoading) {
                        if (content) {
                            content.innerHTML = `
                                <div class="detail-loading-container" style="display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 400px; gap: 20px;">
                                    <div class="loading-wave-bar"></div>
                                    <div style="display: flex; flex-direction: column; align-items: center; gap: 16px;">
                                        ${getMediumLoaderHTML()}
                                        <p style="color: var(--text-secondary); font-size: 16px; margin: 0;">לאודינג...</p>
                                    </div>
                                </div>
                            `;
                        }
                        // For nigunim, wait for ALL songs to load so we can find the specific one
                        await ensureDataForPage(dataPage, page === 'nigunim');
                    }

                    // Find item by ID or name
                    const item = findItemByIdOrName(page, idOrName);
                    console.log('Found item:', item);

                    // Redirect to proper numeric ID URL if we found the item
                    const isNumericId = /^\d+$/.test(idOrName);
                    if (!isNumericId && item?.customId) {
                        const newHash = `#/${page}/${item.customId}`;
                        history.replaceState(null, '', newHash);
                    }

                    const itemName = item?.name || idOrName;
                    const itemId = item?.id || idOrName;
                    const itemCustomId = item?.customId;

                    // Navigate to detail page
                    if (page === 'zman') {
                        // Zman detail page (shows piyutim and collections for a zman)
                        currentPageView = 'zmanim';
                        currentDetailView = { type: 'zman', name: itemName, id: itemId, customId: itemCustomId };
                    } else if (page === 'nigunim') {
                        // Nigun detail page (nigunim/1 format)
                        currentPageView = 'songs';
                        currentDetailView = { type: 'nigunim', name: itemName, id: itemId, customId: itemCustomId };
                    } else {
                        currentPageView = page;
                        currentDetailView = { type: page, name: itemName, id: itemId, customId: itemCustomId };
                    }

                    // Update nav buttons
                    document.querySelectorAll('.nav-btn').forEach(btn => {
                        const btnPage = btn.dataset.page;
                        const activePage = page === 'zman' ? 'zmanim' : (page === 'nigunim' ? 'songs' : page);
                        btn.classList.toggle('active', btnPage === activePage);
                    });

                    renderDetailPage();
                } else {
                    // Navigate to category page
                    currentDetailView = null;
                    // Map URL 'nigunim' to internal 'songs' page
                    currentPageView = page === 'nigunim' ? 'songs' : page;
                    currentPage = 1;

                    // Update nav buttons
                    document.querySelectorAll('.nav-btn').forEach(btn => {
                        btn.classList.toggle('active', btn.dataset.page === page);
                    });

                    renderCurrentPage();
                }

                // Determine enter animation class
                // Skip animation for certain pages
                const categoryPages = ['chatzeros', 'mechabrim', 'verter', 'zmanim', 'collections', 'resources', 'documents', 'albums'];
                const isCategoryPage = categoryPages.includes(targetPage);

                let enterClass;
                if (hadSkeleton) {
                    enterClass = null; // Coming from skeleton - no page animation needed
                } else if (isHomePage) {
                    enterClass = null; // Home page uses its own stagger animation
                } else if (isCategoryPage) {
                    enterClass = null; // Category pages use card animations, no page fade
                } else {
                    // Content pages (including songs): direction-aware
                    enterClass = direction === 'forward' ? 'page-enter' : 'page-enter-back';
                }

                // Apply enter animation
                if (content && enterClass) {
                    content.classList.add(enterClass);
                    setTimeout(() => content.classList.remove(enterClass), 350);
                }

                // Scroll to top
                window.scrollTo({ top: 0, behavior: 'smooth' });
            }
        }

        // NOTE: popstate listener is now in the nigun modal section to handle modal closing

        // Listen to hash changes (from anchor link clicks)
        window.addEventListener('hashchange', function () {
            hideAllDropdowns();
            handleURLChange();
        });

        // Navigate to detail page
        async function navigateToDetail(categoryKey, name, id = null) {
            const content = document.getElementById('mainContent');
            const dataPage = categoryKey === 'zman' ? 'zmanim' : categoryKey;

            // Show loading indicator if data not loaded yet
            if (!isDataLoadedForPage(dataPage)) {
                if (content) {
                    content.innerHTML = `
                        <div class="detail-loading-container" style="display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 400px; gap: 20px;">
                            <div class="loading-wave-bar"></div>
                            <div style="display: flex; flex-direction: column; align-items: center; gap: 16px;">
                                ${getMediumLoaderHTML()}
                                <p style="color: var(--text-secondary); font-size: 16px; margin: 0;">לאודינג ${name}...</p>
                            </div>
                        </div>
                    `;
                }
                await ensureDataForPage(dataPage);
            }

            // Get info object for this category
            let infoObj = {};
            switch (categoryKey) {
                case 'chatzeros': infoObj = chatzerosInfo; break;
                case 'mechabrim': infoObj = mechabrimInfo; break;
                case 'verter': infoObj = verterInfo; break;
                case 'zmanim': infoObj = Object.assign({}, zmaninInfo, piyutimInfo); break;
                case 'piyutim': infoObj = piyutimInfo; break;
                case 'collections': infoObj = collectionsInfo; break;
                case 'albums': infoObj = albumsInfo; break;
                case 'documents': infoObj = documentsInfo; break;
                case 'resources': infoObj = resourcesInfo; break;
            }

            // Get customId and rowId from the loaded data
            const itemInfo = infoObj[name];
            const customId = itemInfo?.customId;
            const rowId = itemInfo?.rowId || id;

            currentDetailView = { type: categoryKey, name: name, id: rowId, customId: customId };
            updateURL();
            renderDetailPage();
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }

        // Go back from detail page
        function goBackFromDetail() {
            const page = currentDetailView?.type || 'home';
            currentDetailView = null;

            // Map category key to page name
            const pageMap = {
                'chatzeros': 'chatzeros',
                'mechabrim': 'mechabrim',
                'verter': 'verter',
                'zmanim': 'zmanim',
                'piyutim': 'piyutim',
                'collections': 'collections',
                'albums': 'albums',
                'documents': 'documents',
                'resources': 'resources'
            };

            navigateTo(pageMap[page] || 'home');
        }

        // Render detail page for a specific item
        async function renderDetailPage() {
            console.log('renderDetailPage called, currentDetailView:', currentDetailView);
            const { type, name } = currentDetailView;
            const container = document.getElementById('mainContent');

            // Handle zman detail page separately
            if (type === 'zman') {
                renderZmanDetailPage(container, name);
                return;
            }

            // Handle nigun detail page separately
            if (type === 'nigunim') {
                // Wait for related data to load so links have proper IDs
                const dataPromises = [];
                if (!loadedCategories.chatzeros) {
                    dataPromises.push(fetchChatzerosInfo().then(() => loadedCategories.chatzeros = true));
                }
                if (!loadedCategories.mechabrim) {
                    dataPromises.push(fetchMechabrimInfo().then(() => loadedCategories.mechabrim = true));
                }
                if (!loadedCategories.verter) {
                    dataPromises.push(fetchVerterInfo().then(() => loadedCategories.verter = true));
                }
                if (!loadedCategories.albums) {
                    dataPromises.push(fetchAlbumsInfo().then(() => loadedCategories.albums = true));
                }
                if (dataPromises.length > 0) {
                    await Promise.all(dataPromises);
                }
                renderNigunDetailPage(container, name);
                return;
            }

            // Handle album detail page separately
            if (type === 'albums') {
                renderAlbumDetailPage(container, name);
                return;
            }

            // Handle document detail page separately
            if (type === 'documents') {
                renderDocumentDetailPage(container, name);
                return;
            }

            // Handle resource detail page separately
            if (type === 'resources') {
                renderResourceDetailPage(container, name);
                return;
            }

            // Get info and songs for this item
            let infoObj = {};
            let icon = '';
            let typeName = '';

            switch (type) {
                case 'chatzeros':
                    infoObj = chatzerosInfo;
                    icon = '';
                    typeName = 'חצר';
                    break;
                case 'mechabrim':
                    infoObj = mechabrimInfo;
                    icon = '';
                    typeName = 'מחבר';
                    break;
                case 'verter':
                    infoObj = verterInfo;
                    icon = '';
                    typeName = 'ווערטער';
                    break;
                case 'zmanim':
                    infoObj = Object.assign({}, zmaninInfo, piyutimInfo);
                    icon = '';
                    // Check if this is a piyut or a zman
                    if (piyutimInfo[name]) {
                        typeName = 'פיוט';
                    } else {
                        typeName = 'זמן';
                    }
                    break;
                case 'piyutim':
                    infoObj = piyutimInfo;
                    icon = '';
                    typeName = 'פיוט';
                    break;
                case 'collections':
                    infoObj = collectionsInfo;
                    icon = '';
                    typeName = 'קאלעקשאן';
                    break;
                case 'albums':
                    infoObj = albumsInfo;
                    icon = '';
                    typeName = 'אלבום';
                    break;
                case 'documents':
                    infoObj = documentsInfo;
                    icon = '';
                    typeName = 'דאקומענט';
                    break;
                case 'resources':
                    infoObj = resourcesInfo;
                    icon = '';
                    typeName = 'רעסורס';
                    break;
            }

            const info = infoObj[name] || {};
            const songIndices = categories[type]?.[name] || [];
            const songs = songIndices.map(idx => allSongs[idx]).filter(Boolean);

            // Only show chatzer tags for mechabrim
            const chatzeros = (type === 'mechabrim' && info.chatzer)
                ? info.chatzer.split(',').map(c => c.trim()).filter(Boolean)
                : [];

            // For mechabrim, build tag name badge, full name, and life dates
            let mechaberTagName = '';
            let mechaberFullName = name; // default to the item name
            let mechaberLifeDates = '';

            if (type === 'mechabrim') {
                // Build name for profile page:
                // If גערופן exists: formal name (small) + known name (big)
                // If גערופן is empty: just show formal name with suffix
                const formalParts = [info.title, info.firstName, info.lastName].filter(Boolean);
                const formalName = formalParts.length > 0 ? formalParts.join(' ') : '';

                if (info.gerufen) {
                    // Has גערופן - show two lines: formal (small) + known (big)
                    const knownParts = [info.gerufen, info.platz, info.suffix].filter(Boolean);
                    const knownName = knownParts.join(' ');
                    mechaberFullName = `<span class="mechaber-formal">${formalName}</span><span class="mechaber-known">${knownName}</span>`;
                } else {
                    // No גערופן - show formal name with suffix in one line
                    const fullNameParts = [info.title, info.firstName, info.lastName, info.platz, info.suffix].filter(Boolean);
                    mechaberFullName = fullNameParts.length > 0 ? fullNameParts.join(' ') : name;
                }

                // Tag name is from טעג נאמען column - show only if different
                if (info.tagName && info.tagName !== mechaberFullName && info.tagName !== formalName) {
                    mechaberTagName = info.tagName;
                }

                // Build life dates string (like: י' אייר ה'תרל"ד - ד' אב ה'תש"ד)
                const birthParts = [info.birthDay, info.birthMonth, info.birthYear].filter(Boolean);
                const deathParts = [info.deathDay, info.deathMonth, info.deathYear].filter(Boolean);
                const birthStr = birthParts.length > 0 ? birthParts.join(' ') : '';
                const deathStr = deathParts.length > 0 ? deathParts.join(' ') : '';

                if (birthStr || deathStr) {
                    if (birthStr && deathStr) {
                        mechaberLifeDates = `${birthStr} - ${deathStr}`;
                    } else if (birthStr) {
                        mechaberLifeDates = `נולד: ${birthStr}`;
                    } else if (deathStr) {
                        mechaberLifeDates = `נפטר: ${deathStr}`;
                    }
                }
            }

            // For chatzeros, find all mechabrim that belong to this chatzer
            let mechabrimFromChatzer = [];
            if (type === 'chatzeros') {
                mechabrimFromChatzer = Object.entries(mechabrimInfo)
                    .filter(([mechName, mechInfo]) => {
                        if (!mechInfo.chatzer) return false;
                        const mechChatzeros = mechInfo.chatzer.split(',').map(c => c.trim());
                        return mechChatzeros.includes(name);
                    })
                    .map(([mechName, mechInfo]) => ({
                        name: mechName,
                        image: mechInfo.image,
                        id: mechInfo.customId || mechName,
                        description: mechInfo.description
                    }));
            }

            // For verter, get the lyrics text
            let verterText = '';
            if (type === 'verter') {
                // Use fullText field first, then description
                if (info.fullText) {
                    verterText = info.fullText;
                } else if (info.description) {
                    verterText = info.description;
                }
            }

            // For verter, get makor and mareh makom
            const verterMakor = (type === 'verter' && info.makor) ? info.makor : '';
            const verterMarehMakom = (type === 'verter' && info.marehMakom) ? info.marehMakom : '';

            // For piyutim (in zmanim category), get zman
            const piyutZman = (type === 'zmanim' && piyutimInfo[name]?.zman) ? piyutimInfo[name].zman : '';

            // Map type to theme class - check if piyut
            const isPiyut = type === 'zmanim' && piyutimInfo[name];
            const themeMap = {
                'chatzeros': 'theme-chatzer',
                'mechabrim': 'theme-mechaber',
                'verter': 'theme-verter',
                'zmanim': isPiyut ? 'theme-piyut' : 'theme-zman',
                'collections': 'theme-collection'
            };
            const themeClass = themeMap[type] || '';

            container.innerHTML = `
                <div class="detail-page ${themeClass}" data-type="${type}">
                    <button class="back-button" onclick="goBackFromDetail()">
                        → צוריק
                    </button>
                    
                    <div class="detail-header">
                        <div class="detail-category-bar">${typeName}</div>
                        <div class="detail-header-content ${(info.image || type === 'mechabrim') ? '' : 'no-image'}">
                            ${info.image ? `
                                <img class="detail-image" src="${info.image}" alt="${name}">
                            ` : (type === 'mechabrim' ? `
                                <div class="detail-image-placeholder"><div class="mechaber-icon"></div></div>
                            ` : '')}
                            <div class="detail-header-text">
                                <h1 class="detail-title">${type === 'mechabrim' ? mechaberFullName : name}</h1>
                                ${mechaberLifeDates ? `
                                    <div class="detail-life-dates">
                                        ${mechaberLifeDates}
                                    </div>
                                ` : ''}
                                ${chatzeros.length > 0 ? `
                                    <div class="detail-badges">
                                        ${chatzeros.map(chatzer => `
                                            <span class="detail-chatzer-badge"
                                                  data-action="filter"
                                                  data-page="chatzeros"
                                                  data-filter-key="chatzer"
                                                  data-value="${escapeHtml(chatzer)}"
                                                  data-tooltip-value="${escapeHtml(chatzer)}"
                                                  data-tooltip-type="chatzer"
                                                  style="cursor:pointer;">${chatzer}</span>
                                        `).join('')}
                                    </div>
                                ` : ''}
                                ${(verterMakor || verterMarehMakom) ? `
                                    <div class="detail-subtitle">
                                        ${verterMakor ? `<span class="detail-makor-text">${verterMakor}</span>` : ''}
                                        ${(verterMakor && verterMarehMakom) ? '<span class="detail-separator">•</span>' : ''}
                                        ${verterMarehMakom ? `<span class="detail-mareh-text">${verterMarehMakom}</span>` : ''}
                                    </div>
                                ` : ''}
                                ${piyutZman ? `
                                    <div class="detail-subtitle">
                                        <span class="detail-zman-text">${piyutZman}</span>
                                    </div>
                                ` : ''}
                                ${(type === 'collections' && info.zmanim && info.zmanim.length > 0) ? `
                                    <div class="detail-zmanim-badges">
                                        ${info.zmanim.map(zman => `
                                            <span class="detail-zman-badge"
                                                  data-action="zman-detail"
                                                  data-name="${escapeHtml(zman)}"
                                                  style="cursor:pointer;">${zman}</span>
                                        `).join('')}
                                    </div>
                                ` : ''}
                            </div>
                        </div>
                    </div>
                    
                    ${info.description ? `
                        <div class="detail-description-section">
                            <h3>דעטאלן</h3>
                            <div class="detail-description-content">${formatDescription(info.description)}</div>
                        </div>
                    ` : ''}
                    
                    ${mechabrimFromChatzer.length > 0 ? `
                        <div class="detail-mechabrim-section">
                            <h3>${mechabrimFromChatzer.length} מחברים</h3>
                            <div class="nigun-mechaber-grid">
                                ${mechabrimFromChatzer.map(mech => `
                                    <a class="nigun-mechaber-card" href="#/mechabrim/${encodeURIComponent(mech.id)}" data-action="detail" data-category="mechabrim" data-name="${escapeHtml(mech.name)}">
                                        ${mech.image
                    ? `<img class="nigun-mechaber-image" src="${mech.image}" alt="${mech.name}">`
                    : `<div class="nigun-mechaber-placeholder"><div class="mechaber-icon"></div></div>`
                }
                                        <div class="nigun-mechaber-name">${mech.name}</div>
                                    </a>
                                `).join('')}
                            </div>
                        </div>
                    ` : ''}
                    
                    ${(type === 'verter' && verterText) ? `
                        <div class="detail-verter-section">
                            <h3>ווערטער</h3>
                            <div class="verter-content">${formatVerter(verterText)}</div>
                        </div>
                    ` : ''}
                    
                    <div class="detail-songs-section">
                        <h3>${songs.length} ניגונים ${type === 'mechabrim' ? 'וואס ער האט מחבר געווען' : ''}</h3>
                        <div class="list-layout">
                            ${(() => {
                    const playlistIndices = songs.map(song => allSongs.indexOf(song));
                    return songs.map((song, i) => {
                        const globalIdx = allSongs.indexOf(song);
                        return renderSongItem(song, globalIdx, i + 1, playlistIndices);
                    }).join('');
                })()}
                        </div>
                    </div>
                    
                    ${(() => {
                    // For mechabrim, show songs where they have a שייכות (personality)
                    if (type === 'mechabrim') {
                        const relatedSongs = allSongs.filter(song => {
                            if (!song.personality) return false;
                            const personalities = song.personality.split(',').map(p => p.trim());
                            return personalities.includes(name);
                        });

                        if (relatedSongs.length > 0) {
                            const playlistIndices = relatedSongs.map(song => allSongs.indexOf(song));
                            return `
                                    <div class="detail-songs-section detail-related-songs">
                                        <h3>${relatedSongs.length} ניגונים וואס ער האט א שייכות</h3>
                                        <div class="list-layout">
                                            ${relatedSongs.map((song, i) => {
                                const globalIdx = allSongs.indexOf(song);
                                return renderSongItem(song, globalIdx, i + 1, playlistIndices);
                            }).join('')}
                                        </div>
                                    </div>
                                `;
                        }
                    }
                    return '';
                })()}
                </div>
            `;
        }

        // Helper function to convert Coda internal links to site links
        function convertCodaLinkToSiteLink(linkText, url) {
            // Check if it's a Coda link
            if (!url.includes('coda.io')) {
                return null; // Not a Coda link
            }

            // Extract rowId from Coda URL
            // Coda URLs can have formats like:
            // https://coda.io/d/{docId}/...#_{tablePrefix}-{rowId}
            // https://coda.io/d/{docId}#...{rowId}
            // The rowId typically starts with 'i-' followed by alphanumeric characters
            let rowId = null;

            // Try to extract from hash fragment
            const hashMatch = url.match(/#.*?(i-[a-zA-Z0-9_-]+)/);
            if (hashMatch) {
                rowId = hashMatch[1];
            }

            // If we found a rowId, search for it across all entities
            if (rowId) {
                // Search in nigunim
                const song = allSongs.find(s => s.rowId === rowId);
                if (song) {
                    const displayId = song.customId || song.name;
                    return `#/nigunim/${encodeURIComponent(displayId)}`;
                }

                // Search in mechabrim
                for (const [name, info] of Object.entries(mechabrimInfo)) {
                    if (info.rowId === rowId) {
                        const displayId = info.customId || name;
                        return `#/mechabrim/${encodeURIComponent(displayId)}`;
                    }
                }

                // Search in verter
                for (const [name, info] of Object.entries(verterInfo)) {
                    if (info.rowId === rowId) {
                        const displayId = info.customId || name;
                        return `#/verter/${encodeURIComponent(displayId)}`;
                    }
                }

                // Search in chatzeros
                for (const [name, info] of Object.entries(chatzerosInfo)) {
                    if (info.rowId === rowId) {
                        const displayId = info.customId || name;
                        return `#/chatzeros/${encodeURIComponent(displayId)}`;
                    }
                }

                // Search in zmanim/piyutim
                for (const [name, info] of Object.entries(zmaninInfo)) {
                    if (info.rowId === rowId) {
                        const displayId = info.customId || name;
                        return `#/zman/${encodeURIComponent(displayId)}`;
                    }
                }
                for (const [name, info] of Object.entries(piyutimInfo)) {
                    if (info.rowId === rowId) {
                        const displayId = info.customId || name;
                        return `#/zman/${encodeURIComponent(displayId)}`;
                    }
                }

                // Search in collections
                for (const [name, info] of Object.entries(collectionsInfo)) {
                    if (info.rowId === rowId) {
                        const displayId = info.customId || name;
                        return `#/collections/${encodeURIComponent(displayId)}`;
                    }
                }

                // Search in albums
                for (const [name, info] of Object.entries(albumsInfo)) {
                    if (info.rowId === rowId) {
                        const displayId = info.customId || name;
                        return `#/albums/${encodeURIComponent(displayId)}`;
                    }
                }

                // Search in documents
                for (const [name, info] of Object.entries(documentsInfo)) {
                    if (info.rowId === rowId) {
                        const displayId = info.customId || name;
                        return `#/documents/${encodeURIComponent(displayId)}`;
                    }
                }

                // Search in resources
                for (const [name, info] of Object.entries(resourcesInfo)) {
                    if (info.rowId === rowId) {
                        const displayId = info.customId || name;
                        return `#/resources/${encodeURIComponent(displayId)}`;
                    }
                }
            }

            // If no rowId found or no match, try searching by name as fallback
            const searchText = linkText.trim();

            // Try to find in nigunim by name
            let found = findItemByIdOrName('nigunim', searchText);
            if (found) {
                const displayId = found.customId || found.name;
                return `#/nigunim/${encodeURIComponent(displayId)}`;
            }

            // Try to find in mechabrim by name
            found = findItemByIdOrName('mechabrim', searchText);
            if (found) {
                const displayId = found.customId || found.name;
                return `#/mechabrim/${encodeURIComponent(displayId)}`;
            }

            // Try to find in verter by name
            found = findItemByIdOrName('verter', searchText);
            if (found) {
                const displayId = found.customId || found.name;
                return `#/verter/${encodeURIComponent(displayId)}`;
            }

            // Try to find in chatzeros by name
            found = findItemByIdOrName('chatzeros', searchText);
            if (found) {
                const displayId = found.customId || found.name;
                return `#/chatzeros/${encodeURIComponent(displayId)}`;
            }

            // If we can't find a match, return null to use the original link
            return null;
        }

        // Format description text (handle markdown-like formatting)
        function formatDescription(text) {
            if (!text) return '';

            // Convert markdown links [text](url) to HTML, with special handling for Coda links
            text = text.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (match, linkText, url) => {
                // Try to convert Coda links to site links
                const siteLink = convertCodaLinkToSiteLink(linkText, url);
                if (siteLink) {
                    return `<a href="${siteLink}">${linkText}</a>`;
                }
                // For non-Coda links or unmatched Coda links, use target="_blank"
                return `<a href="${url}" target="_blank">${linkText}</a>`;
            });

            // Convert **bold** to <strong>
            text = text.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');

            // Convert ## headers to <h4>
            text = text.replace(/^## (.+)$/gm, '<h4>$1</h4>');

            // Convert newlines to <br> for proper display
            text = text.replace(/\n/g, '<br>');

            return text;
        }

        // Format verter (lyrics) text with proper line breaks and styling
        function formatVerter(text) {
            if (!text) return '';

            // Clean up the text
            text = text.replace(/```/g, '').trim();

            // Split into lines and wrap each in a span for styling
            const lines = text.split('\n');

            return lines.map(line => {
                const trimmedLine = line.trim();
                if (!trimmedLine) return '<div class="verter-line verter-empty"></div>';
                return `<div class="verter-line">${trimmedLine}</div>`;
            }).join('');
        }

        // Render zmanim page with zmanim as main items
        function renderZmanimPage(container) {
            const data = categories.zmanim;

            // Build zmanim order
            const zmanOrder = ['שבת', 'מוצאי שבת', 'ראש חודש', 'ימים נוראים', 'סוכות', 'שמחת תורה',
                'שמחת בית השואבה', 'חנוכה', 'פורים', 'פסח', 'שבועות', 'בין המצרים',
                'ל"ג בעומר', 'שלש רגלים', 'חתונה', 'שבע ברכות', 'ברית מילה',
                'בר מצוה', 'קבלת פנים - חופה', 'הכנסת ספר תורה', 'סיום',
                'שבת שירה', 'שבת שקלים', 'יארצייט'];

            // Add any zmanim not in the predefined order
            Object.keys(zmaninInfo).forEach(z => {
                if (!zmanOrder.includes(z)) {
                    zmanOrder.push(z);
                }
            });

            // Filter to only zmanim that exist
            const activeZmanim = zmanOrder.filter(z => zmaninInfo[z]);

            // Count totals
            const totalZmanim = activeZmanim.length;

            // Build HTML
            let html = `
                <div class="page-theme theme-zman">
                    <div class="page-title">
                        <div class="page-title-bar">זמנים</div>
                        <div class="page-title-content">
                            <div class="subtitle"><span class="subtitle-count">0</span> זמנים</div>
                            <div class="loading-wave-bar active" id="zmanLoader"></div>
                        </div>
                    </div>
                    
                    <div class="category-grid">
            `;

            // Show each zman as a card
            activeZmanim.forEach(zmanName => {
                const zmanData = zmaninInfo[zmanName];
                if (!zmanData) return;

                const piyutimCount = zmanData.piyutim?.length || 0;
                const collectionsCount = zmanData.collections?.length || 0;

                const zmanId = zmanData.rowId || zmanName;
                html += `
                    <a class="category-card" href="#/zman/${encodeURIComponent(zmanId)}" data-action="zman-detail" data-name="${escapeHtml(zmanName)}">
                        <div class="card-header">
                            <div class="card-icon"></div>
                            <div class="card-header-text">
                                <div class="card-title">${zmanName}</div>
                                <div class="card-stats">
                                    ${piyutimCount > 0 ? `<span class="card-stat">${piyutimCount} פיוטים</span>` : ''}
                                    ${collectionsCount > 0 ? `<span class="card-stat">${collectionsCount} קאלעקשאנס</span>` : ''}
                                </div>
                            </div>
                        </div>
                    </a>
                `;
            });

            html += `</div></div>`;

            container.innerHTML = html;

            const countEl = container.querySelector('.subtitle-count');
            if (countEl) animateCountUp(countEl, totalZmanim);

            // Hide loading wave bar
            const loader = document.getElementById('zmanLoader');
            if (loader) setTimeout(() => loader.classList.remove('active'), 500);
        }

        // Navigate to zman detail page
        function navigateToZmanDetail(name) {
            const infoObj = Object.assign({}, zmaninInfo, piyutimInfo);
            const id = infoObj[name]?.rowId;
            const customId = infoObj[name]?.customId;
            currentDetailView = { type: 'zman', name: name, id: id, customId: customId };
            updateURL();
            renderCurrentPage();
        }

        // Render nigun detail page
        function renderNigunDetailPage(container, nigunName) {
            // Find the song by name, rowId, or customId
            let songIdx = allSongs.findIndex(s => s.name === nigunName);
            if (songIdx === -1) {
                // Try finding by rowId
                songIdx = allSongs.findIndex(s => s.rowId === nigunName);
            }
            if (songIdx === -1) {
                // Try finding by customId
                songIdx = allSongs.findIndex(s => s.customId === nigunName);
            }
            const song = allSongs[songIdx];

            if (!song) {
                container.innerHTML = '<p>ניגון נישט געפונען</p>';
                return;
            }

            // Parse chatzeros
            const chatzeros = song.category ? song.category.split(',').map(c => c.trim()).filter(Boolean) : [];

            // Get mechaber info for image
            const mechaberName = song.mechaber ? song.mechaber.split(',')[0].trim() : null;
            const mechaberInfo = mechaberName ? mechabrimInfo[mechaberName] : null;

            // Build music details tags (scale, ritem, pasigOif, maure, collections)
            const musicTags = [];
            if (song.scale) musicTags.push({ value: song.scale, type: 'scale', icon: '' });
            if (song.ritem) musicTags.push({ value: song.ritem, type: 'ritem', icon: '' });
            if (song.pasigOif) musicTags.push({ value: song.pasigOif, type: 'zman', icon: '' });
            if (song.maure) musicTags.push({ value: song.maure, type: 'maure', icon: '' });
            if (song.collections) musicTags.push({ value: song.collections, type: 'collection', icon: '' });

            container.innerHTML = `
                <div class="detail-page nigun-detail-page">
                    <button class="back-button" onclick="navigateTo('nigunim')">
                        → ניגונים בלאט
                    </button>
                    
                    <div class="detail-header">
                        <div class="detail-category-bar" style="background: var(--color-nigun);">ניגון</div>
                        <div class="detail-header-content no-image">
                            <div class="detail-header-text">
                                <h1 class="detail-title" style="color: var(--color-nigun-dark);">${song.name}</h1>
                                ${chatzeros.length > 0 ? `
                                    <div class="nigun-chatzer-tags">
                                        ${chatzeros.map(chatzer => {
                const cInfo = chatzerosInfo[chatzer] || {};
                const cId = cInfo.customId || chatzer;
                return `<a class="nigun-tag chatzer-tag" href="#/chatzeros/${encodeURIComponent(cId)}" data-action="detail" data-category="chatzeros" data-name="${escapeHtml(chatzer)}">${chatzer}</a>`;
            }).join('')}
                                    </div>
                                ` : ''}
                            </div>
                        </div>
                    </div>
                    
                    ${song.mechaber ? `
                        <div class="nigun-mechaber-section">
                            <div class="nigun-mechaber-title">מחבר הניגון</div>
                            <div class="nigun-mechaber-grid">
                                ${song.mechaber.split(',').map(m => m.trim()).filter(Boolean).map(mName => {
                const mInfo = mechabrimInfo[mName];
                const mId = mInfo?.customId || mName;
                return `
                                        <a class="nigun-mechaber-card" href="#/mechabrim/${encodeURIComponent(mId)}" data-action="detail" data-category="mechabrim" data-name="${escapeHtml(mName)}">
                                            ${mInfo?.image
                        ? `<img class="nigun-mechaber-image" src="${mInfo.image}" alt="${mName}">`
                        : `<div class="nigun-mechaber-placeholder"><div class="mechaber-icon"></div></div>`
                    }
                                            <div class="nigun-mechaber-name">${mName}</div>
                                        </a>
                                    `;
            }).join('')}
                            </div>
                        </div>
                    ` : ''}

                    ${song.personality ? `
                        <div class="nigun-personality-section">
                            <div class="nigun-personality-title">פערזענליכקייטן וואס האבן א שייכות מיט דעם ניגון</div>
                            <div class="nigun-mechaber-grid">
                                ${song.personality.split(',').map(p => p.trim()).filter(Boolean).map(personName => {
                const pInfo = mechabrimInfo[personName];
                const pId = pInfo?.customId || personName;
                return `
                                        <a class="nigun-mechaber-card" href="#/mechabrim/${encodeURIComponent(pId)}" data-action="detail" data-category="mechabrim" data-name="${escapeHtml(personName)}">
                                            ${pInfo?.image
                        ? `<img class="nigun-mechaber-image" src="${pInfo.image}" alt="${personName}">`
                        : `<div class="nigun-mechaber-placeholder"><div class="mechaber-icon"></div></div>`
                    }
                                            <div class="nigun-mechaber-name">${personName}</div>
                                        </a>
                                    `;
            }).join('')}
                            </div>
                        </div>
                    ` : ''}

                    <div class="nigun-details-card">
                        ${song.siman ? `
                            <div class="nigun-detail-row">
                                <span class="nigun-detail-label">סימן:</span>
                                <span class="nigun-detail-value">${formatDescription(song.siman)}</span>
                            </div>
                        ` : ''}
                        ${song.verter ? `
                            <div class="nigun-detail-row">
                                <span class="nigun-detail-label">ווערטער:</span>
                                <span class="nigun-detail-value nigun-tags-row">
                                    ${song.verter.split(',').map(v => v.trim()).filter(Boolean).map(v => `
                                        <span class="nigun-tag-inline tag-verter"
                                              data-action="detail"
                                              data-category="verter"
                                              data-name="${escapeHtml(v)}"
                                              data-tooltip-value="${escapeHtml(v)}"
                                              data-tooltip-type="verter">${v}</span>
                                    `).join('')}
                                </span>
                            </div>
                        ` : ''}
                        ${song.scale ? `
                            <div class="nigun-detail-row">
                                <span class="nigun-detail-label">סקעיל:</span>
                                <span class="nigun-detail-value">
                                    <span class="nigun-tag-inline tag-scale">${song.scale}</span>
                                </span>
                            </div>
                        ` : ''}
                        ${song.ritem ? `
                            <div class="nigun-detail-row">
                                <span class="nigun-detail-label">ריטעם:</span>
                                <span class="nigun-detail-value">
                                    <span class="nigun-tag-inline tag-ritem">${song.ritem}</span>
                                </span>
                            </div>
                        ` : ''}
                        ${song.maure ? `
                            <div class="nigun-detail-row">
                                <span class="nigun-detail-label">ווערט געזונגען בחצה"ק אויף:    </span>
                                <span class="nigun-detail-value nigun-tags-row">
                                    ${song.maure.split(',').map(m => m.trim()).filter(Boolean).map(m => `
                                        <span class="nigun-tag-inline tag-piyut"
                                              data-action="filter"
                                              data-page="nigunim"
                                              data-filter-key="zman"
                                              data-value="${escapeHtml(m)}"
                                              data-tooltip-value="${escapeHtml(m)}"
                                              data-tooltip-type="zman">${m}</span>
                                    `).join('')}
                                </span>
                            </div>
                        ` : ''}
                        ${song.pasigOif ? `
                            <div class="nigun-detail-row">
                                <span class="nigun-detail-label">איז פאסיג צו זינגען אויף:    </span>
                                <span class="nigun-detail-value nigun-tags-row">
                                    ${song.pasigOif.split(',').map(p => p.trim()).filter(Boolean).map(p => `
                                        <span class="nigun-tag-inline tag-piyut"
                                              data-action="filter"
                                              data-page="nigunim"
                                              data-filter-key="zman"
                                              data-value="${escapeHtml(p)}"
                                              data-tooltip-value="${escapeHtml(p)}"
                                              data-tooltip-type="zman">${p}</span>
                                    `).join('')}
                                </span>
                            </div>
                        ` : ''}
                        ${song.collections ? `
                            <div class="nigun-detail-row">
                                <span class="nigun-detail-label">קאלעקשאן:</span>
                                <span class="nigun-detail-value nigun-tags-row">
                                    ${song.collections.split(',').map(col => col.trim()).filter(Boolean).map(col => `
                                        <span class="nigun-tag-inline tag-collection"
                                              data-action="detail"
                                              data-category="collections"
                                              data-name="${escapeHtml(col)}"
                                              data-tooltip-value="${escapeHtml(col)}"
                                              data-tooltip-type="collection">${col}</span>
                                    `).join('')}
                                </span>
                            </div>
                        ` : ''}
                        ${song.albums ? `
                            <div class="nigun-detail-row">
                                <span class="nigun-detail-label">אלבומס:</span>
                                <span class="nigun-detail-value nigun-tags-row">
                                    ${song.albums.split(',').map(a => a.trim()).filter(Boolean).map(a => `
                                        <span class="nigun-tag-inline tag-album"
                                              data-action="detail"
                                              data-category="albums"
                                              data-name="${escapeHtml(a)}"
                                              style="cursor:pointer;">${a}</span>
                                    `).join('')}
                                </span>
                            </div>
                        ` : ''}
                        ${song.documents ? `
                            <div class="nigun-detail-row">
                                <span class="nigun-detail-label">דאקומענטן:</span>
                                <span class="nigun-detail-value nigun-tags-row">
                                    ${song.documents.split(',').map(d => d.trim()).filter(Boolean).map(d => `
                                        <span class="nigun-tag-inline tag-document"
                                              data-action="detail"
                                              data-category="documents"
                                              data-name="${escapeHtml(d)}"
                                              style="cursor:pointer;">${d}</span>
                                    `).join('')}
                                </span>
                            </div>
                        ` : ''}
                        ${song.resources ? `
                            <div class="nigun-detail-row">
                                <span class="nigun-detail-label">רעסורסן:</span>
                                <span class="nigun-detail-value nigun-tags-row">
                                    ${song.resources.split(',').map(r => r.trim()).filter(Boolean).map(r => `
                                        <span class="nigun-tag-inline tag-resource"
                                              data-action="detail"
                                              data-category="resources"
                                              data-name="${escapeHtml(r)}"
                                              style="cursor:pointer;">${r}</span>
                                    `).join('')}
                                </span>
                            </div>
                        ` : ''}
                    </div>
                    
                    ${song.info ? `
                        <div class="nigun-section">
                            <h3>אינפארמאציע</h3>
                            <div class="nigun-section-content">
                                <div class="nigun-info-text">${formatDescription(song.info)}</div>
                            </div>
                        </div>
                    ` : ''}
                    
                    ${song.notn ? `
                        <div class="nigun-section">
                            <h3>נאטן</h3>
                            <div class="nigun-section-content">
                                <a href="${song.notn}" target="_blank" class="nigun-notn-link">📄 קוק נאטן</a>
                            </div>
                        </div>
                    ` : ''}
                    
                    ${song.recordings && song.recordings.length > 0 ? `
                        <div class="nigun-section nigun-recordings-section">
                            <h3>רעקארדינגס (${song.recordings.length})</h3>
                            <div class="nigun-recordings-list">
                                ${song.recordings.map((rec, i) => {
                // Build meta parts with colored tags like songs page
                let metaHtml = '';
                if (rec.personalities) {
                    metaHtml += rec.personalities.split(',').map(p => '<span class="mechaber-tag song-meta-tag">' + p.trim() + '</span>').join(' ');
                }
                if (rec.album) {
                    if (metaHtml) metaHtml += ' ';
                    metaHtml += rec.album.split(',').map(a => '<span class="album-tag song-meta-tag">' + a.trim() + '</span>').join(' ');
                }

                // Determine display name: details, or personalities, or album, or "רעקארדינג X"
                const displayName = rec.details || rec.personalities || rec.album || ('רעקארדינג ' + (i + 1));
                const nameClass = (!rec.details && !rec.personalities && !rec.album) ? 'nigun-recording-name default-name' : 'nigun-recording-name';

                return '<div class="nigun-recording-item ' + (i === 0 ? 'active' : '') + '" onclick="playNigunRecording(' + songIdx + ', ' + i + ', this)" data-recording-idx="' + i + '" data-rating="' + (rec.rating || 0) + '">' +
                    '<div class="nigun-recording-number">' + (i + 1) + '</div>' +
                    '<div class="nigun-recording-info">' +
                    '<div class="' + nameClass + '">' + displayName + '</div>' +
                    (metaHtml ? '<div class="nigun-recording-meta">' + metaHtml + '</div>' : '') +
                    '</div>' +
                    '<button class="nigun-recording-play-btn">▶</button>' +
                    '</div>';
            }).join('')}
                            </div>
                        </div>
                    ` : ''}

                    ${(song.otherMedia && (song.otherMedia.text || song.otherMedia.audio.length > 0 || song.otherMedia.images.length > 0 || song.otherMedia.videos.length > 0 || song.otherMedia.pdfs.length > 0 || song.otherMedia.files.length > 0)) ? `
                        <div class="nigun-section nigun-other-media-section">
                            <h3>צוגאבן</h3>
                            <div class="nigun-other-media-content">
                                ${song.otherMedia.text ? `
                                    <div class="nigun-other-media-text">${formatDescription(song.otherMedia.text)}</div>
                                ` : ''}

                                ${song.otherMedia.audio.length > 0 ? `
                                    <div class="nigun-recordings-list">
                                        ${song.otherMedia.audio.map((aud, i) => `
                                            <div class="nigun-recording-item" onclick="playOtherMediaAudio('${aud.url}', this)">
                                                <div class="nigun-recording-number">♫</div>
                                                <div class="nigun-recording-info">
                                                    <div class="nigun-recording-name">${aud.name}</div>
                                                </div>
                                                <button class="nigun-recording-play-btn">▶</button>
                                            </div>
                                        `).join('')}
                                    </div>
                                ` : ''}

                                ${song.otherMedia.files.length > 0 ? `
                                    <div class="nigun-files-list">
                                        ${song.otherMedia.files.map((file, i) => `
                                            <a href="${file.url}" target="_blank" class="nigun-file-item">
                                                <div class="nigun-file-icon">📎</div>
                                                <div class="nigun-file-name">${file.name}</div>
                                                <div class="nigun-file-download">הורדה</div>
                                            </a>
                                        `).join('')}
                                    </div>
                                ` : ''}

                                ${(song.otherMedia.images.length > 0 || song.otherMedia.videos.length > 0 || song.otherMedia.pdfs.length > 0) ? `
                                    <div class="nigun-other-media-grid">
                                        ${song.otherMedia.images.map((img, i) => `
                                            <div class="nigun-image-item" onclick="openImageModal('${img.url}')">
                                                <img src="${img.url}" alt="${img.name}" loading="lazy">
                                                <div class="nigun-image-name">${img.name}</div>
                                            </div>
                                        `).join('')}

                                        ${song.otherMedia.videos.map((vid, i) => `
                                            <div class="nigun-video-item" onclick="openVideoModal('${vid.url}')">
                                                <video src="${vid.url}" preload="metadata"></video>
                                                <div class="nigun-video-play-overlay">
                                                    <div class="nigun-video-play-icon">▶</div>
                                                </div>
                                                <div class="nigun-video-name">${vid.name}</div>
                                            </div>
                                        `).join('')}

                                        ${song.otherMedia.pdfs.map((pdf, i) => `
                                            <div class="nigun-pdf-preview" onclick="window.open('${pdf.url}', '_blank')">
                                                <div class="nigun-pdf-preview-box">
                                                    <div class="nigun-pdf-preview-icon">📄</div>
                                                    <div class="nigun-pdf-preview-label">PDF</div>
                                                </div>
                                                <div class="nigun-pdf-preview-name">${pdf.name}</div>
                                            </div>
                                        `).join('')}
                                    </div>
                                ` : ''}
                            </div>
                        </div>
                    ` : ''}
                </div>
            `;
        }

        // Play recording from nigun detail page
        function playNigunRecording(songIdx, recordingIdx, element, playlist = null) {
            const song = allSongs[songIdx];
            if (!song || !song.recordings || !song.recordings[recordingIdx]) return;

            const recording = song.recordings[recordingIdx];

            // If clicking the same recording that's currently playing, toggle play/pause
            if (songIdx === currentSongIndex && recordingIdx === currentRecordingIndex && audioPlayer.src) {
                togglePlayPause();
                return;
            }

            // Update current song and recording index
            currentSongIndex = songIdx;
            currentRecordingIndex = recordingIdx;

            // Update playlist if provided, or if song not in current playlist
            if (playlist && Array.isArray(playlist)) {
                currentPlaylist = playlist;
                currentPlaylistPosition = currentPlaylist.indexOf(songIdx);
            } else if (currentPlaylist.length === 0 || !currentPlaylist.includes(songIdx)) {
                // If no playlist set or song not in current playlist, use filteredSongs
                currentPlaylist = filteredSongs.map(s => allSongs.indexOf(s));
                currentPlaylistPosition = currentPlaylist.indexOf(songIdx);
            } else {
                // Song is in current playlist, just update position
                currentPlaylistPosition = currentPlaylist.indexOf(songIdx);
            }

            // Set recording info for rating updates
            currentPlayingRecordingInfo = {
                songIdx: songIdx,
                recordingIdx: recordingIdx,
                slotIndex: recording.slotIndex,
                rowId: song.rowId
            };

            // Enable recordings mode when playing any recording
            isInRecordingsMode = true;

            // Play the specific recording URL
            audioPlayer.src = recording.url;
            audioPlayer.play();

            showPlayer();
            // Update player UI with song info
            updatePlayerUI(song);
            updateActiveSongInList();

            // Update rating display in player
            updatePlayerRating(recording.rating);

            // Update active state in UI (for nigun detail modal)
            document.querySelectorAll('.nigun-recording-item').forEach((item, i) => {
                item.classList.toggle('active', i === recordingIdx);
            });

            // Update active recording state in song list (recordings mode)
            document.querySelectorAll('.song-item-recording').forEach(rec => {
                rec.classList.remove('active-recording');
            });
            document.querySelectorAll('.rec-play-btn').forEach(btn => {
                btn.classList.remove('playing');
            });
            const activeRecRow = document.querySelector(`.song-item-recording[data-song-idx="${songIdx}"][data-rec-idx="${recordingIdx}"]`);
            if (activeRecRow) {
                activeRecRow.classList.add('active-recording');
            }
            const activeRecBtn = document.querySelector(`.rec-play-btn[data-song-idx="${songIdx}"][data-rec-idx="${recordingIdx}"]`);
            if (activeRecBtn) {
                activeRecBtn.classList.add('playing');
            }
        }

        // Render zman detail page
        function renderZmanDetailPage(container, zmanName) {
            const zmanData = zmaninInfo[zmanName];
            if (!zmanData) {
                container.innerHTML = '<p>זמן נישט געפונען</p>';
                return;
            }

            const piyutim = zmanData.piyutim || [];
            const collections = zmanData.collections || [];

            let html = `
                <div class="detail-page theme-zman">
                    <button class="back-button" onclick="navigateTo('zmanim')">
                        → זמנים בלאט
                    </button>
                    
                    <div class="detail-header">
                        <div class="detail-category-bar">זמן</div>
                        <div class="detail-header-content">
                            <div class="detail-header-text">
                                <div class="detail-title">${zmanName}</div>
                                <div class="detail-subtitle">
                                    ${collections.length > 0 ? `<span>${collections.length} קאלעקשאנס</span>` : ''}
                                    ${piyutim.length > 0 && collections.length > 0 ? '<span class="detail-separator">•</span>' : ''}
                                    ${piyutim.length > 0 ? `<span>${piyutim.length} פיוטים</span>` : ''}
                                </div>
                            </div>
                        </div>
                    </div>
            `;

            // Collections section first
            if (collections.length > 0) {
                html += `
                    <div class="zman-section section-collections">
                        <h3>קאלעקשאנס</h3>
                        <div class="zman-items-grid">
                            ${collections.sort((a, b) => a.localeCompare(b, 'he')).map(colName => {
                    const songCount = categories.collections[colName]?.length || 0;
                    const colInfo = collectionsInfo[colName] || {};
                    const colId = colInfo.customId || colName;
                    return `
                                    <a class="zman-item-card" href="#/collections/${encodeURIComponent(colId)}" data-action="detail" data-category="collections" data-name="${escapeHtml(colName)}">
                                        <div class="zman-item-icon"></div>
                                        <div class="zman-item-info">
                                            <div class="zman-item-name">${colName}</div>
                                            ${songCount > 0 ? `<div class="zman-item-count">${songCount} ניגונים</div>` : ''}
                                        </div>
                                    </a>
                                `;
                }).join('')}
                        </div>
                    </div>
                `;
            }

            // Piyutim section
            if (piyutim.length > 0) {
                html += `
                    <div class="zman-section section-piyutim">
                        <h3>פיוטים</h3>
                        <div class="zman-items-grid">
                            ${piyutim.sort((a, b) => a.localeCompare(b, 'he')).map(piyutName => {
                    const songCount = categories.zmanim[piyutName]?.length || 0;
                    const piyutInfo = piyutimInfo[piyutName] || {};
                    const piyutId = piyutInfo.customId || piyutName;
                    return `
                                    <a class="zman-item-card" href="#/zmanim/${encodeURIComponent(piyutId)}" data-action="detail" data-category="zmanim" data-name="${escapeHtml(piyutName)}">
                                        <div class="zman-item-icon"></div>
                                        <div class="zman-item-info">
                                            <div class="zman-item-name">${piyutName}</div>
                                            ${songCount > 0 ? `<div class="zman-item-count">${songCount} ניגונים</div>` : ''}
                                        </div>
                                    </a>
                                `;
                }).join('')}
                        </div>
                    </div>
                `;
            }

            html += `</div>`;

            container.innerHTML = html;
        }

        // Render collections page with zmanim tags
        function renderCollectionsPage(container) {
            const data = categories.collections;
            const sortedKeys = Object.keys(data).sort((a, b) => a.localeCompare(b, 'he'));

            container.innerHTML = `
                <div class="page-theme theme-collection">
                    <div class="page-title">
                        <div class="page-title-bar">קאלעקשאנס</div>
                        <div class="page-title-content">
                            <div class="subtitle"><span class="subtitle-count">0</span> קאלעקשאנס</div>
                            <div class="loading-wave-bar active" id="collectionLoader"></div>
                        </div>
                    </div>

                    <div class="collections-grid">
                        ${sortedKeys.map(key => {
                const info = collectionsInfo[key] || {};
                const songCount = data[key].length;
                const zmanim = info.zmanim || [];
                const songIndices = data[key] || [];
                const playlistIndices = songIndices.map(idx => idx);

                return `
                            <div class="collection-card">
                                <div class="collection-card-header" data-action="detail" data-category="collections" data-name="${escapeHtml(key)}">
                                    <div class="collection-icon">♫</div>
                                    <div class="collection-info">
                                        <div class="collection-title">${key}</div>
                                        <div class="collection-meta">${songCount} ניגונים</div>
                                        ${zmanim.length > 0 ? `
                                            <div class="collection-zmanim" onclick="event.stopPropagation();">
                                                ${zmanim.slice(0, 3).map(zman => `<span class="collection-zman-tag"
                                                    data-action="zman-detail"
                                                    data-name="${escapeHtml(zman)}"
                                                    data-tooltip-value="${escapeHtml(zman)}"
                                                    data-tooltip-type="zman">${zman}</span>`).join('')}
                                                ${zmanim.length > 3 ? `<span class="collection-zman-more">+${zmanim.length - 3}</span>` : ''}
                                            </div>
                                        ` : ''}
                                    </div>
                                    <button class="collection-play-btn" data-action="play-collection" data-name="${escapeHtml(key)}">
                                        ▶
                                    </button>
                                </div>
                            </div>
                        `}).join('')}
                    </div>
                </div>
            `;

            const countEl = container.querySelector('.subtitle-count');
            if (countEl) animateCountUp(countEl, sortedKeys.length);

            // Hide loading wave bar
            const loader = document.getElementById('collectionLoader');
            if (loader) setTimeout(() => loader.classList.remove('active'), 500);
        }

        // Render piyutim page
        function renderPiyutimPage(container) {
            // Get all piyutim from piyutimInfo
            const sortedPiyutim = Object.keys(piyutimInfo).sort((a, b) => a.localeCompare(b, 'he'));

            container.innerHTML = `
                <div class="page-theme theme-piyut">
                    <div class="page-title">
                        <div class="page-title-bar">פיוטים</div>
                        <div class="page-title-content">
                            <div class="subtitle"><span class="subtitle-count">0</span> פיוטים</div>
                            <div class="loading-wave-bar active" id="piyutimLoader"></div>
                        </div>
                    </div>

                    <div class="category-grid" id="piyutimGrid">
                        ${sortedPiyutim.map(piyutName => {
                const info = piyutimInfo[piyutName] || {};
                const songCount = categories.zmanim[piyutName]?.length || 0;
                const piyutId = info.customId || piyutName;
                return `
                            <a class="category-card piyut-card" href="#/piyutim/${encodeURIComponent(piyutId)}" data-action="detail" data-category="piyutim" data-name="${escapeHtml(piyutName)}">
                                <div class="category-card-content">
                                    <div class="category-card-info">
                                        <div class="category-card-title">${piyutName}</div>
                                        ${songCount > 0 ? `<div class="category-card-count">${songCount} ניגונים</div>` : ''}
                                    </div>
                                </div>
                            </a>
                        `;
            }).join('')}
                    </div>
                </div>
            `;

            const countEl = container.querySelector('.subtitle-count');
            if (countEl) animateCountUp(countEl, sortedPiyutim.length);

            // Hide loading wave bar
            const loader = document.getElementById('piyutimLoader');
            if (loader) setTimeout(() => loader.classList.remove('active'), 500);
        }

        // Render music page (scale & rhythm)
        function renderMusicPage(container) {
            const scaleData = categories.scale;
            const ritemData = categories.ritem;
            const sortedScales = Object.keys(scaleData).sort((a, b) => scaleData[b].length - scaleData[a].length);
            const sortedRitems = Object.keys(ritemData).sort((a, b) => ritemData[b].length - ritemData[a].length);

            // Build structure: for each ritem, group songs by scale
            const ritemGroups = {};
            sortedRitems.forEach(ritem => {
                ritemGroups[ritem] = {};
                const songIndices = ritemData[ritem] || [];
                songIndices.forEach(idx => {
                    const song = allSongs[idx];
                    if (song) {
                        const scale = song.scale || 'אנדערע';
                        if (!ritemGroups[ritem][scale]) {
                            ritemGroups[ritem][scale] = [];
                        }
                        ritemGroups[ritem][scale].push(idx);
                    }
                });
            });

            container.innerHTML = `
                <div class="page-theme theme-music">
                    <div class="page-title">
                        <div class="page-title-bar" style="background: var(--color-music);">מוזיק</div>
                        <div class="page-title-content">
                            <div class="subtitle">ניגונים לויט ריטעם און סקעיל</div>
                            <div class="loading-wave-bar active" id="musicLoader"></div>
                        </div>
                    </div>

                    <!-- Rhythm Groups -->
                    <div class="music-ritem-groups" id="musicRitemGroups">
                        ${sortedRitems.map(ritem => {
                const scalesInRitem = Object.keys(ritemGroups[ritem]).sort((a, b) =>
                    (ritemGroups[ritem][b]?.length || 0) - (ritemGroups[ritem][a]?.length || 0)
                );
                const totalInRitem = ritemData[ritem].length;

                return `
                                <div class="ritem-group" data-ritem="${ritem}">
                                    <div class="ritem-group-header">
                                        <h3>${ritem}</h3>
                                        <span class="ritem-count">${totalInRitem} ניגונים</span>
                                        <button class="ritem-play-btn" data-action="play-music" data-category-type="ritem" data-name="${escapeHtml(ritem)}">▶ שפיל אלע</button>
                                    </div>
                                    <div class="ritem-scales">
                                        ${scalesInRitem.map(scale => {
                    const songsInScale = ritemGroups[ritem][scale] || [];
                    return `
                                                <div class="scale-group" data-scale="${scale}">
                                                    <div class="scale-group-header">
                                                        <span class="scale-name">${scale}</span>
                                                        <span class="scale-song-count">${songsInScale.length}</span>
                                                    </div>
                                                    <div class="scale-songs">
                                                        ${songsInScale.slice(0, 10).map((songIdx, i) => {
                        const song = allSongs[songIdx];
                        const hasAudio = song.audioUrl && song.audioUrl.startsWith('http');
                        return `
                                                                <div class="music-song-item ${!hasAudio ? 'no-audio' : ''}" 
                                                                     onclick="${hasAudio ? `playSong(${songIdx})` : `showSongDetails(${songIdx})`}">
                                                                    <span class="music-song-name">${song.name}</span>
                                                                    ${song.mechaber ? `<span class="music-song-meta">${song.mechaber}</span>` : ''}
                                                                    <button class="music-song-details" onclick="event.stopPropagation(); showSongDetails(${songIdx});">◉</button>
                                                                </div>
                                                            `;
                    }).join('')}
                                                        ${songsInScale.length > 10 ? `
                                                            <div class="scale-more" data-action="scale-more" data-ritem="${escapeHtml(ritem)}" data-scale="${escapeHtml(scale)}">
                                                                + נאך ${songsInScale.length - 10} ניגונים...
                                                            </div>
                                                        ` : ''}
                                                    </div>
                                                </div>
                                            `;
                }).join('')}
                                    </div>
                                </div>
                            `;
            }).join('')}
                    </div>
                </div>
            `;

            // Hide loading wave bar
            setTimeout(() => {
                const loader = document.getElementById('musicLoader');
                if (loader) loader.classList.remove('active');
            }, 500);
        }

        // Show all songs for a scale in a ritem
        function showAllScaleSongs(ritem, scale) {
            // Navigate to songs page with both filters
            activeFilters.ritem = [ritem];
            activeFilters.scale = [scale];
            applyFilters();
            navigateTo('nigunim');
        }

        // Lazy loading state for categories
        let categoryLazyState = {
            currentIndex: 0,
            batchSize: 50,
            allKeys: [],
            isLoading: false,
            isInitialLoad: true  // Track if this is the first batch (for animations)
        };

        // Render a single category card
        function renderCategoryCard(key, categoryKey, icon, infoObj, data) {
            const info = infoObj[key] || {};
            const hasImage = !!info.image;
            const songCount = data[key].length;
            const itemId = info.customId || key;

            // Only show chatzer tags for mechabrim
            const chatzeros = (categoryKey === 'mechabrim' && info.chatzer)
                ? info.chatzer.split(',').map(c => c.trim()).filter(Boolean)
                : [];

            // Count mechabrim for chatzeros
            let mechabrimCount = 0;
            if (categoryKey === 'chatzeros') {
                mechabrimCount = Object.entries(mechabrimInfo)
                    .filter(([mechName, mechInfo]) => {
                        if (!mechInfo.chatzer) return false;
                        const mechChatzeros = mechInfo.chatzer.split(',').map(c => c.trim());
                        return mechChatzeros.includes(key);
                    }).length;
            }

            return `
            <a class="category-card ${hasImage ? 'has-image' : ''}" href="#/${categoryKey}/${encodeURIComponent(itemId)}" data-action="detail" data-category="${categoryKey}" data-name="${escapeHtml(key)}">
                <div class="card-header">
                    ${hasImage ? `
                        <img class="card-image" src="${info.image}" alt="${key}" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                        <div class="card-icon" style="display:none;">${icon}</div>
                    ` : `
                        <div class="card-icon">${icon}</div>
                    `}
                    <div class="card-header-text">
                        ${(() => {
                    if (categoryKey === 'mechabrim') {
                        // Build name display:
                        // If גערופן exists: formal name (small) + known name (big)
                        // If גערופן is empty: just show formal name with suffix
                        const formalParts = [info.title, info.firstName, info.lastName].filter(Boolean);
                        const formalName = formalParts.length > 0 ? formalParts.join(' ') : '';

                        // Build full dates
                        const birthParts = [info.birthDay, info.birthMonth, info.birthYear].filter(Boolean);
                        const deathParts = [info.deathDay, info.deathMonth, info.deathYear].filter(Boolean);
                        const birthDate = birthParts.join(' ');
                        const deathDate = deathParts.join(' ');
                        const dateStr = (birthDate || deathDate) ? `${birthDate || '?'} - ${deathDate || ''}` : '';

                        let titleHtml;
                        if (info.gerufen) {
                            // Has גערופן - show two lines: formal (small) + known (big)
                            const knownParts = [info.gerufen, info.platz, info.suffix].filter(Boolean);
                            const knownName = knownParts.join(' ');
                            titleHtml = `<span class="card-mechaber-formal">${formalName}</span><span class="card-mechaber-known">${knownName}</span>`;
                        } else {
                            // No גערופן - show formal name with suffix in one line (still use mechaber color)
                            const fullNameParts = [info.title, info.firstName, info.lastName, info.platz, info.suffix].filter(Boolean);
                            const fullName = fullNameParts.length > 0 ? fullNameParts.join(' ') : key;
                            titleHtml = `<span class="card-mechaber-known">${fullName}</span>`;
                        }

                        return `
                                    <div class="card-title">${titleHtml}</div>
                                    ${dateStr ? `<div class="card-years">${dateStr}</div>` : ''}
                                `;
                    } else {
                        return `<div class="card-title">${key}</div>`;
                    }
                })()}
                        ${chatzeros.length > 0 ? `
                            <div class="card-tags" onclick="event.stopPropagation();">
                                ${chatzeros.map(chatzer => `
                                    <span class="card-chatzer-tag"
                                          data-action="filter"
                                          data-page="chatzeros"
                                          data-filter-key="chatzer"
                                          data-value="${escapeHtml(chatzer)}"
                                          data-tooltip-value="${escapeHtml(chatzer)}"
                                          data-tooltip-type="chatzer">${chatzer}</span>
                                `).join('')}
                            </div>
                        ` : ''}
                        <div class="card-stats">
                            ${categoryKey === 'chatzeros' && mechabrimCount > 0 ? `<span class="card-stat">${mechabrimCount} מחברים</span>` : ''}
                            <span class="card-stat">${songCount} ניגונים</span>
                        </div>
                    </div>
                </div>
            </a>
        `;
        }

        // Update category page cards incrementally (when categories grow as songs load)
        function updateCategoryPageCards() {
            const grid = document.getElementById('categoryGrid');
            if (!grid) return;

            const categoryKey = currentPageView;
            const data = categories[categoryKey];
            if (!data) return;

            const sortedKeys = Object.keys(data).sort((a, b) => a.localeCompare(b, 'he'));

            // Get existing card keys
            const existingKeys = new Set();
            grid.querySelectorAll('.category-card').forEach(card => {
                const key = card.dataset.name;
                if (key) existingKeys.add(key);
            });

            // Find new keys that don't have cards yet
            const newKeys = sortedKeys.filter(key => !existingKeys.has(key));
            if (newKeys.length === 0) return;

            // Get info object and icon for this category
            let infoObj = {};
            let icon = '';
            switch (categoryKey) {
                case 'chatzeros': infoObj = chatzerosInfo; icon = '🏛️'; break;
                case 'mechabrim': infoObj = mechabrimInfo; icon = '✍️'; break;
                case 'verter': infoObj = verterInfo; icon = '📝'; break;
                case 'zmanim': infoObj = Object.assign({}, zmaninInfo, piyutimInfo); icon = '📅'; break;
                case 'collections': infoObj = collectionsInfo; icon = '📚'; break;
            }

            // Add new cards
            const html = newKeys.map(key => renderCategoryCard(key, categoryKey, icon, infoObj, data)).join('');
            grid.insertAdjacentHTML('beforeend', html);

            // Animate new cards
            const newCards = Array.from(grid.children).slice(-newKeys.length);
            newCards.forEach((card, i) => {
                card.style.animationDelay = `${i * 0.03}s`;
                card.classList.add('item-pop-in');
            });

            // Update count in header
            const countEl = document.querySelector('.subtitle-count');
            if (countEl) {
                const currentCount = parseInt(countEl.textContent) || 0;
                if (sortedKeys.length > currentCount) {
                    animateCountUp(countEl, sortedKeys.length);
                }
            }

            // Update lazy state
            categoryLazyState.allKeys = sortedKeys;

            // Show/hide wave bar based on whether all songs are loaded
            const loader = document.getElementById('categoryLoader');
            if (loader) {
                if (songsFullyLoaded) {
                    loader.classList.remove('active');
                } else {
                    loader.classList.add('active');
                }
            }
        }

        // Load more category cards (lazy loading)
        function loadMoreCategoryCards(categoryKey, icon, infoObj, data) {
            if (categoryLazyState.isLoading) return;
            if (categoryLazyState.currentIndex >= categoryLazyState.allKeys.length) return;

            categoryLazyState.isLoading = true;
            const grid = document.getElementById('categoryGrid');
            const existingCount = grid.children.length;
            const endIndex = Math.min(
                categoryLazyState.currentIndex + categoryLazyState.batchSize,
                categoryLazyState.allKeys.length
            );

            const batch = categoryLazyState.allKeys.slice(categoryLazyState.currentIndex, endIndex);
            const html = batch.map(key => renderCategoryCard(key, categoryKey, icon, infoObj, data)).join('');

            grid.insertAdjacentHTML('beforeend', html);
            categoryLazyState.currentIndex = endIndex;
            categoryLazyState.isLoading = false;

            // Apply animations to newly added cards
            requestAnimationFrame(() => {
                const newCards = Array.from(grid.children).slice(existingCount);
                newCards.forEach((card, i) => {
                    card.style.animationDelay = `${i * 0.04}s`;
                    // Use card-stagger for initial load, pop-in for subsequent loads
                    if (categoryLazyState.isInitialLoad) {
                        card.classList.add('card-stagger');
                    } else {
                        card.classList.add('item-pop-in');
                    }
                });
                // Mark initial load as complete after first batch
                if (categoryLazyState.isInitialLoad) {
                    categoryLazyState.isInitialLoad = false;
                }
            });

            console.log(`Loaded ${batch.length} cards (${categoryLazyState.currentIndex}/${categoryLazyState.allKeys.length})`);

            // Hide loading wave bar when all cards are loaded OR songs are fully loaded
            const allCardsLoaded = categoryLazyState.currentIndex >= categoryLazyState.allKeys.length;
            const loader = document.getElementById('categoryLoader');
            if (loader && (allCardsLoaded || songsFullyLoaded)) {
                loader.classList.remove('active');
            }
        }

        // Render category page with lazy loading
        function renderCategoryPage(container, categoryKey, title, icon, subtitle) {
            const data = categories[categoryKey];
            const sortedKeys = Object.keys(data).sort((a, b) => a.localeCompare(b, 'he'));

            // Reset lazy loading state
            categoryLazyState = {
                currentIndex: 0,
                batchSize: 50,
                allKeys: sortedKeys,
                isLoading: false,
                isInitialLoad: true
            };

            // Get the right info object and theme class for this category
            let infoObj = {};
            let themeClass = '';
            switch (categoryKey) {
                case 'chatzeros': infoObj = chatzerosInfo; themeClass = 'page-theme theme-chatzer'; break;
                case 'mechabrim': infoObj = mechabrimInfo; themeClass = 'page-theme theme-mechaber'; break;
                case 'verter': infoObj = verterInfo; themeClass = 'page-theme theme-verter'; break;
                case 'zmanim': infoObj = Object.assign({}, zmaninInfo, piyutimInfo); themeClass = 'page-theme theme-zman'; break;
                case 'collections': infoObj = collectionsInfo; themeClass = 'page-theme theme-collection'; break;
            }

            // Check if we're coming from skeleton (skeleton-grid exists)
            const existingSkeletonGrid = container.querySelector('.skeleton-grid');
            const existingCountEl = container.querySelector('.subtitle-count');

            if (existingSkeletonGrid && existingCountEl) {
                // Coming from skeleton - just update count and replace grid, keep header
                animateCountUp(existingCountEl, sortedKeys.length);

                // Replace skeleton-grid with category-grid
                const categoryGrid = document.createElement('div');
                categoryGrid.className = 'category-grid';
                categoryGrid.id = 'categoryGrid';
                existingSkeletonGrid.replaceWith(categoryGrid);

                // Add load more indicator
                const indicator = document.createElement('div');
                indicator.id = 'loadMoreIndicator';
                indicator.style.cssText = 'text-align: center; padding: 20px; color: var(--text-muted);';
                categoryGrid.after(indicator);

                // Show wave bar if large dataset
                if (sortedKeys.length > 50) {
                    const loader = document.getElementById('categoryLoader');
                    if (loader) loader.classList.add('active');
                }

                // Load first batch of cards
                loadMoreCategoryCards(categoryKey, icon, infoObj, data);

                // Setup infinite scroll for skeleton path too
                const observer = new IntersectionObserver((entries) => {
                    if (entries[0].isIntersecting) {
                        loadMoreCategoryCards(categoryKey, icon, infoObj, data);
                    }
                }, { rootMargin: '200px' });
                if (indicator) observer.observe(indicator);
            } else {
                // Fresh render - create everything
                container.innerHTML = `
                    <div class="${themeClass}">
                        <div class="page-title">
                            <div class="page-title-bar">${title}</div>
                            <div class="page-title-content">
                                <div class="subtitle"><span class="subtitle-count">0</span> ${title}</div>
                                <div class="loading-wave-bar category-loader" id="categoryLoader"></div>
                            </div>
                        </div>

                        <div class="category-grid" id="categoryGrid">
                        </div>
                        <div id="loadMoreIndicator" style="text-align: center; padding: 20px; color: var(--text-muted);"></div>
                    </div>
                `;

                // Animate subtitle count
                const countEl = container.querySelector('.subtitle-count');
                if (countEl) animateCountUp(countEl, sortedKeys.length);
            }

            // Show loading wave bar
            const loader = document.getElementById('categoryLoader');
            if (loader && sortedKeys.length > 50) {
                loader.classList.add('active');
            }

            // Load first batch
            loadMoreCategoryCards(categoryKey, icon, infoObj, data);

            // Setup infinite scroll
            const observer = new IntersectionObserver((entries) => {
                if (entries[0].isIntersecting) {
                    loadMoreCategoryCards(categoryKey, icon, infoObj, data);
                }
            }, { rootMargin: '200px' });

            const indicator = document.getElementById('loadMoreIndicator');
            if (indicator) observer.observe(indicator);
        }

        // OLD CODE BELOW - keeping structure for reference
        function renderResourcesPage(container) {
            const allKeys = Object.keys(resourcesInfo);

            // Group by sort type
            const groups = {};
            allKeys.forEach(key => {
                const info = resourcesInfo[key] || {};
                const sortType = (info.sort || info.allData?.['סארט'] || 'אנדערע').replace(/\`\`\`/g, '').trim();
                if (!groups[sortType]) groups[sortType] = [];
                groups[sortType].push({ key, info });
            });

            // Sort groups and items within groups
            const sortedGroupNames = Object.keys(groups).sort((a, b) => {
                if (a === 'אנדערע') return 1;
                if (b === 'אנדערע') return -1;
                return a.localeCompare(b, 'he');
            });

            let html = `
                <div class="page-theme theme-resource">
                    <div class="page-title">
                        <div class="page-title-bar">רעסורסן</div>
                        <div class="page-title-content">
                            <div class="subtitle"><span class="subtitle-count">0</span> רעסורסן</div>
                            <div class="loading-wave-bar active" id="resourceLoader"></div>
                        </div>
                    </div>
            `;

            sortedGroupNames.forEach(groupName => {
                const items = groups[groupName].sort((a, b) => a.key.localeCompare(b.key, 'he'));

                html += `
                    <div class="category-group">
                        <div class="category-group-header themed theme-resource">
                            <span class="category-group-title">${groupName}</span>
                            <span class="category-group-count">${items.length}</span>
                        </div>
                        <div class="resources-grid">
                `;

                items.forEach(({ key, info }) => {
                    // Extract link from various formats
                    let linkUrl = info.link;
                    if (!linkUrl && info.allData) {
                        const linkData = info.allData['לינק / נאמבער'] || info.allData['לינק'];
                        if (linkData) {
                            const urlMatch = linkData.match(/https?:\/\/[^\s\)]+/);
                            if (urlMatch) linkUrl = urlMatch[0];
                        }
                    }
                    const hasLink = linkUrl && linkUrl.startsWith('http');
                    const linkText = info.allData?.['לינק / נאמבער'] || '';
                    const isPhone = linkText && !linkText.includes('http') && linkText.match(/\d{3}/);
                    const phoneNumber = isPhone ? linkText.replace(/\`\`\`/g, '').trim() : '';

                    html += `
                        <div class="resource-card">
                            <div class="resource-card-header clickable-header" data-action="detail" data-category="resources" data-name="${escapeHtml(key)}">
                                <div class="resource-sort-label">${groupName}</div>
                            </div>
                            <div class="resource-card-body">
                                <div class="resource-title clickable-title" data-action="detail" data-category="resources" data-name="${escapeHtml(key)}">${key}</div>
                                ${info.chatzer ? `<div class="resource-chatzer"><span class="chatzer-tag" data-action="detail" data-category="chatzeros" data-name="${escapeHtml(info.chatzer)}" data-tooltip-value="${escapeHtml(info.chatzer)}" data-tooltip-type="chatzer">${info.chatzer}</span></div>` : ''}
                                ${info.description ? `<div class="resource-details">${formatDescription(info.description)}</div>` : ''}
                                ${info.notes ? `<div class="resource-notes">${formatDescription(info.notes)}</div>` : ''}
                            </div>
                            <div class="resource-card-footer">
                                ${hasLink ? `
                                    <a href="${linkUrl}" target="_blank" class="resource-link-btn">
                                        עפן לינק
                                    </a>
                                ` : isPhone ? `
                                    <div class="resource-phone-number">${phoneNumber}</div>
                                ` : '<div class="resource-btn-placeholder"></div>'}
                            </div>
                        </div>
                    `;
                });

                html += `
                        </div>
                    </div>
                `;
            });

            html += `</div>`;
            container.innerHTML = html;

            const countEl = container.querySelector('.subtitle-count');
            if (countEl) animateCountUp(countEl, allKeys.length);

            // Hide loading wave bar
            const loader = document.getElementById('resourceLoader');
            if (loader) setTimeout(() => loader.classList.remove('active'), 500);
        }

        // Render documents page
        function renderDocumentsPage(container) {
            const allKeys = Object.keys(documentsInfo);

            // Group by sort type
            const groups = {};
            allKeys.forEach(key => {
                const info = documentsInfo[key] || {};
                const sortType = (info.allData?.['סארט'] || info.sort || 'אנדערע').replace(/\`\`\`/g, '').trim();
                if (!groups[sortType]) groups[sortType] = [];
                groups[sortType].push({ key, info });
            });

            // Sort groups
            const sortedGroupNames = Object.keys(groups).sort((a, b) => {
                if (a === 'אנדערע') return 1;
                if (b === 'אנדערע') return -1;
                return a.localeCompare(b, 'he');
            });

            let html = `
                <div class="page-theme theme-document">
                    <div class="page-title">
                        <div class="page-title-bar">דאקומענטן</div>
                        <div class="page-title-content">
                            <div class="subtitle"><span class="subtitle-count">0</span> דאקומענטן</div>
                            <div class="loading-wave-bar active" id="documentLoader"></div>
                        </div>
                    </div>
            `;

            sortedGroupNames.forEach(groupName => {
                const items = groups[groupName].sort((a, b) => a.key.localeCompare(b.key, 'he'));

                html += `
                    <div class="category-group">
                        <div class="category-group-header themed theme-document">
                            <span class="category-group-title">${groupName}</span>
                            <span class="category-group-count">${items.length}</span>
                        </div>
                        <div class="documents-grid">
                `;

                items.forEach(({ key, info }) => {
                    const hasFile = info.file && info.file.startsWith('http');
                    const hasLink = info.link && info.link.startsWith('http');
                    const previewUrl = hasFile ? `https://docs.google.com/viewer?url=${encodeURIComponent(info.file)}&embedded=true` : '';

                    html += `
                        <div class="document-card">
                            <div class="document-card-header clickable-header" data-action="detail" data-category="documents" data-name="${escapeHtml(key)}">
                                <div class="document-sort-label">${groupName}</div>
                            </div>
                            ${hasFile ? `
                                <div class="document-preview">
                                    <iframe class="document-preview-frame" data-src="${previewUrl}" loading="lazy"></iframe>
                                    <div class="document-preview-overlay" onclick="openPdfPopup('${encodeURIComponent(info.file)}', '${encodeURIComponent(key)}')">
                                        <span class="preview-expand-icon">🔍</span>
                                    </div>
                                </div>
                            ` : ''}
                            <div class="document-card-body">
                                <div class="document-title clickable-title" data-action="detail" data-category="documents" data-name="${escapeHtml(key)}">${key}</div>
                                ${info.serie ? `<div class="document-serie">${info.serie.replace(/\`\`\`/g, '')}</div>` : ''}
                                ${info.chatzer ? `<div class="document-chatzer"><span class="chatzer-tag" data-action="detail" data-category="chatzeros" data-name="${escapeHtml(info.chatzer)}" data-tooltip-value="${escapeHtml(info.chatzer)}" data-tooltip-type="chatzer">${info.chatzer}</span></div>` : ''}
                                ${info.description ? `<div class="document-details">${formatDescription(info.description)}</div>` : ''}
                                ${info.notes ? `<div class="document-notes">${formatDescription(info.notes)}</div>` : ''}
                            </div>
                            <div class="document-card-footer">
                                ${hasLink ? `
                                    <a href="${info.link}" target="_blank" class="document-link-btn">
                                        לינק
                                    </a>
                                ` : '<div class="document-btn-placeholder"></div>'}
                            </div>
                        </div>
                    `;
                });

                html += `
                        </div>
                    </div>
                `;
            });

            html += `</div>`;
            container.innerHTML = html;

            const countEl = container.querySelector('.subtitle-count');
            if (countEl) animateCountUp(countEl, allKeys.length);

            // Hide loading wave bar
            const loader = document.getElementById('documentLoader');
            if (loader) setTimeout(() => loader.classList.remove('active'), 500);

            // Lazy load PDF previews when they become visible
            const observer = new IntersectionObserver((entries) => {
                entries.forEach(entry => {
                    if (entry.isIntersecting) {
                        const iframe = entry.target;
                        if (iframe.dataset.src && !iframe.src) {
                            iframe.src = iframe.dataset.src;
                        }
                        observer.unobserve(iframe);
                    }
                });
            }, { rootMargin: '100px' });

            document.querySelectorAll('.document-preview-frame').forEach(iframe => {
                observer.observe(iframe);
            });
        }

        // Render albums page
        function renderAlbumsPage(container) {
            const allKeys = Object.keys(albumsInfo);

            // Group by producer
            const groups = {};
            allKeys.forEach(key => {
                const info = albumsInfo[key] || {};
                const producer = (info.producer || 'אנדערע').replace(/\`\`\`/g, '').trim();
                if (!groups[producer]) groups[producer] = [];
                groups[producer].push({ key, info });
            });

            // Sort groups
            const sortedGroupNames = Object.keys(groups).sort((a, b) => {
                if (a === 'אנדערע') return 1;
                if (b === 'אנדערע') return -1;
                return a.localeCompare(b, 'he');
            });

            let html = `
                <div class="page-theme theme-album">
                    <div class="page-title">
                        <div class="page-title-bar">אלבומס</div>
                        <div class="page-title-content">
                            <div class="subtitle"><span class="subtitle-count">0</span> אלבומס</div>
                            <div class="loading-wave-bar active" id="albumLoader"></div>
                        </div>
                    </div>
            `;

            sortedGroupNames.forEach(groupName => {
                const items = groups[groupName].sort((a, b) => a.key.localeCompare(b.key, 'he'));

                html += `
                    <div class="category-group">
                        <div class="category-group-header themed theme-album">
                            <span class="category-group-title">${groupName}</span>
                            <span class="category-group-count">${items.length}</span>
                        </div>
                        <div class="albums-grid">
                `;

                items.forEach(({ key, info }) => {
                    const hasImage = info.cover || info.image;
                    const coverUrl = info.cover || info.image;
                    const hasBooklet = info.booklet && info.booklet.startsWith('http');
                    const hasBuyLink = info.whereToBuy && info.whereToBuy.startsWith('http');
                    const serie = info.serie?.replace(/\`\`\`/g, '') || '';
                    const notes = info.notes?.replace(/\`\`\`/g, '') || '';

                    html += `
                        <div class="album-card ${hasImage ? 'has-cover' : ''}">
                            <div class="album-card-header clickable-header" data-action="detail" data-category="albums" data-name="${escapeHtml(key)}">
                                <div class="album-producer-label">${groupName}</div>
                            </div>
                            ${hasImage ? `
                                <img class="album-cover clickable-cover" src="${coverUrl}" alt="${key}" data-action="detail" data-category="albums" data-name="${escapeHtml(key)}" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                                <div class="album-cover-placeholder clickable-cover" style="display:none;" data-action="detail" data-category="albums" data-name="${escapeHtml(key)}">♪</div>
                            ` : `
                                <div class="album-cover-placeholder clickable-cover" data-action="detail" data-category="albums" data-name="${escapeHtml(key)}">♪</div>
                            `}
                            <div class="album-info">
                                <div class="album-title clickable-title" data-action="detail" data-category="albums" data-name="${escapeHtml(key)}">${key}</div>
                                ${info.chatzer ? `<div class="album-chatzer"><span class="chatzer-tag" data-action="detail" data-category="chatzeros" data-name="${escapeHtml(info.chatzer)}" data-tooltip-value="${escapeHtml(info.chatzer)}" data-tooltip-type="chatzer">${info.chatzer}</span></div>` : ''}
                                ${serie ? `<div class="album-serie">${serie}</div>` : ''}
                                ${info.year ? `<div class="album-year">שנת ${info.year}</div>` : ''}
                                ${notes ? `<div class="album-notes">${notes}</div>` : ''}
                            </div>
                            <div class="album-actions">
                                ${hasBooklet ? `
                                    <button class="album-booklet-btn" onclick="openPdfPopup('${encodeURIComponent(info.booklet)}', '${encodeURIComponent(key + ' - בוקלעט')}')">
                                        בוקלעט
                                    </button>
                                ` : '<div class="album-btn-placeholder"></div>'}
                                ${hasBuyLink ? `
                                    <a href="${info.whereToBuy}" target="_blank" class="album-buy-btn">
                                        צו באקומען
                                    </a>
                                ` : '<div class="album-btn-placeholder"></div>'}
                            </div>
                        </div>
                    `;
                });

                html += `
                        </div>
                    </div>
                `;
            });

            html += `</div>`;
            container.innerHTML = html;

            const countEl = container.querySelector('.subtitle-count');
            if (countEl) animateCountUp(countEl, allKeys.length);

            // Hide loading wave bar
            const loader = document.getElementById('albumLoader');
            if (loader) setTimeout(() => loader.classList.remove('active'), 500);
        }

        // Generic detail page renderer - consolidates album, document, resource detail pages
        function renderGenericDetailPage(config) {
            const {
                container,
                itemName,
                infoSource,
                type, // 'albums', 'documents', 'resources'
                typeLabel, // 'אלבום', 'דאקומענט', 'רעסורס'
                backPage, // 'albums', 'documents', 'resources'
                backLabel, // 'אלבומס בלאט', etc.
                themeVar, // 'album', 'document', 'resource'
                buildSections // function that returns custom HTML sections
            } = config;

            const trimmedName = itemName.trim();
            const info = infoSource[trimmedName] || infoSource[itemName] || {};

            // Find related nigunim
            const relatedNigunim = allSongs.filter(song => {
                const field = song[type]; // e.g., song.albums, song.documents
                return field && field.split(',').map(x => x.trim()).includes(itemName);
            });

            // Build custom sections
            const customSections = buildSections(info, itemName);

            container.innerHTML = `
                <div class="detail-page page-theme theme-${themeVar}">
                    <button class="back-button" onclick="navigateTo('${backPage}')">
                        → ${backLabel}
                    </button>

                    <div class="detail-header">
                        <div class="detail-category-bar" style="background: var(--color-${themeVar});">${typeLabel}</div>
                        ${customSections.header}
                    </div>

                    ${customSections.body}

                    ${relatedNigunim.length > 0 ? `
                        <div class="detail-section">
                            <h3>פארבונדענע ניגונים (${relatedNigunim.length})</h3>
                            <div class="songs-card">
                                <div class="all-songs-grid">
                                    ${(() => {
                                        const playlistIndices = relatedNigunim.map(song => allSongs.indexOf(song));
                                        return relatedNigunim.map((song, idx) => {
                                            const globalIdx = allSongs.indexOf(song);
                                            return renderSongItem(song, globalIdx, idx + 1, playlistIndices);
                                        }).join('');
                                    })()}
                                </div>
                            </div>
                        </div>
                    ` : ''}
                </div>
            `;
        }

        // Render album detail page - uses generic renderer
        function renderAlbumDetailPage(container, albumName) {
            renderGenericDetailPage({
                container,
                itemName: albumName,
                infoSource: albumsInfo,
                type: 'albums',
                typeLabel: 'אלבום',
                backPage: 'albums',
                backLabel: 'אלבומס בלאט',
                themeVar: 'album',
                buildSections: (info, name) => {
                    const hasImage = info.cover || info.image;
                    const coverUrl = info.cover || info.image;
                    const hasBooklet = info.booklet && info.booklet.startsWith('http');
                    const hasBuyLink = info.whereToBuy && info.whereToBuy.startsWith('http');
                    const producer = info.producer?.replace(/\`\`\`/g, '') || '';

                    return {
                        header: `
                            <div class="detail-header-content ${hasImage ? '' : 'no-image'}">
                                ${hasImage ? `<img class="detail-image" src="${coverUrl}" alt="${name}">` : ''}
                                <div class="detail-header-text">
                                    <h1 class="detail-title" style="color: var(--color-album-dark);">${name}</h1>
                                    ${producer ? `<div class="detail-subtitle">${producer}</div>` : ''}
                                    ${info.chatzer ? `<div class="detail-chatzer"><span class="chatzer-tag" data-action="detail" data-category="chatzeros" data-name="${escapeHtml(info.chatzer)}">${info.chatzer}</span></div>` : ''}
                                    ${info.year ? `<div class="detail-year">שנת ${info.year}</div>` : ''}
                                </div>
                            </div>
                        `,
                        body: `
                            ${info.description || info.notes ? `
                                <div class="detail-section">
                                    <h3>אינפארמאציע</h3>
                                    <p>${formatDescription(info.description || info.notes || '')}</p>
                                </div>
                            ` : ''}

                            ${(hasBooklet || hasBuyLink) ? `
                                <div class="detail-actions">
                                    ${hasBooklet ? `
                                        <button class="detail-action-btn primary" onclick="openPdfPopup('${encodeURIComponent(info.booklet)}', '${encodeURIComponent(name + ' - בוקלעט')}')">
                                            📄 בוקלעט
                                        </button>
                                    ` : ''}
                                    ${hasBuyLink ? `
                                        <a href="${info.whereToBuy}" target="_blank" class="detail-action-btn primary">
                                            🛒 צו באקומען
                                        </a>
                                    ` : ''}
                                </div>
                            ` : ''}
                        `
                    };
                }
            });
        }

        // Render document detail page - uses generic renderer
        function renderDocumentDetailPage(container, docName) {
            renderGenericDetailPage({
                container,
                itemName: docName,
                infoSource: documentsInfo,
                type: 'documents',
                typeLabel: 'דאקומענט',
                backPage: 'documents',
                backLabel: 'דאקומענטן בלאט',
                themeVar: 'document',
                buildSections: (info, name) => {
                    // Extract file URL
                    let fileUrl = info.file;
                    if (!fileUrl && info.allData) {
                        fileUrl = extractUrl(info.allData['פייל']) || extractUrl(info.allData['file']);
                    }

                    // Extract link URL
                    let linkUrl = info.link;
                    if (!linkUrl && info.allData) {
                        linkUrl = extractUrl(info.allData['לינק']) || extractUrl(info.allData['link']);
                    }

                    const hasFile = fileUrl && fileUrl.startsWith('http');
                    const hasLink = linkUrl && linkUrl.startsWith('http');
                    const sortType = (info.allData?.['סארט'] || info.sort || '').replace(/\`\`\`/g, '');

                    return {
                        header: `
                            <div class="detail-header-content no-image">
                                <div class="detail-header-text">
                                    <h1 class="detail-title" style="color: var(--color-document-dark);">${name}</h1>
                                    ${sortType ? `<div class="detail-type-badge" style="background: var(--color-document-pale); color: var(--color-document-dark);">${sortType}</div>` : ''}
                                    ${info.serie ? `<div class="detail-subtitle">${info.serie.replace(/\`\`\`/g, '')}</div>` : ''}
                                    ${info.chatzer ? `<div class="detail-chatzer"><span class="chatzer-tag" data-action="detail" data-category="chatzeros" data-name="${escapeHtml(info.chatzer)}">${info.chatzer}</span></div>` : ''}
                                </div>
                            </div>
                        `,
                        body: `
                            ${hasFile ? `
                                <div class="detail-pdf-preview">
                                    <iframe src="https://docs.google.com/viewer?url=${encodeURIComponent(fileUrl)}&embedded=true" style="width:100%; height:500px; border:none; border-radius:10px;"></iframe>
                                </div>
                            ` : ''}

                            ${info.description ? `
                                <div class="detail-section">
                                    <h3>אינפארמאציע</h3>
                                    <p>${formatDescription(info.description)}</p>
                                </div>
                            ` : ''}

                            ${(hasFile || hasLink) ? `
                                <div class="detail-actions">
                                    ${hasFile ? `
                                        <button class="detail-action-btn primary" onclick="openPdfPopup('${encodeURIComponent(fileUrl)}', '${encodeURIComponent(name)}')">
                                            📄 עפן PDF
                                        </button>
                                    ` : ''}
                                    ${hasLink ? `
                                        <a href="${linkUrl}" target="_blank" class="detail-action-btn secondary">
                                            🔗 לינק
                                        </a>
                                    ` : ''}
                                </div>
                            ` : ''}
                        `
                    };
                }
            });
        }

        // Render resource detail page - uses generic renderer
        function renderResourceDetailPage(container, resourceName) {
            renderGenericDetailPage({
                container,
                itemName: resourceName,
                infoSource: resourcesInfo,
                type: 'resources',
                typeLabel: 'רעסורס',
                backPage: 'resources',
                backLabel: 'רעסורסן בלאט',
                themeVar: 'resource',
                buildSections: (info, name) => {
                    // Extract link URL
                    let linkUrl = info.link;
                    if (!linkUrl && info.allData) {
                        const linkData = info.allData['לינק / נאמבער'] || info.allData['לינק'];
                        if (linkData) {
                            const urlMatch = linkData.match(/https?:\/\/[^\s\)]+/);
                            if (urlMatch) linkUrl = urlMatch[0];
                        }
                    }

                    const hasLink = linkUrl && linkUrl.startsWith('http');
                    const linkText = info.allData?.['לינק / נאמבער'] || '';
                    const isPhone = linkText && !linkText.includes('http') && linkText.match(/\d{3}/);
                    const sortType = (info.sort || info.allData?.['סארט'] || '').replace(/\`\`\`/g, '');

                    return {
                        header: `
                            <div class="detail-header-content no-image">
                                <div class="detail-header-text">
                                    <h1 class="detail-title" style="color: var(--color-resource-dark);">${name}</h1>
                                    ${sortType ? `<div class="detail-type-badge" style="background: var(--color-resource-pale); color: var(--color-resource-dark);">${sortType}</div>` : ''}
                                    ${info.chatzer ? `<div class="detail-chatzer"><span class="chatzer-tag" data-action="detail" data-category="chatzeros" data-name="${escapeHtml(info.chatzer)}">${info.chatzer}</span></div>` : ''}
                                </div>
                            </div>
                        `,
                        body: `
                            ${info.description ? `
                                <div class="detail-section">
                                    <h3>אינפארמאציע</h3>
                                    <p>${formatDescription(info.description)}</p>
                                </div>
                            ` : ''}

                            ${isPhone ? `
                                <div class="detail-section">
                                    <h3>טעלעפאן נומער</h3>
                                    <div class="resource-phone-display">${linkText.replace(/\`\`\`/g, '')}</div>
                                </div>
                            ` : ''}

                            ${hasLink ? `
                                <div class="detail-actions">
                                    <a href="${linkUrl}" target="_blank" class="detail-action-btn primary">
                                        🔗 עפן לינק
                                    </a>
                                </div>
                            ` : ''}
                        `
                    };
                }
            });
        }

        // Toggle category card - no longer needed but keep for compatibility
        function toggleCategoryCard(encodedKey, categoryKey) {
            navigateToDetail(categoryKey, encodedKey);
        }

        // Hover tooltip functionality
        const hoverTooltip = document.getElementById('hoverTooltip');
        let tooltipTimeout = null;
        let showTooltipTimeout = null;

        function showTooltip(event, value, fieldType) {
            clearTimeout(tooltipTimeout);
            clearTimeout(showTooltipTimeout);

            // Add a delay before showing the tooltip
            showTooltipTimeout = setTimeout(() => {
                performShowTooltip(event, value, fieldType);
            }, 400);
        }

        function performShowTooltip(event, value, fieldType) {

            const tooltip = hoverTooltip;
            const typeEl = document.getElementById('tooltipType');
            const titleEl = document.getElementById('tooltipTitle');
            const imageEl = document.getElementById('tooltipImage');
            const subtitleEl = document.getElementById('tooltipSubtitle');
            const statsEl = document.getElementById('tooltipStats');

            // Remove all theme classes first
            tooltip.classList.remove('tooltip-chatzer', 'tooltip-mechaber', 'tooltip-verter', 'tooltip-zman', 'tooltip-piyut', 'tooltip-collection');

            // Get info based on field type
            let categoryKey = '';
            let typeName = '';
            let extraInfo = null;
            let themeClass = '';

            switch (fieldType) {
                case 'mechaber':
                    categoryKey = 'mechabrim';
                    typeName = 'מחבר';
                    extraInfo = mechabrimInfo[value];
                    themeClass = 'tooltip-mechaber';
                    break;
                case 'chatzer':
                    categoryKey = 'chatzeros';
                    typeName = 'חצר';
                    extraInfo = chatzerosInfo[value];
                    themeClass = 'tooltip-chatzer';
                    break;
                case 'verter':
                    categoryKey = 'verter';
                    typeName = 'ווערטער';
                    extraInfo = verterInfo[value];
                    themeClass = 'tooltip-verter';
                    break;
                case 'zman':
                    // Check if it's a piyut or zman
                    if (piyutimInfo[value]) {
                        categoryKey = 'piyutim';
                        typeName = 'פיוט';
                        extraInfo = piyutimInfo[value];
                        themeClass = 'tooltip-piyut';
                    } else {
                        categoryKey = 'zmanim';
                        typeName = 'זמן';
                        extraInfo = zmaninInfo[value];
                        themeClass = 'tooltip-zman';
                    }
                    break;
                case 'piyut':
                    categoryKey = 'piyutim';
                    typeName = 'פיוט';
                    extraInfo = piyutimInfo[value];
                    themeClass = 'tooltip-piyut';
                    break;
                case 'collection':
                    categoryKey = 'collections';
                    typeName = 'קאלעקשאן';
                    extraInfo = collectionsInfo[value];
                    themeClass = 'tooltip-collection';
                    break;
            }

            // Add theme class
            if (themeClass) {
                tooltip.classList.add(themeClass);
            }

            // Set type label
            typeEl.textContent = typeName;

            // For mechaber, build full name
            let displayName = value;
            if (fieldType === 'mechaber' && extraInfo) {
                const nameParts = [extraInfo.title, extraInfo.firstName, extraInfo.lastName, extraInfo.suffix].filter(Boolean);
                if (nameParts.length > 0) {
                    displayName = nameParts.join(' ');
                }
            }
            // For verter, show the full text if available
            if (fieldType === 'verter' && extraInfo && extraInfo.fullText) {
                displayName = extraInfo.fullText;
            }
            titleEl.textContent = displayName;

            // Handle image
            if (extraInfo && extraInfo.image) {
                imageEl.src = extraInfo.image;
                imageEl.style.display = 'block';
                // Make mechaber images round
                if (fieldType === 'mechaber') {
                    imageEl.classList.add('round');
                } else {
                    imageEl.classList.remove('round');
                }
            } else {
                imageEl.style.display = 'none';
                imageEl.classList.remove('round');
            }

            // Handle subtitle (dates for mechabrim)
            let subtitle = '';
            if (extraInfo && fieldType === 'mechaber') {
                const birthParts = [extraInfo.birthDay, extraInfo.birthMonth, extraInfo.birthYear].filter(Boolean);
                const deathParts = [extraInfo.deathDay, extraInfo.deathMonth, extraInfo.deathYear].filter(Boolean);
                const birthStr = birthParts.join(' ');
                const deathStr = deathParts.join(' ');

                if (birthStr && deathStr) {
                    subtitle = `${birthStr} - ${deathStr}`;
                } else if (birthStr) {
                    subtitle = `נולד: ${birthStr}`;
                } else if (deathStr) {
                    subtitle = `נפטר: ${deathStr}`;
                }
            }
            subtitleEl.textContent = subtitle;
            subtitleEl.style.display = subtitle ? 'block' : 'none';

            // Build stats
            const songIndices = categories[categoryKey]?.[value] || [];
            const songCount = songIndices.length;

            let statsHtml = `${songCount} ניגונים`;

            // For chatzeros, count mechabrim
            if (fieldType === 'chatzer') {
                const mechabrimCount = Object.entries(mechabrimInfo)
                    .filter(([mechName, mechInfo]) => {
                        if (!mechInfo.chatzer) return false;
                        const mechChatzeros = mechInfo.chatzer.split(',').map(c => c.trim());
                        return mechChatzeros.includes(value);
                    }).length;
                if (mechabrimCount > 0) {
                    statsHtml = `${mechabrimCount} מחברים · ${statsHtml}`;
                }
            }

            statsEl.textContent = statsHtml;

            // Position tooltip near mouse
            const x = event.clientX;
            const y = event.clientY;

            tooltip.style.left = `${Math.min(x + 15, window.innerWidth - 300)}px`;
            tooltip.style.top = `${Math.min(y + 15, window.innerHeight - 200)}px`;

            tooltip.classList.add('show');
        }

        function hideTooltip() {
            clearTimeout(showTooltipTimeout);
            tooltipTimeout = setTimeout(() => {
                hoverTooltip.classList.remove('show');
            }, 100);
        }

        function forceHideTooltip() {
            clearTimeout(tooltipTimeout);
            clearTimeout(showTooltipTimeout);
            hoverTooltip.classList.remove('show');
        }

        // Create meta links for song items (handles multiple comma-separated values)
        // Uses pill button styling with category colors and data attributes for event delegation
        function createMetaLinks(value, page, filterKey, entityType = null) {
            if (!value) return '';
            const values = value.split(',').map(v => v.trim()).filter(Boolean);
            const tagClass = filterKey === 'mechaber' ? 'mechaber-tag' :
                filterKey === 'chatzer' ? 'chatzer-tag' :
                    filterKey === 'verter' ? 'verter-tag' :
                        filterKey === 'zman' ? 'zman-tag' :
                            filterKey === 'collection' ? 'collection-tag' : 'song-meta-link';
            return values.map(v =>
                `<span class="${tagClass} song-meta-tag"
                    data-action="filter"
                    data-page="${page}"
                    data-filter-key="${filterKey}"
                    data-value="${escapeHtml(v)}"
                    data-tooltip-value="${escapeHtml(v)}"
                    data-tooltip-type="${filterKey}">${v}</span>`
            ).join(' ');
        }

        // Render song item - supports display modes: 'minimal', 'recordings', 'full'
        function renderSongItem(song, globalIdx, displayNum, playlistIndices = null) {
            const isActive = globalIdx === currentSongIndex;
            const hasAudio = songHasQualityAudio(song); // Uses quality filter
            const displayMode = currentDisplayMode || 'minimal';

            // Create meta with links - using entity-specific colors
            let metaParts = [];
            if (song.mechaber) {
                metaParts.push(createMetaLinks(song.mechaber, 'mechabrim', 'mechaber', 'mechaber'));
            }
            if (song.category) {
                metaParts.push(createMetaLinks(song.category, 'chatzeros', 'chatzer', 'chatzer'));
            }

            // Clicking on song always opens details
            const clickAction = `showSongDetails(${globalIdx})`;

            // Play button action
            const playAction = playlistIndices
                ? `playSongFromList(${globalIdx}, [${playlistIndices.join(',')}])`
                : `playSong(${globalIdx})`;

            // Wave animation for currently playing song
            const waveAnimation = isActive ? `
                <div class="song-wave-animation">
                    <span class="wave-bar"></span>
                    <span class="wave-bar"></span>
                    <span class="wave-bar"></span>
                    <span class="wave-bar"></span>
                </div>
            ` : '';

            // === MINIMAL MODE (default) ===
            if (displayMode === 'minimal') {
                // Determine play button state
                let playBtnHtml = '';
                const hasAnyRecordings = song.recordings && song.recordings.length > 0;
                const hasQualityRecordings = hasAudio; // already checked with songHasQualityAudio

                if (hasQualityRecordings) {
                    // Quality recordings - show play button
                    playBtnHtml = `<button class="song-play-btn-new" onclick="event.stopPropagation(); ${playAction};" data-song-idx="${globalIdx}">
                        <span class="play-icon"><svg viewBox="0 0 24 24" fill="currentColor" width="16" height="16"><path d="M8 5v14l11-7L8 5z"/></svg></span>
                        <span class="pause-icon"><svg viewBox="0 0 24 24" fill="currentColor" width="16" height="16"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg></span>
                    </button>`;
                } else if (hasAnyRecordings) {
                    // Has recordings but none meet quality - show faded half-star
                    playBtnHtml = `<div class="song-play-btn-new song-btn-low-quality" title="רעקארדירונגען מיט נידעריגע קוואליטעט">
                        <span class="low-quality-icon">☆</span>
                    </div>`;
                }
                // No recordings = no button (playBtnHtml stays empty)

                return `
                    <div class="song-item ${isActive ? 'active' : ''} ${!hasAudio ? 'no-audio' : ''}" data-song-idx="${globalIdx}" onclick="${clickAction}">
                        <div class="song-number">${displayNum}</div>
                        <div class="song-info">
                            <div class="song-name">${song.name}</div>
                            <div class="song-meta">${metaParts.join(' • ')}</div>
                        </div>
                        ${waveAnimation}
                        ${playBtnHtml}
                    </div>
                `;
            }

            if (displayMode === 'recordings') {
                let recordingsHtml = '';
                if (song.recordings && song.recordings.length > 0) {
                    recordingsHtml = `
                        <div class="song-item-recordings">
                            ${song.recordings.map((rec, i) => {
                        const playlistParam = playlistIndices ? `, [${playlistIndices.join(',')}]` : '';
                        const recPlayAction = `playNigunRecording(${globalIdx}, ${i}, this${playlistParam})`;
                        const recMeetsQuality = recordingMeetsQuality(rec);
                        const isRecActive = isActive && currentRecordingIndex === i;
                        const recPlayBtn = recMeetsQuality
                            ? `<button class="song-play-btn-new rec-play-btn ${isRecActive && !audioPlayer.paused ? 'playing' : ''}" onclick="event.stopPropagation(); ${recPlayAction}" data-song-idx="${globalIdx}" data-rec-idx="${i}">
                                <span class="play-icon"><svg viewBox="0 0 24 24" fill="currentColor" width="14" height="14"><path d="M8 5v14l11-7L8 5z"/></svg></span>
                                <span class="pause-icon"><svg viewBox="0 0 24 24" fill="currentColor" width="14" height="14"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg></span>
                            </button>`
                            : `<span class="song-play-btn-new song-btn-low-quality rec-play-btn" title="קוואליטעט נידעריג"><span class="low-quality-icon">☆</span></span>`;
                        const recRating = rec.rating ? `<span class="rec-rating-display">${'★'.repeat(rec.rating)}</span>` : '';

                        // Build tags for this recording
                        let recTags = [];
                        if (rec.album) {
                            recTags.push(`<span class="nigun-tag-inline tag-album" 
                                data-action="detail" 
                                data-category="albums" 
                                data-name="${escapeHtml(rec.album)}"
                                style="cursor:pointer;"
                                onclick="event.stopPropagation();">${rec.album}</span>`);
                        }
                        if (rec.personalities) {
                            rec.personalities.split(',').forEach(p => {
                                const name = p.trim();
                                if (name) {
                                    recTags.push(`<span class="nigun-tag-inline tag-mechaber" 
                                        data-action="detail"
                                        data-category="mechabrim"
                                        data-name="${escapeHtml(name)}"
                                        style="cursor:pointer;"
                                        onclick="event.stopPropagation();">${name}</span>`);
                                }
                            });
                        }
                        const displayName = rec.details || ('רעקארדינג ' + (i + 1));

                        return `
                                    <div class="song-item-recording ${isRecActive ? 'active-recording' : ''} ${!recMeetsQuality ? 'low-quality-rec' : ''}" onclick="event.stopPropagation(); ${recPlayAction}" data-song-idx="${globalIdx}" data-rec-idx="${i}">
                                        <div class="song-item-rec-num">${i + 1}</div>
                                        <div class="song-item-rec-info">
                                            <div class="song-item-rec-name">${displayName}</div>
                                            ${recTags.length > 0 ? `<div class="song-item-rec-tags">${recTags.join('')}</div>` : ''}
                                        </div>
                                        ${recRating}
                                        ${recPlayBtn}
                                    </div>
                                `;
                    }).join('')}
                        </div>
                    `;
                }

                return `
                    <div class="song-item song-item-expanded ${isActive ? 'active' : ''} ${!hasAudio ? 'no-audio' : ''}" data-song-idx="${globalIdx}" onclick="${clickAction}">
                        <div class="song-item-header">
                            <div class="song-number">${displayNum}</div>
                            <div class="song-info">
                                <div class="song-name">${song.name}</div>
                                <div class="song-meta">${metaParts.join(' • ')}</div>
                            </div>
                            ${waveAnimation}
                        </div>
                        ${recordingsHtml}
                    </div>
                `;
            }

            // === FULL MODE - show all available info ===
            if (displayMode === 'full') {
                // Build additional info sections with colorful tags like detail page
                let extraInfo = [];

                // Verter (lyrics) - purple/verter color with tooltip
                if (song.verter) {
                    const verterTags = song.verter.split(',').map(v => v.trim()).filter(Boolean).map(v => `
                        <span class="nigun-tag-inline tag-verter" 
                              data-action="detail" 
                              data-category="verter" 
                              data-name="${escapeHtml(v)}"
                              data-tooltip-value="${escapeHtml(v)}"
                              data-tooltip-type="verter"
                              onclick="event.stopPropagation();">${v}</span>
                    `).join('');
                    extraInfo.push(`<div class="song-full-row"><span class="song-full-label">ווערטער:</span><div class="song-full-tags">${verterTags}</div></div>`);
                }

                // Collections - collection color with tooltip
                if (song.collections) {
                    const collectionTags = song.collections.split(',').map(c => c.trim()).filter(Boolean).map(c => `
                        <span class="nigun-tag-inline tag-collection" 
                              data-action="detail" 
                              data-category="collections" 
                              data-name="${escapeHtml(c)}"
                              data-tooltip-value="${escapeHtml(c)}"
                              data-tooltip-type="collection"
                              onclick="event.stopPropagation();">${c}</span>
                    `).join('');
                    extraInfo.push(`<div class="song-full-row"><span class="song-full-label">קאלעקשאנס:</span><div class="song-full-tags">${collectionTags}</div></div>`);
                }

                // Pasig Oif (suitable for) - zman/piyut color with tooltip
                if (song.pasigOif) {
                    const pasigTags = song.pasigOif.split(',').map(p => p.trim()).filter(Boolean).map(p => `
                        <span class="nigun-tag-inline tag-piyut" 
                              data-action="filter" 
                              data-page="nigunim"
                              data-filter-key="zman"
                              data-value="${escapeHtml(p)}"
                              data-tooltip-value="${escapeHtml(p)}"
                              data-tooltip-type="zman"
                              onclick="event.stopPropagation();">${p}</span>
                    `).join('');
                    extraInfo.push(`<div class="song-full-row"><span class="song-full-label">פאסיג אויף:</span><div class="song-full-tags">${pasigTags}</div></div>`);
                }

                // Scale - music color with tooltip
                if (song.scale) {
                    const scaleTags = song.scale.split(',').map(s => s.trim()).filter(Boolean).map(s => `
                        <span class="nigun-tag-inline tag-scale" 
                              data-action="filter" 
                              data-page="nigunim"
                              data-filter-key="scale"
                              data-value="${escapeHtml(s)}"
                              data-tooltip-value="${escapeHtml(s)}"
                              data-tooltip-type="scale"
                              onclick="event.stopPropagation();">${s}</span>
                    `).join('');
                    extraInfo.push(`<div class="song-full-row"><span class="song-full-label">סקעיל:</span><div class="song-full-tags">${scaleTags}</div></div>`);
                }

                // Ritem - music color with tooltip
                if (song.ritem) {
                    const ritemTags = song.ritem.split(',').map(r => r.trim()).filter(Boolean).map(r => `
                        <span class="nigun-tag-inline tag-ritem" 
                              data-action="filter" 
                              data-page="nigunim"
                              data-filter-key="ritem"
                              data-value="${escapeHtml(r)}"
                              data-tooltip-value="${escapeHtml(r)}"
                              data-tooltip-type="ritem"
                              onclick="event.stopPropagation();">${r}</span>
                    `).join('');
                    extraInfo.push(`<div class="song-full-row"><span class="song-full-label">ריטעם:</span><div class="song-full-tags">${ritemTags}</div></div>`);
                }

                // Siman - simple tag
                if (song.siman) {
                    extraInfo.push(`<div class="song-full-row"><span class="song-full-label">סימן:</span><span class="nigun-tag-inline tag-siman">${formatDescription(song.siman)}</span></div>`);
                }

                // Recordings section with more info
                let recordingsHtml = '';
                if (song.recordings && song.recordings.length > 0) {
                    const hasMultipleRecordings = song.recordings.length > 1;
                    const toggleArrow = hasMultipleRecordings ?
                        `<span class="rec-collapse-toggle" onclick="event.stopPropagation(); toggleRecordingsCollapse();" title="${recordingsCollapsed ? 'הויף אויף אלע רעקארדירונגען' : 'קלאפ צו רעקארדירונגען'}">${recordingsCollapsed ? '◀' : '▼'}</span>` : '';

                    // If collapsed and multiple recordings, show only first
                    const displayRecordings = (recordingsCollapsed && hasMultipleRecordings) ? [song.recordings[0]] : song.recordings;

                    recordingsHtml = `
                        <div class="song-full-recordings ${recordingsCollapsed ? 'collapsed' : ''}">
                            <div class="song-full-rec-title">${toggleArrow}רעקארדירונגען (${song.recordings.length})</div>
                            ${displayRecordings.map((rec, i) => {
                        const actualIndex = recordingsCollapsed && hasMultipleRecordings ? 0 : i;
                        const playlistParam = playlistIndices ? `, [${playlistIndices.join(',')}]` : '';
                        const recPlayAction = `playNigunRecording(${globalIdx}, ${actualIndex}, this${playlistParam})`;
                        // Build info tags for this recording
                        let recTags = [];
                        if (rec.album) {
                            recTags.push(`<span class="nigun-tag-inline tag-album" 
                                data-action="detail" 
                                data-category="albums" 
                                data-name="${escapeHtml(rec.album)}"
                                style="cursor:pointer;"
                                onclick="event.stopPropagation();">${rec.album}</span>`);
                        }
                        if (rec.personalities) {
                            rec.personalities.split(',').forEach(p => {
                                const name = p.trim();
                                if (name) {
                                    recTags.push(`<span class="nigun-tag-inline tag-mechaber" 
                                        data-action="detail"
                                        data-category="mechabrim"
                                        data-name="${escapeHtml(name)}"
                                        style="cursor:pointer;"
                                        onclick="event.stopPropagation();">${name}</span>`);
                                }
                            });
                        }
                        const displayName = rec.details || ('רעקארדינג ' + (actualIndex + 1));
                        const recMeetsQuality = recordingMeetsQuality(rec);
                        const isRecActive = isActive && currentRecordingIndex === actualIndex;
                        const recPlayBtn = recMeetsQuality
                            ? `<button class="song-play-btn-new rec-play-btn ${isRecActive && !audioPlayer.paused ? 'playing' : ''}" onclick="event.stopPropagation(); ${recPlayAction}" data-song-idx="${globalIdx}" data-rec-idx="${actualIndex}">
                                <span class="play-icon"><svg viewBox="0 0 24 24" fill="currentColor" width="14" height="14"><path d="M8 5v14l11-7L8 5z"/></svg></span>
                                <span class="pause-icon"><svg viewBox="0 0 24 24" fill="currentColor" width="14" height="14"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg></span>
                            </button>`
                            : `<span class="song-play-btn-new song-btn-low-quality rec-play-btn" title="קוואליטעט נידעריג"><span class="low-quality-icon">☆</span></span>`;
                        const recRating = rec.rating ? `<span class="rec-rating-display">${'★'.repeat(rec.rating)}</span>` : '';
                        return `
                                    <div class="song-item-recording ${isRecActive ? 'active-recording' : ''} ${!recMeetsQuality ? 'low-quality-rec' : ''}" onclick="event.stopPropagation(); ${recPlayAction}" data-song-idx="${globalIdx}" data-rec-idx="${actualIndex}">
                                        <div class="song-item-rec-num">${actualIndex + 1}</div>
                                        <div class="song-item-rec-info">
                                            <div class="song-item-rec-name">${displayName}</div>
                                            ${recTags.length > 0 ? `<div class="song-item-rec-tags">${recTags.join('')}</div>` : ''}
                                        </div>
                                        ${recRating}
                                        ${recPlayBtn}
                                    </div>
                                `;
                    }).join('')}
                        </div>
                    `;
                }
                return `
                    <div class="song-item song-item-full ${isActive ? 'active' : ''} ${!hasAudio ? 'no-audio' : ''}" data-song-idx="${globalIdx}" onclick="${clickAction}">
                        <div class="song-item-header">
                            <div class="song-number">${displayNum}</div>
                            <div class="song-info">
                                <div class="song-name">${song.name}</div>
                                <div class="song-meta">${metaParts.join(' • ')}</div>
                            </div>
                            ${waveAnimation}
                        </div>
                        ${extraInfo.length > 0 ? `<div class="song-full-details">${extraInfo.join('')}</div>` : ''}
                        ${recordingsHtml}
                    </div>
                `;
            }

            // Fallback to minimal
            return `
                <div class="song-item ${isActive ? 'active' : ''} ${!hasAudio ? 'no-audio' : ''}" data-song-idx="${globalIdx}" onclick="${clickAction}">
                    <div class="song-number">${displayNum}</div>
                    <div class="song-info">
                        <div class="song-name">${song.name}</div>
                        <div class="song-meta">${metaParts.join(' • ')}</div>
                    </div>
                    ${waveAnimation}
                    ${hasAudio ? `<button class="song-play-btn-new" onclick="event.stopPropagation(); ${playAction};" data-song-idx="${globalIdx}">
                        <span class="play-icon"><svg viewBox="0 0 24 24" fill="currentColor" width="16" height="16"><path d="M8 5v14l11-7L8 5z"/></svg></span>
                        <span class="pause-icon"><svg viewBox="0 0 24 24" fill="currentColor" width="16" height="16"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg></span>
                    </button>` : ''}
                </div>
            `;
        }

        // Go to page
        function goToPage(page) {
            const totalPages = Math.ceil(filteredSongs.length / songsPerPage);
            if (page < 1 || page > totalPages) return;
            currentPage = page;
            renderCurrentPage();
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }

        // Show the player banner with animation (when song first plays)
        function showPlayer() {
            if (!playerVisible) {
                playerVisible = true;
                const playerSection = document.querySelector('.player-section');
                playerSection.classList.add('player-visible');
                playerSection.classList.remove('player-minimized');
                document.getElementById('playerShowBtn').classList.remove('visible');
            }
        }

        // Hide the player banner (user clicked hide button)
        function hidePlayerBanner() {
            const playerSection = document.querySelector('.player-section');
            playerSection.classList.add('player-minimized');

            // Show the floating button with song name
            const showBtn = document.getElementById('playerShowBtn');
            const songName = document.getElementById('playerSongName').textContent;
            document.getElementById('showBtnSongName').textContent = songName || 'ווייז פלעיער';
            showBtn.classList.add('visible');
        }

        // Show the player banner (user clicked show button)
        function showPlayerBanner() {
            const playerSection = document.querySelector('.player-section');
            playerSection.classList.remove('player-minimized');
            document.getElementById('playerShowBtn').classList.remove('visible');
        }

        // Play song (sets playlist if not already set)
        function playSong(idx, playlist = null) {
            if (idx < 0 || idx >= allSongs.length) return;

            // If clicking the same song, toggle play/pause
            if (idx === currentSongIndex && audioPlayer.src) {
                togglePlayPause();
                return;
            }

            // Reset recordings mode when playing from song list
            isInRecordingsMode = false;
            currentRecordingIndex = 0;

            // If a playlist is provided, use it
            if (playlist && Array.isArray(playlist)) {
                currentPlaylist = playlist;
                currentPlaylistPosition = currentPlaylist.indexOf(idx);
            } else if (currentPlaylist.length === 0 || !currentPlaylist.includes(idx)) {
                // If no playlist set or song not in current playlist, use filteredSongs
                currentPlaylist = filteredSongs.map(s => allSongs.indexOf(s));
                currentPlaylistPosition = currentPlaylist.indexOf(idx);
            } else {
                // Song is in current playlist, just update position
                currentPlaylistPosition = currentPlaylist.indexOf(idx);
            }

            currentSongIndex = idx;
            const song = allSongs[idx];

            // Set recording info for rating updates (default to first recording)
            if (song.recordings && song.recordings.length > 0) {
                currentPlayingRecordingInfo = {
                    songIdx: idx,
                    recordingIdx: 0,
                    slotIndex: song.recordings[0].slotIndex,
                    rowId: song.rowId
                };
            } else {
                currentPlayingRecordingInfo = null;
            }

            audioPlayer.src = song.audioUrl;
            audioPlayer.play();

            showPlayer();
            updatePlayerUI(song);
            updateActiveSongInList();
        }

        // Play song from a specific playlist (detail pages)
        function playSongFromList(idx, playlistIndices) {
            // If clicking the same song, toggle play/pause
            if (idx === currentSongIndex && audioPlayer.src) {
                togglePlayPause();
                return;
            }
            currentPlaylist = playlistIndices;
            currentPlaylistPosition = playlistIndices.indexOf(idx);
            playSong(idx, playlistIndices);
        }

        // Play a collection from the start
        function playCollectionFromStart(collectionName) {
            const trimmedName = collectionName.trim();
            console.log('Playing collection:', trimmedName);
            console.log('Available collections:', Object.keys(categories.collections));

            const songIndices = categories.collections[trimmedName] || categories.collections[collectionName] || [];
            console.log('Song indices:', songIndices);

            if (songIndices.length === 0) {
                console.log('No songs found for collection');
                return;
            }

            // Filter to only songs with audio
            const playableSongs = songIndices.filter(idx => {
                const song = allSongs[idx];
                return song && song.audioUrl && song.audioUrl.startsWith('http');
            });

            console.log('Playable songs:', playableSongs);

            if (playableSongs.length === 0) {
                console.log('No playable songs');
                return;
            }

            // Set playlist and play first song
            currentPlaylist = playableSongs;
            currentPlaylistPosition = 0;
            playSong(playableSongs[0], playableSongs);
        }

        // Play a music category (scale or ritem)
        function playMusicCategory(categoryType, categoryName) {
            const trimmedName = categoryName.trim();

            const categoryData = categoryType === 'scale' ? categories.scale : categories.ritem;
            const songIndices = categoryData[trimmedName] || categoryData[categoryName] || [];

            if (songIndices.length === 0) return;

            // Filter to only songs with audio
            const playableSongs = songIndices.filter(idx => {
                const song = allSongs[idx];
                return song && song.audioUrl && song.audioUrl.startsWith('http');
            });

            if (playableSongs.length === 0) return;

            // Set playlist and play first song
            currentPlaylist = playableSongs;
            currentPlaylistPosition = 0;
            playSong(playableSongs[0], playableSongs);
        }

        // Play a specific recording from a song
        function playRecording(songIdx, recordingIdx) {
            const song = allSongs[songIdx];
            if (!song || !song.recordings || !song.recordings[recordingIdx]) return;

            const recording = song.recordings[recordingIdx];

            // Update current song index and recording index
            currentSongIndex = songIdx;
            currentRecordingIndex = recordingIdx;

            // Set recording info for rating updates
            currentPlayingRecordingInfo = {
                songIdx: songIdx,
                recordingIdx: recordingIdx,
                slotIndex: recording.slotIndex,
                rowId: song.rowId
            };

            // Play the specific recording URL
            audioPlayer.src = recording.url;
            audioPlayer.play();

            showPlayer();
            // Update player UI with song info
            updatePlayerUI(song);
            updateActiveSongInList();

            // Update rating display in player
            updatePlayerRating(recording.rating);

            // Update active state in modal
            document.querySelectorAll('.modal-recording-item').forEach((item, i) => {
                item.classList.toggle('active', i === recordingIdx);
            });
        }

        // Create player meta links (handles multiple comma-separated values)
        // entityType determines the color class (mechaber, chatzer, collection)
        function createPlayerMetaLinks(value, page, filterKey, entityType = null) {
            if (!value) return '';
            const values = value.split(',').map(v => v.trim()).filter(Boolean);
            // Use entity-specific class if provided, otherwise generic
            const linkClass = entityType ? `player-meta-link-${entityType}` : 'player-meta-link';
            return values.map(v =>
                `<a href="#" class="${linkClass}"
                    data-action="filter"
                    data-page="${page}"
                    data-filter-key="${filterKey}"
                    data-value="${escapeHtml(v)}"
                    data-tooltip-value="${escapeHtml(v)}"
                    data-tooltip-type="${filterKey}">${v}</a>`
            ).join(', ');
        }

        // Update player UI
        function updatePlayerUI(song) {
            document.getElementById('playerSongName').textContent = song.name;

            // Create meta with links - using entity-specific colors
            let metaParts = [];
            if (song.mechaber) {
                metaParts.push(createPlayerMetaLinks(song.mechaber, 'mechabrim', 'mechaber', 'mechaber'));
            }
            if (song.category) {
                metaParts.push(createPlayerMetaLinks(song.category, 'chatzeros', 'chatzer', 'chatzer'));
            }
            document.getElementById('playerSongMeta').innerHTML = metaParts.join(' • ');

            // Collection link
            if (song.collections) {
                document.getElementById('playerCollection').innerHTML = createPlayerMetaLinks(song.collections, 'collections', 'collection', 'collection');
            } else {
                document.getElementById('playerCollection').textContent = '';
            }

            document.getElementById('playerGezungen').textContent = song.gezungen ? `געזונגען: ${song.gezungen}` : '';

            // Update rating from current recording (if in recordings mode) or first recording
            const currentRec = song.recordings && song.recordings[currentRecordingIndex];
            if (currentRec && currentRec.rating) {
                updatePlayerRating(currentRec.rating);
            } else {
                updatePlayerRating(0);
            }

            // Update player play button
            document.getElementById('playPauseBtn').classList.add('playing');
            // Remove paused state from equalizer
            const equalizerIcon = document.querySelector('.equalizer-icon');
            if (equalizerIcon) {
                equalizerIcon.classList.remove('paused');
            }
        }

        // Update active song highlight
        function updateActiveSongInList() {
            // Remove active state and wave animation from all song items
            document.querySelectorAll('.song-item').forEach(item => {
                item.classList.remove('active');
                item.classList.remove('paused');
                const existingWave = item.querySelector('.song-wave-animation');
                if (existingWave) {
                    existingWave.remove();
                }
            });

            // Reset all play buttons
            document.querySelectorAll('.song-play-btn-new').forEach(btn => {
                btn.classList.remove('playing');
            });

            // Find and mark the currently playing song
            if (currentSongIndex >= 0) {
                const activeItem = document.querySelector(`.song-item[data-song-idx="${currentSongIndex}"]`);
                if (activeItem) {
                    activeItem.classList.add('active');

                    // Add wave animation if not already present
                    if (!activeItem.querySelector('.song-wave-animation')) {
                        const waveDiv = document.createElement('div');
                        waveDiv.className = 'song-wave-animation';
                        waveDiv.innerHTML = `
                            <span class="wave-bar"></span>
                            <span class="wave-bar"></span>
                            <span class="wave-bar"></span>
                            <span class="wave-bar"></span>
                        `;

                        // For expanded/full items, insert into header
                        const header = activeItem.querySelector('.song-item-header');
                        if (header) {
                            header.appendChild(waveDiv);
                        } else {
                            // Insert before the play button for minimal items
                            const playBtn = activeItem.querySelector('.song-play-btn-new');
                            if (playBtn) {
                                activeItem.insertBefore(waveDiv, playBtn);
                            } else {
                                activeItem.appendChild(waveDiv);
                            }
                        }
                    }

                    // Update play button state
                    const activeBtn = activeItem.querySelector('.song-play-btn-new');
                    if (activeBtn && !audioPlayer.paused) {
                        activeBtn.classList.add('playing');
                    }

                    // Update paused state
                    if (audioPlayer.paused) {
                        activeItem.classList.add('paused');
                    }
                }
            }
        }

        // Toggle play/pause
        function togglePlayPause() {
            if (audioPlayer.paused) {
                audioPlayer.play();
            } else {
                audioPlayer.pause();
            }
            // UI updates handled by play/pause event listeners
        }

        // Play previous
        function playPrevious() {
            // Only cycle through recordings if display mode is 'recordings' or 'full'
            const allowRecordingsMode = currentDisplayMode === 'recordings' || currentDisplayMode === 'full';

            // If in recordings mode AND display allows it, go to previous quality recording
            if (allowRecordingsMode && isInRecordingsMode && currentSongIndex >= 0) {
                const song = allSongs[currentSongIndex];
                if (song && song.recordings) {
                    // Find previous recording that meets quality
                    for (let i = currentRecordingIndex - 1; i >= 0; i--) {
                        if (recordingMeetsQuality(song.recordings[i])) {
                            playNigunRecording(currentSongIndex, i, null);
                            return;
                        }
                    }
                }
            }

            // Otherwise, go to previous song in playlist (skip songs without quality audio)
            if (currentPlaylist.length > 0) {
                let newPosition = currentPlaylistPosition - 1;
                while (newPosition >= 0) {
                    const songIdx = currentPlaylist[newPosition];
                    const song = allSongs[songIdx];
                    if (song && songHasQualityAudio(song)) {
                        currentPlaylistPosition = newPosition;
                        isInRecordingsMode = false;
                        currentRecordingIndex = 0;

                        // If in recordings/full mode, start at the last quality recording of the previous song
                        if (allowRecordingsMode && song.recordings && song.recordings.length > 1) {
                            // Find last quality recording
                            for (let i = song.recordings.length - 1; i >= 0; i--) {
                                if (recordingMeetsQuality(song.recordings[i])) {
                                    isInRecordingsMode = true;
                                    currentRecordingIndex = i;
                                    playNigunRecording(songIdx, i, null);
                                    return;
                                }
                            }
                        }
                        // Play first quality recording
                        const firstQualityRec = getFirstQualityRecording(song);
                        if (firstQualityRec) {
                            const recIdx = song.recordings.indexOf(firstQualityRec);
                            playNigunRecording(songIdx, recIdx, null);
                        } else {
                            playSong(songIdx, currentPlaylist);
                        }
                        return;
                    }
                    newPosition--;
                }
            }
        }

        // Play next
        function playNext() {
            // Only cycle through recordings if display mode is 'recordings' or 'full'
            const allowRecordingsMode = currentDisplayMode === 'recordings' || currentDisplayMode === 'full';

            // If in recordings mode AND display allows it, go to next quality recording
            if (allowRecordingsMode && isInRecordingsMode && currentSongIndex >= 0) {
                const song = allSongs[currentSongIndex];
                if (song && song.recordings) {
                    // Find next recording that meets quality
                    for (let i = currentRecordingIndex + 1; i < song.recordings.length; i++) {
                        if (recordingMeetsQuality(song.recordings[i])) {
                            playNigunRecording(currentSongIndex, i, null);
                            return;
                        }
                    }
                }
            }

            // Otherwise, go to next song in playlist (skip songs without quality audio)
            if (currentPlaylist.length > 0) {
                let newPosition = currentPlaylistPosition + 1;
                while (newPosition < currentPlaylist.length) {
                    const songIdx = currentPlaylist[newPosition];
                    const song = allSongs[songIdx];
                    if (song && songHasQualityAudio(song)) {
                        currentPlaylistPosition = newPosition;
                        isInRecordingsMode = false;
                        currentRecordingIndex = 0;

                        // If in recordings/full mode and song has multiple quality recordings, enter recordings mode
                        if (allowRecordingsMode && song.recordings && song.recordings.length > 1) {
                            // Find first quality recording
                            const firstQualityRec = getFirstQualityRecording(song);
                            if (firstQualityRec) {
                                const recIdx = song.recordings.indexOf(firstQualityRec);
                                isInRecordingsMode = true;
                                playNigunRecording(songIdx, recIdx, null);
                            } else {
                                playSong(songIdx, currentPlaylist);
                            }
                        } else {
                            // Play first quality recording
                            const firstQualityRec = getFirstQualityRecording(song);
                            if (firstQualityRec) {
                                const recIdx = song.recordings.indexOf(firstQualityRec);
                                playNigunRecording(songIdx, recIdx, null);
                            } else {
                                playSong(songIdx, currentPlaylist);
                            }
                        }
                        return;
                    }
                    newPosition++;
                }
            }
        }

        // Seek to position
        function seekTo(event) {
            const container = event.currentTarget;
            const rect = container.getBoundingClientRect();
            const percent = (event.clientX - rect.left) / rect.width;
            audioPlayer.currentTime = percent * audioPlayer.duration;
        }

        // Format time
        function formatTime(seconds) {
            if (isNaN(seconds)) return '0:00';
            const mins = Math.floor(seconds / 60);
            const secs = Math.floor(seconds % 60);
            return `${mins}:${secs.toString().padStart(2, '0')}`;
        }

        // Audio event listeners
        audioPlayer.addEventListener('timeupdate', () => {
            const progress = (audioPlayer.currentTime / audioPlayer.duration) * 100;
            document.getElementById('progressBar').style.width = `${progress}%`;
            document.getElementById('currentTime').textContent = formatTime(audioPlayer.currentTime);
        });

        audioPlayer.addEventListener('loadedmetadata', () => {
            document.getElementById('duration').textContent = formatTime(audioPlayer.duration);
        });

        audioPlayer.addEventListener('ended', () => {
            playNext();
        });

        audioPlayer.addEventListener('pause', () => {
            updatePlayState(false);
        });

        audioPlayer.addEventListener('play', () => {
            updatePlayState(true);
        });

        // Update play/pause state for both player and song list
        function updatePlayState(isPlaying) {
            const playPauseBtn = document.getElementById('playPauseBtn');
            const equalizerIcon = document.querySelector('.equalizer-icon');

            // Update player button
            if (isPlaying) {
                playPauseBtn.classList.add('playing');
            } else {
                playPauseBtn.classList.remove('playing');
            }

            // Update equalizer icon in player-show-btn (pause when audio paused)
            if (equalizerIcon) {
                if (isPlaying) {
                    equalizerIcon.classList.remove('paused');
                } else {
                    equalizerIcon.classList.add('paused');
                }
            }

            // Update song list play buttons
            document.querySelectorAll('.song-play-btn-new').forEach(btn => {
                btn.classList.remove('playing');
            });

            // Update song items paused state
            document.querySelectorAll('.song-item').forEach(item => {
                item.classList.remove('paused');
            });

            // Remove active-recording class from all recordings
            document.querySelectorAll('.song-item-recording').forEach(rec => {
                rec.classList.remove('active-recording');
            });

            // Find the active song's button and update it
            if (currentSongIndex >= 0) {
                const activeBtn = document.querySelector(`.song-play-btn-new[data-song-idx="${currentSongIndex}"]:not([data-rec-idx])`);
                const activeItem = document.querySelector(`.song-item[data-song-idx="${currentSongIndex}"]`);

                if (activeBtn && isPlaying) {
                    activeBtn.classList.add('playing');
                }
                if (activeItem && !isPlaying) {
                    activeItem.classList.add('paused');
                }

                // Update active recording button
                const activeRecBtn = document.querySelector(`.rec-play-btn[data-song-idx="${currentSongIndex}"][data-rec-idx="${currentRecordingIndex}"]`);
                if (activeRecBtn && isPlaying) {
                    activeRecBtn.classList.add('playing');
                }

                // Highlight active recording row
                const activeRecRow = document.querySelector(`.song-item-recording[data-song-idx="${currentSongIndex}"][data-rec-idx="${currentRecordingIndex}"]`);
                if (activeRecRow) {
                    activeRecRow.classList.add('active-recording');
                }
            }
        }

        // Create clickable link for a field value
        function createFieldLink(value, fieldType) {
            if (!value) return '';

            // Split by comma for multiple values
            const values = value.split(',').map(v => v.trim()).filter(Boolean);

            return values.map(v => {
                let page = '';
                let filterKey = '';

                switch (fieldType) {
                    case 'mechaber':
                        page = 'mechabrim';
                        filterKey = 'mechaber';
                        break;
                    case 'chatzer':
                        page = 'chatzeros';
                        filterKey = 'chatzer';
                        break;
                    case 'verter':
                        page = 'verter';
                        filterKey = 'verter';
                        break;
                    case 'zman':
                        page = 'zmanim';
                        filterKey = 'zman';
                        break;
                    case 'collection':
                        page = 'collections';
                        filterKey = 'collection';
                        break;
                    case 'scale':
                        page = 'songs';
                        filterKey = 'scale';
                        break;
                    case 'ritem':
                        page = 'songs';
                        filterKey = 'ritem';
                        break;
                    case 'gezungen':
                        page = 'songs';
                        filterKey = 'gezungen';
                        break;
                    case 'maure':
                        page = 'songs';
                        filterKey = 'maure';
                        break;
                    default:
                        return v; // No link for other fields
                }

                return `<a href="#" class="field-link"
                    data-action="filter"
                    data-page="${page}"
                    data-filter-key="${filterKey}"
                    data-value="${escapeHtml(v)}"
                    data-close-modal="true"
                    data-tooltip-value="${escapeHtml(v)}"
                    data-tooltip-type="${filterKey}">${v}</a>`;
            }).join(' ');
        }

        // Navigate to page with filter applied (opens detail page)
        function navigateToWithFilter(page, filterKey, value) {
            // Handle scale, rhythm, gezungen, maure - filter songs page
            if (filterKey === 'scale' || filterKey === 'ritem' || filterKey === 'gezungen' || filterKey === 'maure') {
                // Reset all filters first
                activeFilters = {
                    chatzer: [],
                    mechaber: [],
                    verter: [],
                    zman: [],
                    collection: [],
                    scale: [],
                    ritem: [],
                    gezungen: [],
                    maure: []
                };

                // Set the specific filter
                activeFilters[filterKey] = [value];

                // Filter songs
                applyFilters();

                currentDetailView = null;
                currentPageView = 'songs';

                // Update nav buttons
                document.querySelectorAll('.nav-btn').forEach(btn => {
                    btn.classList.toggle('active', btn.dataset.page === 'songs');
                });

                updateURL();
                renderCurrentPage();
                window.scrollTo({ top: 0, behavior: 'smooth' });
                return;
            }

            // Map page to category key
            const categoryMap = {
                'chatzeros': 'chatzeros',
                'mechabrim': 'mechabrim',
                'verter': 'verter',
                'zmanim': 'zmanim',
                'collections': 'collections'
            };

            const categoryKey = categoryMap[page];
            if (categoryKey) {
                // Navigate to detail page for this item
                navigateToDetail(categoryKey, value);
            }
        }

        // Show song details - opens in modal overlay
        function showSongDetails(idx) {
            console.log('showSongDetails called with idx:', idx);
            const song = allSongs[idx];
            console.log('Song found:', song ? song.name : 'NOT FOUND');
            if (!song) return;

            // Open modal instead of navigating to full page
            openNigunModal(idx, song);
        }

        // Open nigun modal overlay
        function openNigunModal(idx, song) {
            // Save current scroll position
            savedScrollPosition = window.pageYOffset || document.documentElement.scrollTop;
            currentNigunModalIdx = idx;
            nigunModalOpen = true;

            // Update URL to nigun detail URL using pushState
            const nigunId = song.customId || song.rowId || song.name;
            const hash = `#/nigunim/${encodeURIComponent(nigunId)}`;
            history.pushState({ nigunModal: true, scrollPos: savedScrollPosition, idx: idx }, '', hash);

            // Render content in modal
            const modalBody = document.getElementById('nigunModalBody');
            renderNigunDetailInModal(modalBody, song.name, idx);

            // Show overlay
            const overlay = document.getElementById('nigunModalOverlay');
            overlay.classList.add('active');
            document.body.style.overflow = 'hidden'; // Prevent background scrolling
        }

        // Render nigun detail content inside modal (uses same logic as renderNigunDetailPage)
        function renderNigunDetailInModal(container, nigunName, idx) {
            // Find the song
            let songIdx = idx !== undefined ? idx : allSongs.findIndex(s => s.name === nigunName);
            if (songIdx === -1) {
                songIdx = allSongs.findIndex(s => s.rowId === nigunName);
            }
            if (songIdx === -1) {
                songIdx = allSongs.findIndex(s => s.customId === nigunName);
            }
            const song = allSongs[songIdx];

            if (!song) {
                container.innerHTML = '<p>ניגון נישט געפונען</p>';
                return;
            }

            // Use the existing render function but target the modal container
            // Create a temporary container to hold the content
            const tempContainer = document.createElement('div');
            renderNigunDetailPage(tempContainer, song.name);

            // Insert the rendered content into modal
            container.innerHTML = tempContainer.innerHTML;
        }

        // Close nigun modal overlay
        function closeNigunModal() {
            if (!nigunModalOpen) return;

            nigunModalOpen = false;
            currentNigunModalIdx = null;

            // Hide overlay
            const overlay = document.getElementById('nigunModalOverlay');
            overlay.classList.remove('active');
            document.body.style.overflow = ''; // Restore scrolling

            // Go back in history to restore previous URL
            history.back();
        }

        // Handle browser back/forward for nigun modal
        window.addEventListener('popstate', (e) => {
            // Check if we're closing the modal
            if (nigunModalOpen) {
                // Close modal without triggering another history change
                nigunModalOpen = false;
                currentNigunModalIdx = null;

                const overlay = document.getElementById('nigunModalOverlay');
                overlay.classList.remove('active');
                document.body.style.overflow = '';

                // Restore scroll position
                if (e.state && e.state.scrollPos !== undefined) {
                    window.scrollTo(0, e.state.scrollPos);
                } else {
                    window.scrollTo(0, savedScrollPosition);
                }
                return;
            }

            // Otherwise handle normal URL changes
            handleURLChange();
        });

        // Wrapper to show details without playing
        function showDetailsOnly(event, idx) {
            event.stopPropagation();
            event.preventDefault();
            showSongDetails(idx);
        }

        // Close modal (old song modal)
        function closeModal() {
            document.getElementById('songModal').classList.remove('show');
        }

        // Close modal on outside click
        document.getElementById('songModal').addEventListener('click', (e) => {
            if (e.target.id === 'songModal') closeModal();
        });

        // Close nigun modal with Escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && nigunModalOpen) {
                closeNigunModal();
            }
        });

        // PDF Popup functions
        function openPdfPopup(encodedUrl, encodedTitle) {
            const url = decodeURIComponent(encodedUrl);
            const title = decodeURIComponent(encodedTitle);

            // Use Google Docs viewer to display PDF inline
            const viewerUrl = `https://docs.google.com/viewer?url=${encodeURIComponent(url)}&embedded=true`;

            document.getElementById('pdfPopupTitle').textContent = title;
            document.getElementById('pdfPopupFrame').src = viewerUrl;
            document.getElementById('pdfDownloadLink').href = url;
            document.getElementById('pdfPopup').classList.add('show');
            document.body.style.overflow = 'hidden';
        }

        function closePdfPopup(event) {
            if (event && event.target !== document.getElementById('pdfPopup')) return;
            document.getElementById('pdfPopup').classList.remove('show');
            document.getElementById('pdfPopupFrame').src = '';
            document.body.style.overflow = '';
        }

        // Close PDF popup on Escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                closePdfPopup();
            }
        });

        // Global search
        function handleGlobalSearch(term) {
            // Don't search on every keystroke - wait for Enter
        }

        // Open search modal
        function openSearchModal() {
            const overlay = document.getElementById('searchModalOverlay');
            overlay.classList.add('active');
            // Focus on input
            setTimeout(() => {
                document.getElementById('searchModalInput').focus();
                // Initialize typing animation if enabled (and if function exists)
                if (typeof initTypingForInput === 'function') {
                    initTypingForInput('searchModalInput');
                }
            }, 100);
        }

        // Close search modal
        function closeSearchModal() {
            const overlay = document.getElementById('searchModalOverlay');
            overlay.classList.remove('active');
        }

        // Execute search from modal
        async function executeSearchFromModal() {
            const input = document.getElementById('searchModalInput');
            const term = input?.value?.trim();

            if (!term) return;

            closeSearchModal();

            // Show loading state
            const content = document.getElementById('mainContent');
            content.innerHTML = `
                <div class="page-theme theme-nigun">
                    <div class="page-title">
                        <div class="page-title-bar">זוכט "${term}"...</div>
                        <div class="page-title-content">
                            <div class="subtitle">לאודינג רעזולטאטן...</div>
                        </div>
                    </div>
                    <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 400px; gap: 16px;">
                        <div class="loader-medium"><span class="note">♪</span><span class="note">♫</span><span class="note">♪</span></div>
                        <p style="color: var(--text-secondary); font-size: 16px; margin: 0;">לאדינג רעזולטאטן...</p>
                    </div>
                </div>
            `;

            // Load all data needed for search
            await ensureDataForPage('search');

            searchQuery = term;
            currentPage = 1;
            currentPageView = 'search';
            currentDetailView = null;
            updateURL();
            renderCurrentPage();
        }

        // Execute search from results page
        async function executeSearchFromResults() {
            const input = document.getElementById('searchResultsInput');
            const term = input?.value?.trim();

            if (!term) return;

            // Show loading state
            const content = document.getElementById('mainContent');
            content.innerHTML = `
                <div class="page-theme theme-nigun">
                    <div class="page-title">
                        <div class="page-title-bar">זוכט "${term}"...</div>
                    </div>
                    <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 400px; gap: 16px;">
                        <div class="loader-medium"><span class="note">♪</span><span class="note">♫</span><span class="note">♪</span></div>
                        <p style="color: var(--text-secondary); font-size: 16px; margin: 0;">לאדינג רעזולטאטן...</p>
                    </div>
                </div>
            `;

            // Load all data needed for search
            await ensureDataForPage('search');

            searchQuery = term;
            currentPage = 1;
            currentPageView = 'search';
            currentDetailView = null;
            updateURL();
            renderCurrentPage();
        }

        // Search within songs only (from "see all" button)
        function searchInSongs(term) {
            // Set search query in songs page
            searchQuery = term;
            currentPage = 1;
            currentPageView = 'songs';
            currentDetailView = null;

            // Apply the search filter
            applyFilters();

            updateURL();
            renderCurrentPage();
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }

        function executeGlobalSearch() {
            const input = document.getElementById('globalSearch');
            const term = input?.value?.trim();

            if (!term) return;

            searchQuery = term;
            currentPage = 1;
            currentPageView = 'search';
            currentDetailView = null;
            updateURL();
            renderCurrentPage();
        }

        // Render search results page
        function renderSearchResultsPage(container) {
            const term = searchQuery.toLowerCase();

            // Helper function to search in an info object
            function searchInInfo(info, term) {
                if (!info) return false;
                // Search in all properties
                for (const key of Object.keys(info)) {
                    const val = info[key];
                    if (typeof val === 'string' && val.toLowerCase().includes(term)) return true;
                    if (Array.isArray(val) && val.some(v => typeof v === 'string' && v.toLowerCase().includes(term))) return true;
                }
                // Search in allData
                if (info.allData) {
                    for (const key of Object.keys(info.allData)) {
                        const val = info.allData[key];
                        if (typeof val === 'string' && val.toLowerCase().includes(term)) return true;
                    }
                }
                return false;
            }

            // Search in songs
            const matchingSongs = allSongs.filter(song => {

                const searchableText = [
                    song.name,
                    song.mechaber,
                    song.category,
                    song.verter,
                    song.collections,
                    song.pasigOif,
                    song.info,
                    song.siman
                ].filter(Boolean).join(' ').toLowerCase();
                return searchableText.includes(term);
            });

            // Search in chatzeros (use chatzerosInfo directly)
            const matchingChatzeros = Object.keys(chatzerosInfo || {}).filter(name => {
                if (name.toLowerCase().includes(term)) return true;
                return searchInInfo(chatzerosInfo?.[name], term);
            });

            // Search in mechabrim (use mechabrimInfo directly)
            const matchingMechabrim = Object.keys(mechabrimInfo || {}).filter(name => {
                if (name.toLowerCase().includes(term)) return true;
                return searchInInfo(mechabrimInfo?.[name], term);
            });

            // Search in collections (use collectionsInfo directly)
            const matchingCollections = Object.keys(collectionsInfo || {}).filter(name => {
                if (name.toLowerCase().includes(term)) return true;
                return searchInInfo(collectionsInfo?.[name], term);
            });

            // Search in verter (use verterInfo directly)
            const matchingVerter = Object.keys(verterInfo || {}).filter(name => {
                if (name.toLowerCase().includes(term)) return true;
                return searchInInfo(verterInfo?.[name], term);
            });

            // Search in zmanim (name and all info)
            const matchingZmanim = Object.keys(zmaninInfo || {}).filter(name => {
                if (name.toLowerCase().includes(term)) return true;
                return searchInInfo(zmaninInfo?.[name], term);
            });

            // Search in albums (name and all info)
            const matchingAlbums = Object.keys(albumsInfo || {}).filter(name => {
                if (name.toLowerCase().includes(term)) return true;
                return searchInInfo(albumsInfo?.[name], term);
            });

            // Search in scale
            const matchingScale = Object.keys(categories.scale || {}).filter(name =>
                name.toLowerCase().includes(term)
            );

            // Search in ritem
            const matchingRitem = Object.keys(categories.ritem || {}).filter(name =>
                name.toLowerCase().includes(term)
            );

            // Search in gezungen
            const matchingGezungen = Object.keys(categories.gezungen || {}).filter(name =>
                name.toLowerCase().includes(term)
            );

            const totalResults = matchingSongs.length + matchingChatzeros.length +
                matchingMechabrim.length + matchingCollections.length +
                matchingVerter.length + matchingZmanim.length + matchingAlbums.length +
                matchingScale.length + matchingRitem.length + matchingGezungen.length;

            let html = `
                <div class="page-theme theme-nigun">
                    <div class="page-title">
                        <div class="page-title-bar">רעזולטאטן פאר "${searchQuery}"</div>
                        <div class="page-title-content">
                            <div class="subtitle">${totalResults} רעזולטאטן געפונען</div>
                        </div>
                    </div>
                    
                    <div class="search-results-searchbar">
                        <input type="text" 
                               id="searchResultsInput" 
                               class="search-results-input" 
                               placeholder="זוכט עפעס אנדערש..."
                               value="${searchQuery}"
                               onkeydown="if(event.key==='Enter') executeSearchFromResults()">
                        <button class="search-results-btn" onclick="executeSearchFromResults()">🔍 זוכן</button>
                    </div>
            `;

            // Nigunim section - use song-item style
            if (matchingSongs.length > 0) {
                const displaySongs = matchingSongs.slice(0, 10);
                html += `
                    <div class="search-section">
                        <div class="search-section-header">
                            <h3 style="color: var(--color-nigun);">🎵 ניגונים (${matchingSongs.length})</h3>
                            ${matchingSongs.length > 10 ? `<button class="see-all-btn" onclick="searchInSongs('${searchQuery.replace(/'/g, "\\'")}')">זע אלע ${matchingSongs.length} →</button>` : ''}
                        </div>
                        <div class="list-layout">
                            ${displaySongs.map((song, idx) => {
                    const globalIdx = allSongs.indexOf(song);
                    const hasAudio = !!song.audioUrl;
                    // No longer need to escape - using data attributes now
                    // Build meta with entity-specific colored links
                    const metaParts = [];
                    if (song.mechaber) metaParts.push(createMetaLinks(song.mechaber, 'mechabrim', 'mechaber', 'mechaber'));
                    if (song.category) metaParts.push(createMetaLinks(song.category, 'chatzeros', 'chatzer', 'chatzer'));
                    return `
                                    <div class="song-item ${!hasAudio ? 'no-audio' : ''}" onclick="${hasAudio ? `playSong(${globalIdx})` : `showSongDetails(${globalIdx})`}">
                                        <div class="song-number">${idx + 1}</div>
                                        <div class="song-info">
                                            <div class="song-name">${song.name}</div>
                                            <div class="song-meta">${metaParts.join(' • ')}</div>
                                        </div>
                                        <button class="song-details-btn" onclick="event.stopPropagation(); showSongDetails(${globalIdx});">דעטאלן</button>
                                    </div>
                                `;
                }).join('')}
                        </div>
                    </div>
                `;
            }

            // Albums section
            if (matchingAlbums.length > 0) {
                html += `
                    <div class="search-section">
                        <div class="search-section-header">
                            <h3 style="color: var(--color-album);">💿 אלבומס (${matchingAlbums.length})</h3>
                        </div>
                        <div class="search-results-grid">
                            ${matchingAlbums.map(name => {
                    const albumData = albumsInfo[name];
                    return `
                                <div class="search-result-card result-album" data-action="detail" data-category="albums" data-name="${escapeHtml(name)}" style="border-right-color: var(--color-album);">
                                    <div class="result-name">${name}</div>
                                    <div class="result-count">${albumData?.songs?.length || 0} ניגונים</div>
                                </div>
                            `}).join('')}
                        </div>
                    </div>
                `;
            }

            // Chatzeros section
            if (matchingChatzeros.length > 0) {
                html += `
                    <div class="search-section">
                        <div class="search-section-header">
                            <h3 style="color: var(--color-chatzer);">🏛️ חצרות (${matchingChatzeros.length})</h3>
                        </div>
                        <div class="search-results-grid">
                            ${matchingChatzeros.map(name => `
                                <div class="search-result-card result-chatzer" data-action="detail" data-category="chatzeros" data-name="${escapeHtml(name)}" style="border-right-color: var(--color-chatzer);">
                                    <div class="result-name">${name}</div>
                                    <div class="result-count">${categories.chatzeros[name]?.length || 0} ניגונים</div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            // Mechabrim section
            if (matchingMechabrim.length > 0) {
                html += `
                    <div class="search-section">
                        <div class="search-section-header">
                            <h3 style="color: var(--color-mechaber);">👤 מחברים (${matchingMechabrim.length})</h3>
                        </div>
                        <div class="search-results-grid">
                            ${matchingMechabrim.map(name => `
                                <div class="search-result-card result-mechaber" data-action="detail" data-category="mechabrim" data-name="${escapeHtml(name)}" style="border-right-color: var(--color-mechaber);">
                                    <div class="result-name">${name}</div>
                                    <div class="result-count">${categories.mechabrim[name]?.length || 0} ניגונים</div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            // Collections section
            if (matchingCollections.length > 0) {
                html += `
                    <div class="search-section">
                        <div class="search-section-header">
                            <h3 style="color: var(--color-collection);">📁 קאלעקשאנס (${matchingCollections.length})</h3>
                        </div>
                        <div class="search-results-grid">
                            ${matchingCollections.map(name => `
                                <div class="search-result-card result-collection" data-action="detail" data-category="collections" data-name="${escapeHtml(name)}" style="border-right-color: var(--color-collection);">
                                    <div class="result-name">${name}</div>
                                    <div class="result-count">${categories.collections[name]?.length || 0} ניגונים</div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            // Verter section
            if (matchingVerter.length > 0) {
                html += `
                    <div class="search-section">
                        <div class="search-section-header">
                            <h3 style="color: var(--color-verter);">📝 ווערטער (${matchingVerter.length})</h3>
                        </div>
                        <div class="search-results-grid">
                            ${matchingVerter.map(name => `
                                <div class="search-result-card result-verter" data-action="detail" data-category="verter" data-name="${escapeHtml(name)}" style="border-right-color: var(--color-verter);">
                                    <div class="result-name">${name}</div>
                                    <div class="result-count">${categories.verter[name]?.length || 0} ניגונים</div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            // Zmanim section
            if (matchingZmanim.length > 0) {
                html += `
                    <div class="search-section">
                        <div class="search-section-header">
                            <h3 style="color: var(--color-zman);">📅 זמנים (${matchingZmanim.length})</h3>
                        </div>
                        <div class="search-results-grid">
                            ${matchingZmanim.map(name => `
                                <div class="search-result-card result-zman" data-action="zman-detail" data-name="${escapeHtml(name)}" style="border-right-color: var(--color-zman);">
                                    <div class="result-name">${name}</div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            // Gezungen section
            if (matchingGezungen.length > 0) {
                html += `
                    <div class="search-section">
                        <div class="search-section-header">
                            <h3 style="color: #2e8b2e;">🎤 געזונגען אויף (${matchingGezungen.length})</h3>
                        </div>
                        <div class="search-results-grid">
                            ${matchingGezungen.map(name => `
                                <div class="search-result-card" data-action="filter" data-page="nigunim" data-filter-key="gezungen" data-value="${escapeHtml(name)}" style="border-right-color: #2e8b2e;">
                                    <div class="result-name">${name}</div>
                                    <div class="result-count">${categories.gezungen[name]?.length || 0} ניגונים</div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            // Ritem section
            if (matchingRitem.length > 0) {
                html += `
                    <div class="search-section">
                        <div class="search-section-header">
                            <h3 style="color: var(--color-music);">🥁 ריטעם (${matchingRitem.length})</h3>
                        </div>
                        <div class="search-results-grid">
                            ${matchingRitem.map(name => `
                                <div class="search-result-card result-music" data-action="filter" data-page="nigunim" data-filter-key="ritem" data-value="${escapeHtml(name)}" style="border-right-color: var(--color-music);">
                                    <div class="result-name">${name}</div>
                                    <div class="result-count">${categories.ritem[name]?.length || 0} ניגונים</div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            // Scale section
            if (matchingScale.length > 0) {
                html += `
                    <div class="search-section">
                        <div class="search-section-header">
                            <h3 style="color: var(--color-nigun);">🎼 סקעיל (${matchingScale.length})</h3>
                        </div>
                        <div class="search-results-grid">
                            ${matchingScale.map(name => `
                                <div class="search-result-card result-nigun" data-action="filter" data-page="nigunim" data-filter-key="scale" data-value="${escapeHtml(name)}" style="border-right-color: var(--color-nigun);">
                                    <div class="result-name">${name}</div>
                                    <div class="result-count">${categories.scale[name]?.length || 0} ניגונים</div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            if (totalResults === 0) {
                html += `
                    <div class="no-results">
                        <div class="no-results-icon">🔍</div>
                        <p>קיין רעזולטאטן נישט געפונען פאר "${searchQuery}"</p>
                        <p class="no-results-hint">פרובירט א אנדער זוך</p>
                    </div>
                `;
            }

            html += `</div>`;
            container.innerHTML = html;
        }

        // Close dropdowns on outside click
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.filter-dropdown')) {
                document.querySelectorAll('.filter-dropdown-content').forEach(d => d.classList.remove('show'));
            }

            // Close sidebar dropdowns only when clicking outside
            if (!e.target.closest('.sidebar-filter-dropdown')) {
                document.querySelectorAll('.sidebar-dropdown-content').forEach(d => d.classList.remove('show'));
            }
        });

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.target.tagName === 'INPUT') return;

            if (e.code === 'Space') {
                e.preventDefault();
                togglePlayPause();
            } else if (e.code === 'ArrowRight') {
                playPrevious();
            } else if (e.code === 'ArrowLeft') {
                playNext();
            }
        });

        // Image modal functions
        function openImageModal(imageUrl) {
            let modal = document.getElementById('imageModal');
            if (!modal) {
                modal = document.createElement('div');
                modal.id = 'imageModal';
                modal.className = 'image-modal';
                modal.innerHTML = `
                    <button class="image-modal-close" onclick="closeImageModal()">×</button>
                    <img src="" id="imageModalImg">
                `;
                modal.onclick = function (e) {
                    if (e.target === modal) closeImageModal();
                };
                document.body.appendChild(modal);
            }
            document.getElementById('imageModalImg').src = imageUrl;
            modal.classList.add('active');
        }

        function closeImageModal() {
            const modal = document.getElementById('imageModal');
            if (modal) modal.classList.remove('active');
        }

        // Video modal functions
        function openVideoModal(videoUrl) {
            let modal = document.getElementById('videoModal');
            if (!modal) {
                modal = document.createElement('div');
                modal.id = 'videoModal';
                modal.className = 'video-modal';
                modal.innerHTML = `
                    <button class="video-modal-close" onclick="closeVideoModal()">×</button>
                    <video src="" id="videoModalVideo" controls autoplay></video>
                `;
                modal.onclick = function (e) {
                    if (e.target === modal) closeVideoModal();
                };
                document.body.appendChild(modal);
            }
            document.getElementById('videoModalVideo').src = videoUrl;
            modal.classList.add('active');
        }

        function closeVideoModal() {
            const modal = document.getElementById('videoModal');
            if (modal) {
                const video = document.getElementById('videoModalVideo');
                if (video) {
                    video.pause();
                    video.src = '';
                }
                modal.classList.remove('active');
            }
        }

        // Play audio from other media section (6th attachment)
        function playOtherMediaAudio(audioUrl, element) {
            audioPlayer.src = audioUrl;
            audioPlayer.play();
            showPlayer();

            // Update player UI with generic info
            document.getElementById('playerSongName').textContent = 'אודיאו';
            document.getElementById('playerSongMeta').innerHTML = '';
            document.getElementById('playerCollection').textContent = '';
            document.getElementById('playerGezungen').textContent = '';

            // Hide rating for other media audio
            const playerRating = document.getElementById('playerRating');
            if (playerRating) playerRating.style.display = 'none';

            // Update active state
            document.querySelectorAll('.nigun-recording-item').forEach(item => {
                item.classList.remove('active');
            });
            if (element) {
                element.classList.add('active');
            }
        }

        // Update player rating display - INTERACTIVE version
        function updatePlayerRating(rating) {
            const playerRating = document.getElementById('playerRating');
            const starsContainer = document.getElementById('playerRatingStars');
            if (!playerRating || !starsContainer) return;

            // Always show rating container (even if rating is 0, user can click to set one)
            playerRating.style.display = 'flex';
            playerRating.classList.remove('saving');

            // Generate all 5 clickable stars
            let starsHtml = '';
            for (let i = 0; i < 5; i++) {
                const starNum = i + 1;
                const isEmpty = i >= (rating || 0);
                starsHtml += `<span class="player-rating-star${isEmpty ? ' empty' : ''}" data-rating="${starNum}">★</span>`;
            }
            starsContainer.innerHTML = starsHtml;

            // Add hover preview
            const stars = starsContainer.querySelectorAll('.player-rating-star');
            stars.forEach((star, idx) => {
                star.addEventListener('mouseenter', () => {
                    // Show preview of what rating would be
                    stars.forEach((s, i) => {
                        if (i <= idx) {
                            s.classList.add('hover-preview');
                        } else {
                            s.classList.remove('hover-preview');
                        }
                    });
                });

                star.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const newRating = parseInt(star.dataset.rating, 10);
                    submitRating(newRating);
                });
            });

            starsContainer.addEventListener('mouseleave', () => {
                stars.forEach(s => s.classList.remove('hover-preview'));
            });
        }

        // Submit rating to Coda API
        async function submitRating(newRating) {
            if (!currentPlayingRecordingInfo) {
                console.log('No recording info available for rating');
                return;
            }

            const { songIdx, recordingIdx, slotIndex, rowId } = currentPlayingRecordingInfo;
            if (!rowId || slotIndex === undefined) {
                console.log('Missing rowId or slotIndex for rating update');
                return;
            }

            // Get the rating column ID for this recording slot
            const ratingColId = RECORDING_SLOTS[slotIndex]?.rating;
            if (!ratingColId) {
                console.log('No rating column ID for slot', slotIndex);
                return;
            }

            const playerRating = document.getElementById('playerRating');
            if (playerRating) {
                playerRating.classList.add('saving');
            }

            console.log(`Submitting rating ${newRating} for song ${songIdx}, recording ${recordingIdx}, slot ${slotIndex}`);

            try {
                // Coda API requires PUT to update a specific cell
                const url = `${WORKER_URL}docs/${DOC_ID}/tables/${TABLE_ID}/rows/${rowId}`;

                const response = await fetch(url, {
                    method: 'PUT',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        row: {
                            cells: [
                                {
                                    column: ratingColId,
                                    value: newRating
                                }
                            ]
                        }
                    })
                });

                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }

                console.log('Rating updated successfully');

                // Update local data
                if (allSongs[songIdx] && allSongs[songIdx].recordings && allSongs[songIdx].recordings[recordingIdx]) {
                    allSongs[songIdx].recordings[recordingIdx].rating = newRating;
                }

                // Clear local cache so next load gets fresh data
                localStorage.removeItem(`${CACHE_VERSION}_songs`);

                // Update the display
                updatePlayerRating(newRating);

            } catch (error) {
                console.error('Error submitting rating:', error);
                alert('פאלגענדער באריכטונג געשלאגן. ביטע פריווט נאכאמאל.');
            } finally {
                if (playerRating) {
                    playerRating.classList.remove('saving');
                }
            }
        }

        // Note: animateCards disabled for songs page to prevent unwanted animations

        // Register Service Worker for caching
        if ('serviceWorker' in navigator) {
            navigator.serviceWorker.register('/sw.js')
                .then((registration) => {
                    console.log('Service Worker registered:', registration.scope);
                })
                .catch((error) => {
                    console.log('Service Worker registration failed:', error);
                });
        }

        // Initialize
        fetchSongs();

        // Search Placeholder Typing Animation
        const searchPhrases = [
            "זוך א ניגון...",
            "זוך א מחבר...",
            "זוך א חצר...",
            "זוך אן אלבום...",
            "זוך א זמן...",
            "זוך א קאלעקשאן...",
            "זוך ווערטער...",
            "זוך א ריטעם..."
        ];

        function initTypingForInput(inputId) {
            const input = document.getElementById(inputId);
            if (!input) return;

            // Clear any existing interval/timeout if re-initializing (simple check)
            if (input.dataset.typingInitialized) return;
            input.dataset.typingInitialized = "true";

            let phraseIndex = 0;
            let charIndex = 0;
            let isDeleting = false;
            let typeSpeed = 150;

            function type() {
                // If input is no longer in DOM or not visible (optional check), maybe stop? 
                // but for now we keep it simple.

                const currentPhrase = searchPhrases[phraseIndex];

                if (isDeleting) {
                    input.placeholder = currentPhrase.substring(0, charIndex - 1);
                    charIndex--;
                    typeSpeed = 50; // Faster deletion
                } else {
                    input.placeholder = currentPhrase.substring(0, charIndex + 1);
                    charIndex++;
                    typeSpeed = 100; // Normal typing speed
                }

                if (!isDeleting && charIndex === currentPhrase.length) {
                    // Finished typing phrase
                    isDeleting = true;
                    typeSpeed = 2000; // Pause at end
                } else if (isDeleting && charIndex === 0) {
                    // Finished deleting phrase
                    isDeleting = false;
                    phraseIndex = (phraseIndex + 1) % searchPhrases.length;
                    typeSpeed = 500; // Pause before new phrase
                }

                // If element still exists
                if (document.getElementById(inputId)) {
                    setTimeout(type, typeSpeed);
                }
            }

            // Start typing immediately
            type();
        }

        // Start animation after a short delay to ensure load
        setTimeout(() => {
            initTypingForInput('homeSearchInput');
        }, 500);

        // Header hide on scroll down, show on scroll up
        (function () {
            const header = document.querySelector('.main-header');
            if (!header) return;

            let lastScrollY = window.scrollY;
            let ticking = false;

            function updateHeader() {
                const currentScrollY = window.scrollY;

                // Only hide if scrolled past 100px
                if (currentScrollY > 100) {
                    if (currentScrollY > lastScrollY) {
                        // Scrolling down - hide header
                        header.classList.add('header-hidden');
                    } else {
                        // Scrolling up - show header
                        header.classList.remove('header-hidden');
                    }
                } else {
                    // At top - always show header
                    header.classList.remove('header-hidden');
                }

                lastScrollY = currentScrollY;
                ticking = false;
            }

            window.addEventListener('scroll', function () {
                if (!ticking) {
                    window.requestAnimationFrame(updateHeader);
                    ticking = true;
                }
            }, { passive: true });
        })();
    </script>
