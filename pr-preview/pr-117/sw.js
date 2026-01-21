// Service Worker for Oitzer Hanigunim
const CACHE_NAME = 'oitzer-hanigunim-v1';
const API_CACHE_NAME = 'oitzer-api-cache-v1';
const API_CACHE_DURATION = 5 * 60 * 1000; // 5 minutes

// Files to cache immediately
const STATIC_ASSETS = [
    '/',
    '/index.html'
];

// Install event - cache static assets
self.addEventListener('install', (event) => {
    console.log('[SW] Installing...');
    self.skipWaiting();
});

// Activate event - clean old caches
self.addEventListener('activate', (event) => {
    console.log('[SW] Activating...');
    event.waitUntil(
        caches.keys().then((cacheNames) => {
            return Promise.all(
                cacheNames.map((cacheName) => {
                    if (cacheName !== CACHE_NAME && cacheName !== API_CACHE_NAME) {
                        console.log('[SW] Deleting old cache:', cacheName);
                        return caches.delete(cacheName);
                    }
                })
            );
        }).then(() => self.clients.claim())
    );
});

// Fetch event - serve from cache or network
self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);
    
    // Handle API requests (oitzerhanigunim.org/api)
    if (url.hostname.includes('oitzerhanigunim.org') && url.pathname.startsWith('/api')) {
        event.respondWith(handleApiRequest(event.request));
        return;
    }
    
    // For other requests, use network first, then cache
    event.respondWith(
        fetch(event.request)
            .then((response) => {
                // Cache successful responses
                if (response.ok) {
                    const responseClone = response.clone();
                    caches.open(CACHE_NAME).then((cache) => {
                        cache.put(event.request, responseClone);
                    });
                }
                return response;
            })
            .catch(() => {
                // If network fails, try cache
                return caches.match(event.request);
            })
    );
});

// Handle API requests with cache-first strategy
async function handleApiRequest(request) {
    const cache = await caches.open(API_CACHE_NAME);
    const cacheKey = request.url;
    
    // Check if we have a cached response
    const cachedResponse = await cache.match(cacheKey);
    
    if (cachedResponse) {
        // Check if cache is still fresh
        const cachedTime = cachedResponse.headers.get('x-cached-time');
        const now = Date.now();
        
        if (cachedTime && (now - parseInt(cachedTime)) < API_CACHE_DURATION) {
            console.log('[SW] Serving from cache:', cacheKey);
            return cachedResponse;
        }
    }
    
    // Fetch from network
    try {
        console.log('[SW] Fetching from network:', cacheKey);
        const networkResponse = await fetch(request);
        
        if (networkResponse.ok) {
            // Clone the response and add cache timestamp
            const responseData = await networkResponse.clone().text();
            const headers = new Headers(networkResponse.headers);
            headers.set('x-cached-time', Date.now().toString());
            
            const cachedResponse = new Response(responseData, {
                status: networkResponse.status,
                statusText: networkResponse.statusText,
                headers: headers
            });
            
            // Store in cache
            await cache.put(cacheKey, cachedResponse.clone());
            
            return new Response(responseData, {
                status: networkResponse.status,
                statusText: networkResponse.statusText,
                headers: networkResponse.headers
            });
        }
        
        return networkResponse;
    } catch (error) {
        console.log('[SW] Network failed, trying cache:', error);
        
        // If network fails, return cached response even if stale
        if (cachedResponse) {
            return cachedResponse;
        }
        
        throw error;
    }
}

// Listen for messages from the main thread
self.addEventListener('message', (event) => {
    if (event.data === 'clearCache') {
        console.log('[SW] Clearing all caches...');
        caches.keys().then((cacheNames) => {
            return Promise.all(
                cacheNames.map((cacheName) => caches.delete(cacheName))
            );
        }).then(() => {
            event.ports[0].postMessage('Cache cleared');
        });
    }
    
    if (event.data === 'getCacheStatus') {
        caches.open(API_CACHE_NAME).then((cache) => {
            cache.keys().then((keys) => {
                event.ports[0].postMessage({
                    cachedItems: keys.length
                });
            });
        });
    }
});
