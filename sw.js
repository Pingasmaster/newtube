// Service Worker for newtube
const CACHE_NAME = 'newtube-static-v3';

// List of files to cache on install
const urlsToCache = [
    '/',
    '/index.html',
    '/app.js',
    '/userData.js',
    '/pageHome.js',
    '/pageViewer.js',
    '/pageAdmin.js',
    '/styles.css',
    '/Roboto-VariableFont_wdth,wght.ttf',
    '/Roboto-Italic-VariableFont_wdth,wght.ttf'
];

// Install event - cache all files
self.addEventListener('install', (event) => {
    console.log('[Service Worker] Installing...');
    
    event.waitUntil(
        caches.open(CACHE_NAME)
            .then((cache) => {
                console.log('[Service Worker] Caching all files');
                return cache.addAll(urlsToCache);
            })
            .then(() => {
                console.log('[Service Worker] All files cached successfully');
                return self.skipWaiting(); // Activate immediately
            })
            .catch((error) => {
                console.error('[Service Worker] Caching failed:', error);
            })
    );
});

// Activate event - clean up old caches
self.addEventListener('activate', (event) => {
    console.log('[Service Worker] Activating...');
    
    event.waitUntil(
        caches.keys()
            .then((cacheNames) => {
                return Promise.all(
                    cacheNames.map((cacheName) => {
                        if (cacheName !== CACHE_NAME) {
                            console.log('[Service Worker] Deleting old cache:', cacheName);
                            return caches.delete(cacheName);
                        }
                    })
                );
            })
            .then(() => {
                console.log('[Service Worker] Activated successfully');
                return self.clients.claim(); // Take control immediately
            })
    );
});

// Fetch event - serve from cache, fallback to network
self.addEventListener('fetch', (event) => {
    const { request } = event;

    if (request.method !== 'GET') {
        return;
    }

    const url = new URL(request.url);
    const isApiRequest = url.pathname.startsWith('/api/');
    const isStreamRequest = url.pathname.includes('/streams/');
    const isMetadataFile = url.pathname.endsWith('/metadata.db');

    if (isApiRequest || isStreamRequest || isMetadataFile) {
        // Never cache dynamic API/video responses
        return;
    }

    const isNavigate = request.mode === 'navigate';
    const isStaticAsset = urlsToCache.includes(url.pathname);

    if (isNavigate) {
        event.respondWith(
            fetch(request).catch(() => caches.match('/index.html'))
        );
        return;
    }

    if (!isStaticAsset) {
        return;
    }

    event.respondWith(
        caches.match(request).then((cachedResponse) => {
            if (cachedResponse) {
                return cachedResponse;
            }

            return fetch(request)
                .then((response) => {
                    if (!response || response.status !== 200 || response.type !== 'basic') {
                        return response;
                    }

                    const responseToCache = response.clone();

                    caches.open(CACHE_NAME).then((cache) => {
                        cache.put(request, responseToCache);
                    });

                    return response;
                })
                .catch((error) => {
                    console.error('[Service Worker] Fetch failed:', error);
                    throw error;
                });
        })
    );
});

// Message event - for manual cache updates
self.addEventListener('message', (event) => {
    if (event.data && event.data.type === 'SKIP_WAITING') {
        self.skipWaiting();
    }
    
    if (event.data && event.data.type === 'CLEAR_CACHE') {
        event.waitUntil(
            caches.keys().then((cacheNames) => {
                return Promise.all(
                    cacheNames.map((cacheName) => caches.delete(cacheName))
                );
            })
        );
    }
});
