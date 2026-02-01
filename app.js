// Database Manager - centralizes all IndexedDB access
class DatabaseManager {
    constructor() {
        this.dbName = 'NewTubeDB';
        this.version = 1;
        this.db = null;
        this.initPromise = null;
        this.seeded = false;
        this.apiSyncPromise = null;
        this.metadataPath = '/metadata.db';
        this.api = new ApiClient('/api');
    }

    init() {
        if (this.initPromise) {
            return this.initPromise;
        }

        if (!('indexedDB' in window)) {
            console.warn('⚠️ IndexedDB not supported in this browser');
            this.initPromise = Promise.resolve(null);
            return this.initPromise;
        }

        this.initPromise = new Promise((resolve, reject) => {
            const request = indexedDB.open(this.dbName, this.version);

            request.onerror = () => reject(request.error);
            request.onupgradeneeded = (event) => this.setupStores(event.target.result);
            request.onsuccess = () => {
                this.db = request.result;

                this.seedFromMetadata()
                    .catch((error) => {
                        console.warn('⚠️ Metadata seeding skipped:', error);
                    })
                    .finally(() => {
                        resolve(this.db);
                        this.refreshFromApi().catch((error) => {
                            console.warn('⚠️ API sync failed:', error);
                        });
                    });
            };
        });

        return this.initPromise;
    }

    setupStores(db) {
        if (!db.objectStoreNames.contains('videos')) {
            const videoStore = db.createObjectStore('videos', { keyPath: 'videoid' });
            videoStore.createIndex('author', 'author', { unique: false });
            videoStore.createIndex('uploadDate', 'uploadDate', { unique: false });
        }

        if (!db.objectStoreNames.contains('shorts')) {
            const shortStore = db.createObjectStore('shorts', { keyPath: 'videoid' });
            shortStore.createIndex('author', 'author', { unique: false });
            shortStore.createIndex('uploadDate', 'uploadDate', { unique: false });
        }

        if (!db.objectStoreNames.contains('subtitles')) {
            db.createObjectStore('subtitles', { keyPath: 'videoid' });
        }

        if (!db.objectStoreNames.contains('comments')) {
            const commentStore = db.createObjectStore('comments', { autoIncrement: true });
            commentStore.createIndex('videoid', 'videoid', { unique: false });
            commentStore.createIndex('parentCommentId', 'parentCommentId', { unique: false });
        }
    }

    async seedFromMetadata() {
        if (!this.db || this.seeded) {
            return;
        }

        const hasData = await this.storeHasData('videos');
        if (hasData) {
            this.seeded = true;
            return;
        }

        try {
            const response = await fetch(this.metadataPath, { cache: 'no-store' });
            if (response.ok) {
                if (response.body && typeof response.body.cancel === 'function') {
                    await response.body.cancel();
                } else {
                    await response.arrayBuffer();
                }
            } else {
                throw new Error(`metadata fetch failed (${response.status})`);
            }
        } catch (error) {
            console.warn('⚠️ metadata.db not yet available:', error);
        }

        try {
            const payload = await this.api.fetchBootstrap();
            if (payload && typeof payload === 'object') {
                const videos = (payload.videos || []).map((row) => this.normalizeVideo(row));
                const shorts = (payload.shorts || []).map((row) => this.normalizeVideo(row));
                const subtitles = (payload.subtitles || []).map((row) => this.normalizeSubtitle(row));
                const comments = (payload.comments || []).map((row) => this.normalizeComment(row));

                await this.bulkReplace('videos', videos);
                await this.bulkReplace('shorts', shorts);
                await this.bulkReplace('subtitles', subtitles);
                await this.bulkReplace('comments', comments);
            }
        } catch (error) {
            console.warn('⚠️ Bootstrap seeding failed:', error);
        }

        this.seeded = true;
    }

    async storeHasData(storeName) {
        if (!this.db) {
            return false;
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction([storeName], 'readonly');
            const store = transaction.objectStore(storeName);
            const request = store.count();

            request.onsuccess = () => resolve(request.result > 0);
            request.onerror = () => reject(request.error);
        });
    }

    normalizeVideo(row) {
        const tags = this.parseJson(row.tags ?? row.tags_json ?? row.tagsJson, []);
        const thumbnails = this.parseJson(row.thumbnails ?? row.thumbnails_json ?? row.thumbnailsJson, []);
        const extras = this.parseJson(row.extras ?? row.extras_json ?? row.extrasJson, {});
        const sourcesRaw = this.parseJson(row.sources ?? row.sources_json ?? row.sourcesJson, []);
        const sources = sourcesRaw
            .map((source) => this.normalizeSource(source))
            .filter(Boolean);

        const duration = this.toNumber(row.duration);
        const durationText =
            row.durationText ||
            row.duration_text ||
            (typeof duration === 'number' ? this.secondsToReadable(duration) : null);

        const uploadDate = row.uploadDate || row.upload_date || null;
        const subscriberCount = this.toNumber(row.subscriberCount ?? row.subscriber_count);
        const thumbnailUrl =
            row.thumbnailUrl ||
            row.thumbnail_url ||
            (Array.isArray(thumbnails) && thumbnails.length > 0 ? thumbnails[0] : null);

        return {
            videoid: row.videoid,
            title: row.title,
            description: row.description || '',
            likes: this.toNumber(row.likes),
            dislikes: this.toNumber(row.dislikes),
            views: this.toNumber(row.views),
            uploadDate,
            author: row.author || null,
            subscriberCount,
            duration,
            durationText,
            channelUrl: row.channelUrl || row.channel_url || null,
            thumbnailUrl,
            tags,
            thumbnails,
            extras,
            sources
        };
    }

    normalizeSource(source) {
        if (!source) {
            return null;
        }

        const formatId = source.formatId || source.format_id || '';
        const qualityLabel =
            source.qualityLabel || source.quality_label || source.format_note || null;

        return {
            formatId,
            qualityLabel,
            width: this.toNumber(source.width),
            height: this.toNumber(source.height),
            fps: typeof source.fps === 'number' ? source.fps : this.toNumber(source.fps),
            mimeType: source.mimeType || source.mime_type || null,
            ext: source.ext || null,
            fileSize: this.toNumber(source.fileSize ?? source.file_size),
            url: source.url,
            path: source.path || null
        };
    }

    normalizeSubtitle(row) {
        const raw = this.parseJson(
            row.languages ?? row.languages_json ?? row.languagesJson,
            []
        );
        const languages = Array.isArray(raw)
            ? raw.map((track) => ({
                  code: track.code,
                  name: track.name,
                  url: track.url,
                  path: track.path || null
              }))
            : [];

        return {
            videoid: row.videoid,
            languages
        };
    }

    normalizeComment(row) {
        const parentCommentId =
            row.parentCommentId ?? row.parent_comment_id ?? null;

        return {
            id: row.id,
            videoid: row.videoid,
            author: row.author || '',
            text: row.text || '',
            likes: this.toNumber(row.likes),
            timePosted: row.timePosted || row.time_posted || null,
            parentCommentId,
            status_likedbycreator: Boolean(
                row.status_likedbycreator ?? row.statusLikedByCreator ?? row.statusLikedbycreator ?? 0
            ),
            replyCount: this.toNumber(row.replyCount ?? row.reply_count)
        };
    }

    parseJson(value, fallback) {
        if (Array.isArray(value) || (value && typeof value === 'object')) {
            return value;
        }

        if (typeof value === 'string') {
            try {
                return JSON.parse(value);
            } catch {
                return fallback;
            }
        }

        return fallback;
    }

    toNumber(value) {
        if (typeof value === 'number') {
            return Number.isNaN(value) ? null : value;
        }
        if (typeof value === 'string' && value.trim() !== '') {
            const parsed = Number(value);
            return Number.isNaN(parsed) ? null : parsed;
        }
        return null;
    }

    secondsToReadable(duration) {
        const totalSeconds = Math.max(0, Math.floor(duration));
        const hours = Math.floor(totalSeconds / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = totalSeconds % 60;

        if (hours > 0) {
            return `${hours}:${minutes.toString().padStart(2, '0')}:${seconds
                .toString()
                .padStart(2, '0')}`;
        }

        return `${minutes}:${seconds.toString().padStart(2, '0')}`;
    }

    async bulkInsert(storeName, records, options = {}) {
        if (!this.db || !records || records.length === 0) {
            return;
        }

        const { clearFirst = false } = options;

        await new Promise((resolve, reject) => {
            const transaction = this.db.transaction([storeName], 'readwrite');
            const store = transaction.objectStore(storeName);

            transaction.oncomplete = () => resolve();
            transaction.onerror = () => reject(transaction.error);
            transaction.onabort = () => reject(transaction.error);

            if (clearFirst) {
                store.clear();
            }

            records.forEach((record) => {
                if (record && typeof record === 'object') {
                    store.put(record);
                }
            });
        });
    }

    async bulkReplace(storeName, records) {
        await this.bulkInsert(storeName, records, { clearFirst: true });
    }

    async deleteCommentsForVideo(videoid) {
        if (!this.db) {
            return;
        }

        await new Promise((resolve, reject) => {
            const transaction = this.db.transaction(['comments'], 'readwrite');
            const store = transaction.objectStore('comments');
            const index = store.index('videoid');
            const range = IDBKeyRange.only(videoid);
            const request = index.openCursor(range);

            request.onsuccess = (event) => {
                const cursor = event.target.result;
                if (cursor) {
                    cursor.delete();
                    cursor.continue();
                }
            };
            request.onerror = () => reject(request.error);
            transaction.oncomplete = () => resolve();
            transaction.onerror = () => reject(transaction.error);
        });
    }

    async replaceComments(videoid, comments) {
        if (!this.db) {
            return;
        }

        await this.deleteCommentsForVideo(videoid);

        if (comments && comments.length > 0) {
            await this.bulkInsert('comments', comments);
        }
    }

    async refreshFromApi() {
        if (!this.db) {
            return;
        }

        if (this.apiSyncPromise) {
            return this.apiSyncPromise;
        }

        this.apiSyncPromise = (async () => {
            try {
                const payload = await this.api.fetchBootstrap().catch(() => null);

                if (payload) {
                    const videos = (payload.videos || []).map((item) => this.normalizeVideo(item));
                    const shorts = (payload.shorts || []).map((item) => this.normalizeVideo(item));
                    const subtitles = (payload.subtitles || []).map((item) => this.normalizeSubtitle(item));
                    const comments = (payload.comments || []).map((item) => this.normalizeComment(item));

                    if (videos.length > 0) {
                        await this.bulkInsert('videos', videos);
                    }
                    if (shorts.length > 0) {
                        await this.bulkInsert('shorts', shorts);
                    }
                    await this.bulkReplace('subtitles', subtitles);
                    await this.bulkReplace('comments', comments);
                    return;
                }

                const [videosFallback, shortsFallback] = await Promise.all([
                    this.api.fetchVideos().catch(() => []),
                    this.api.fetchShorts().catch(() => [])
                ]);

                if (Array.isArray(videosFallback) && videosFallback.length > 0) {
                    const normalized = videosFallback.map((item) => this.normalizeVideo(item));
                    await this.bulkInsert('videos', normalized);
                }

                if (Array.isArray(shortsFallback) && shortsFallback.length > 0) {
                    const normalized = shortsFallback.map((item) => this.normalizeVideo(item));
                    await this.bulkInsert('shorts', normalized);
                }
            } finally {
                this.apiSyncPromise = null;
            }
        })();

        return this.apiSyncPromise;
    }

    async fetchAndStoreMedia(videoid, storeName) {
        if (!this.db) {
            return;
        }

        try {
            const raw =
                storeName === 'videos'
                    ? await this.api.fetchVideo(videoid)
                    : await this.api.fetchShort(videoid);

            if (raw) {
                const normalized = this.normalizeVideo(raw);
                await this.bulkInsert(storeName, [normalized]);
            }
        } catch (error) {
            console.warn(`⚠️ Unable to refresh ${storeName} entry ${videoid}:`, error);
        }
    }

    async fetchCommentsFromApi(videoid) {
        if (!this.db) {
            return;
        }

        try {
            let comments = await this.api.fetchComments(videoid).catch(() => null);
            if (!Array.isArray(comments)) {
                comments = await this.api.fetchShortComments(videoid).catch(() => null);
            }
            if (Array.isArray(comments)) {
                const normalized = comments.map((item) => this.normalizeComment(item));
                await this.replaceComments(videoid, normalized);
            }
        } catch (error) {
            console.warn(`⚠️ Unable to refresh comments for ${videoid}:`, error);
        }
    }

    async getSettings() {
        return this.api.fetchSettings();
    }

    async updateSettings(settings) {
        return this.api.updateSettings(settings);
    }

    async startVideoDownload(videoId, mediaKind) {
        return this.api.startVideoDownload(videoId, mediaKind);
    }

    async startChannelDownload(videoId, mediaKind) {
        return this.api.startChannelDownload(videoId, mediaKind);
    }

    async getDownloadStatus(jobId) {
        return this.api.fetchDownloadStatus(jobId);
    }

    async getAllVideos() {
        await this.init();
        const videos = await this.getAllFromStore('videos');
        return this.sortByUploadDate(videos);
    }

    async getAllShorts() {
        await this.init();
        const shorts = await this.getAllFromStore('shorts');
        return this.sortByUploadDate(shorts);
    }

    sortByUploadDate(records) {
        return (records || []).slice().sort((a, b) => {
            const timeA = a.uploadDate ? Date.parse(a.uploadDate) : 0;
            const timeB = b.uploadDate ? Date.parse(b.uploadDate) : 0;
            return timeB - timeA;
        });
    }

    async getVideo(videoid) {
        await this.init();
        await this.fetchAndStoreMedia(videoid, 'videos');
        return this.getFromStore('videos', videoid);
    }

    async getShort(videoid) {
        await this.init();
        await this.fetchAndStoreMedia(videoid, 'shorts');
        return this.getFromStore('shorts', videoid);
    }

    async getSubtitles(videoid) {
        await this.init();
        return this.getFromStore('subtitles', videoid);
    }

    async getComments(videoid) {
        await this.init();
        await this.fetchCommentsFromApi(videoid);
        const comments = await this.readCommentsFromStore(videoid);
        return comments.filter((comment) => !comment.parentCommentId);
    }

    async getCommentReplies(commentId) {
        await this.init();
        return this.readRepliesFromStore(commentId);
    }

    async getAllFromStore(storeName) {
        if (!this.db) {
            return [];
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction([storeName], 'readonly');
            const store = transaction.objectStore(storeName);
            const request = store.getAll();

            request.onsuccess = () => resolve(request.result || []);
            request.onerror = () => reject(request.error);
        });
    }

    async getFromStore(storeName, key) {
        if (!this.db) {
            return null;
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction([storeName], 'readonly');
            const store = transaction.objectStore(storeName);
            const request = store.get(key);

            request.onsuccess = () => resolve(request.result || null);
            request.onerror = () => reject(request.error);
        });
    }

    async readCommentsFromStore(videoid) {
        if (!this.db) {
            return [];
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction(['comments'], 'readonly');
            const store = transaction.objectStore('comments');
            const index = store.index('videoid');
            const request = index.getAll(videoid);

            request.onsuccess = () => resolve(request.result || []);
            request.onerror = () => reject(request.error);
        });
    }

    async readRepliesFromStore(parentCommentId) {
        if (!this.db) {
            return [];
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction(['comments'], 'readonly');
            const store = transaction.objectStore('comments');
            const index = store.index('parentCommentId');
            const request = index.getAll(parentCommentId);

            request.onsuccess = () => resolve(request.result || []);
            request.onerror = () => reject(request.error);
        });
    }
}

class ApiClient {
    constructor(baseUrl = '/api') {
        this.baseUrl = baseUrl.replace(/\/$/, '');
    }

    async fetchJson(path) {
        const response = await fetch(`${this.baseUrl}${path}`, { cache: 'no-store' });
        if (!response.ok) {
            throw new Error(`Request failed (${response.status})`);
        }
        return response.json();
    }

    async sendJson(method, path, payload) {
        const response = await fetch(`${this.baseUrl}${path}`, {
            method,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });
        if (!response.ok) {
            throw new Error(`Request failed (${response.status})`);
        }
        return response.json();
    }

    postJson(path, payload) {
        return this.sendJson('POST', path, payload);
    }

    putJson(path, payload) {
        return this.sendJson('PUT', path, payload);
    }

    fetchVideos() {
        return this.fetchJson('/videos');
    }

    fetchShorts() {
        return this.fetchJson('/shorts');
    }

    fetchVideo(videoid) {
        return this.fetchJson(`/videos/${encodeURIComponent(videoid)}`);
    }

    fetchShort(videoid) {
        return this.fetchJson(`/shorts/${encodeURIComponent(videoid)}`);
    }

    fetchComments(videoid) {
        return this.fetchJson(`/videos/${encodeURIComponent(videoid)}/comments`);
    }

    fetchShortComments(videoid) {
        return this.fetchJson(`/shorts/${encodeURIComponent(videoid)}/comments`);
    }

    fetchBootstrap() {
        return this.fetchJson('/bootstrap');
    }

    fetchSettings() {
        return this.fetchJson('/settings');
    }

    updateSettings(settings) {
        return this.putJson('/settings', settings);
    }

    startVideoDownload(videoId, mediaKind) {
        return this.postJson('/downloads/video', {
            videoId,
            mediaKind
        });
    }

    startChannelDownload(videoId, mediaKind) {
        return this.postJson('/downloads/channel', {
            videoId,
            mediaKind
        });
    }

    fetchDownloadStatus(jobId) {
        return this.fetchJson(`/downloads/${encodeURIComponent(jobId)}`);
    }
}

// Global App Router
class App {
    constructor() {
        // Page routing table - maps page names to their class constructors and titles
        this.pages = {
            'home': {
                title: 'NewTube - Home',
                script: 'pageHome.js',
                class: null // Will be set after script loads
            },
            'watch': {
                title: 'NewTube - Watch',
                script: 'pageViewer.js',
                class: null
            },
            'shorts': {
                title: 'NewTube - Shorts',
                script: 'pageViewer.js',
                class: null
            },
            'admin': {
                title: 'NewTube - Admin',
                script: 'pageAdmin.js',
                class: null
            }
        };
        
        this.currentPage = null;
        this.currentPageInstance = null;
        this.database = new DatabaseManager();
        this.databaseReady = this.database.init().catch((error) => {
            console.error('❌ Failed to initialize database:', error);
            return null;
        });
    }

    // Change to a different page
    async changePage(pageName) {
        const pageConfig = this.pages[pageName];
        
        if (!pageConfig) {
            console.error(`Page "${pageName}" not found`);
            return;
        }

        // Close the current page if one exists
        if (this.currentPageInstance && this.currentPageInstance.close) {
            this.currentPageInstance.close();
            this.currentPageInstance = null;
        }

        // Set the page title
        document.title = pageConfig.title;

        // Prepare page-level services (database access, etc.)
        const pageServices = this.getPageServices(pageName);

        // Load the page script dynamically if not already loaded
        if (!pageConfig.class) {
            await this.loadScript(pageConfig.script);
            
            // Map the loaded class based on page name
            switch(pageName) {
                case 'home':
                    pageConfig.class = HomePage;
                    break;
                case 'watch':
                    pageConfig.class = ViewerPage;
                    break;
                case 'shorts':
                    pageConfig.class = ViewerPage;
                    break;
                case 'admin':
                    pageConfig.class = AdminPage;
                    break;
            }
        }

        // Create and initialize new page instance
        this.currentPageInstance = new pageConfig.class(pageServices);
        await this.currentPageInstance.init();
        this.currentPage = pageName;
    }

    // Dynamically load a JavaScript file
    loadScript(src) {
        return new Promise((resolve, reject) => {
            // Check if script already loaded
            const existingScript = document.querySelector(`script[src="${src}"]`);
            if (existingScript) {
                resolve();
                return;
            }

            const script = document.createElement('script');
            script.src = src;
            script.onload = () => resolve();
            script.onerror = () => reject(new Error(`Failed to load script: ${src}`));
            document.body.appendChild(script);
        });
    }

    // Initialize the app
    init() {
        // Determine which page to load based on URL
        const path = window.location.pathname;
        
        if (path.startsWith('/admin')) {
            this.changePage('admin');
        } else if (path.startsWith('/watch')) {
            this.changePage('watch');
        } else if (path.startsWith('/shorts/')) {
            this.changePage('shorts');
        } else {
            // Load the home page by default
            this.changePage('home');
        }
    }

    // Provide per-page service hooks while keeping database access centralized
    getPageServices(pageName) {
        if (pageName === 'home') {
            return {
                ready: () => this.databaseReady,
                getVideos: () => this.database.getAllVideos(),
                getShorts: () => this.database.getAllShorts()
            };
        }

        if (pageName === 'watch' || pageName === 'shorts') {
            return {
                ready: () => this.databaseReady,
                getVideo: (videoId) => this.database.getVideo(videoId),
                getShort: (videoId) => this.database.getShort(videoId),
                getSubtitles: (videoId) => this.database.getSubtitles(videoId),
                getComments: (videoId) => this.database.getComments(videoId),
                getCommentReplies: (commentId) => this.database.getCommentReplies(commentId),
                getSettings: () => this.database.getSettings(),
                startVideoDownload: (videoId, mediaKind) =>
                    this.database.startVideoDownload(videoId, mediaKind),
                startChannelDownload: (videoId, mediaKind) =>
                    this.database.startChannelDownload(videoId, mediaKind),
                getDownloadStatus: (jobId) => this.database.getDownloadStatus(jobId)
            };
        }

        if (pageName === 'admin') {
            return {
                ready: () => this.databaseReady,
                getSettings: () => this.database.getSettings(),
                updateSettings: (settings) => this.database.updateSettings(settings)
            };
        }

        return {
            ready: () => Promise.resolve()
        };
    }
}

if (typeof document !== 'undefined' && typeof window !== 'undefined' && !window.__NEWTUBE_TEST__) {
    document.addEventListener('DOMContentLoaded', () => {
        const app = new App();
        app.init();
        
        // Register service worker with better error handling
        if ('serviceWorker' in navigator) {
            // Check if we're on a secure context (HTTPS or localhost)
            if (window.isSecureContext) {
                navigator.serviceWorker.register('/sw.js')
                    .then((registration) => {
                        console.log('✅ Service Worker registered successfully:', registration.scope);
                    })
                    .catch((error) => {
                        // Handle different error types
                        if (error.name === 'NotSupportedError') {
                            console.warn('⚠️ Service Worker not supported or blocked by browser settings');
                            console.warn('💡 This may be due to:');
                            console.warn('   - Browser privacy settings blocking Service Workers');
                            console.warn('   - Incognito/Private browsing mode');
                            console.warn('   - Non-standard port restrictions');
                            console.warn('   - The app will work, but without offline caching');
                        } else if (error.name === 'SecurityError') {
                            console.warn('⚠️ Service Worker blocked due to security policy');
                        } else {
                            console.error('❌ Service Worker registration failed:', error);
                        }
                    });
            } else {
                console.warn('⚠️ Service Workers require a secure context (HTTPS)');
            }
        } else {
            console.warn('⚠️ Service Workers not supported in this browser');
        }
    });
}

if (typeof module !== 'undefined' && module.exports) {
    module.exports = { DatabaseManager, ApiClient, App };
}
