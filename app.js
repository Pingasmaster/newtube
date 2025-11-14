// Database Manager - centralizes all IndexedDB access
class DatabaseManager {
    constructor() {
        this.dbName = 'ViewTubeDB';
        this.version = 1;
        this.db = null;
        this.initPromise = null;
    }

    // Initialize the IndexedDB database (singleton)
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
            request.onsuccess = () => {
                this.db = request.result;
                resolve(this.db);
            };

            request.onupgradeneeded = (event) => {
                const db = event.target.result;

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
            };
        });

        return this.initPromise;
    }

    // Check if the database is ready for transactions
    isReady() {
        return !!this.db;
    }

    // Fetch a regular video by ID
    getVideo(videoid) {
        if (!this.db) {
            return Promise.resolve(null);
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction(['videos'], 'readonly');
            const store = transaction.objectStore('videos');
            const request = store.get(videoid);

            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    // Fetch a short by ID
    getShort(videoid) {
        if (!this.db) {
            return Promise.resolve(null);
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction(['shorts'], 'readonly');
            const store = transaction.objectStore('shorts');
            const request = store.get(videoid);

            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    // Fetch subtitles for a video
    getSubtitles(videoid) {
        if (!this.db) {
            return Promise.resolve(null);
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction(['subtitles'], 'readonly');
            const store = transaction.objectStore('subtitles');
            const request = store.get(videoid);

            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    // Fetch all comments for a video
    getComments(videoid) {
        if (!this.db) {
            return Promise.resolve([]);
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction(['comments'], 'readonly');
            const store = transaction.objectStore('comments');
            const index = store.index('videoid');
            const request = index.getAll(videoid);

            request.onsuccess = () => {
                const comments = request.result.filter((comment) => !comment.parentCommentId);
                resolve(comments);
            };
            request.onerror = () => reject(request.error);
        });
    }

    // Fetch replies for a given comment
    getCommentReplies(commentId) {
        if (!this.db) {
            return Promise.resolve([]);
        }

        return new Promise((resolve, reject) => {
            const transaction = this.db.transaction(['comments'], 'readonly');
            const store = transaction.objectStore('comments');
            const index = store.index('parentCommentId');
            const request = index.getAll(commentId);

            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }
}

// Global App Router
class App {
    constructor() {
        // Page routing table - maps page names to their class constructors and titles
        this.pages = {
            'home': {
                title: 'ViewTube - Home',
                script: 'pageHome.js',
                class: null // Will be set after script loads
            },
            'watch': {
                title: 'ViewTube - Watch',
                script: 'pageViewer.js',
                class: null
            },
            'shorts': {
                title: 'ViewTube - Shorts',
                script: 'pageViewer.js',
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
        
        if (path.startsWith('/watch')) {
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
        if (pageName === 'watch' || pageName === 'shorts') {
            return {
                ready: () => this.databaseReady,
                getVideo: (videoId) => this.database.getVideo(videoId),
                getShort: (videoId) => this.database.getShort(videoId),
                getSubtitles: (videoId) => this.database.getSubtitles(videoId),
                getComments: (videoId) => this.database.getComments(videoId),
                getCommentReplies: (commentId) => this.database.getCommentReplies(commentId)
            };
        }

        return {
            ready: () => Promise.resolve()
        };
    }
}

// Initialize app when DOM is ready
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
