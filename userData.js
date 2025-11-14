class UserDataStore {
    constructor(storageKey = 'viewtube:userData') {
        this.storageKey = storageKey;
        this.data = this.loadData();
        this.ensureDefaults();
    }

    isStorageAvailable() {
        try {
            return typeof window !== 'undefined' && 'localStorage' in window && window.localStorage != null;
        } catch (error) {
            console.warn('⚠️ localStorage unavailable:', error);
            return false;
        }
    }

    loadData() {
        if (!this.isStorageAvailable()) {
            return this.createEmptyState();
        }

        try {
            const raw = window.localStorage.getItem(this.storageKey);
            if (!raw) {
                return this.createEmptyState();
            }
            const parsed = JSON.parse(raw);
            if (parsed && typeof parsed === 'object') {
                return this.normalizeState(parsed);
            }
        } catch (error) {
            console.warn('⚠️ Failed to parse stored user data, resetting to default.', error);
        }

        return this.createEmptyState();
    }

    createEmptyState() {
        return {
            version: 1,
            likes: {},
            dislikes: {},
            playlists: {},
            subscriptions: {},
            watchHistory: {},
            metadata: {
                createdAt: Date.now(),
                updatedAt: Date.now()
            }
        };
    }

    normalizeState(state) {
        const normalized = this.createEmptyState();
        normalized.version = state.version || 1;
        normalized.likes = state.likes || {};
        normalized.dislikes = state.dislikes || {};
        normalized.playlists = state.playlists || {};
        normalized.subscriptions = state.subscriptions || {};
        normalized.watchHistory = state.watchHistory || {};
        normalized.metadata = Object.assign({}, normalized.metadata, state.metadata || {});
        return normalized;
    }

    ensureDefaults() {
        if (!this.data.playlists || Object.keys(this.data.playlists).length === 0) {
            this.data.playlists = {};
            this.addPlaylist('Favorites');
        }
    }

    save(eventType = 'update') {
        this.data.metadata.updatedAt = Date.now();
        if (this.isStorageAvailable()) {
            try {
                window.localStorage.setItem(this.storageKey, JSON.stringify(this.data));
            } catch (error) {
                console.warn('⚠️ Failed to persist user data:', error);
            }
        }

        window.dispatchEvent(
            new CustomEvent('viewtube:userdata', {
                detail: {
                    type: eventType,
                    data: this.getSnapshot()
                }
            })
        );
    }

    getSnapshot() {
        return JSON.parse(JSON.stringify(this.data));
    }

    buildVideoMetadata(metadata = {}) {
        return {
            videoid: metadata.videoid || null,
            title: metadata.title || null,
            author: metadata.author || null,
            channelId: metadata.channelId || null,
            thumbnail: metadata.thumbnail || null,
            savedAt: Date.now()
        };
    }

    normalizeKey(value = '') {
        return value.trim().toLowerCase().replace(/\s+/g, '-').slice(0, 64) || 'default';
    }

    addPlaylist(name) {
        const key = this.normalizeKey(name || 'Favorites');
        if (!this.data.playlists[key]) {
            this.data.playlists[key] = {
                id: key,
                name: name || 'Favorites',
                videoIds: [],
                createdAt: Date.now(),
                updatedAt: Date.now()
            };
            this.save('playlist');
        }
        return this.data.playlists[key];
    }

    addToPlaylist(name, videoId, metadata = {}) {
        if (!videoId) return null;
        const playlist = this.addPlaylist(name || 'Favorites');
        if (!playlist.videoIds.includes(videoId)) {
            playlist.videoIds.push(videoId);
            playlist.updatedAt = Date.now();
            playlist.lastEntry = this.buildVideoMetadata(Object.assign({}, metadata, { videoid: videoId }));
            this.save('playlist');
        }
        return playlist;
    }

    getWatchEntry(videoId) {
        return this.data.watchHistory[videoId] || null;
    }

    getWatchProgress(videoId) {
        const entry = this.getWatchEntry(videoId);
        return entry && typeof entry.progress === 'number' ? entry.progress : 0;
    }

    isWatched(videoId) {
        const entry = this.getWatchEntry(videoId);
        return Boolean(entry && entry.watched);
    }

    setWatchProgress(videoId, progress, metadata = {}) {
        if (!videoId || typeof progress !== 'number') return;
        const clamped = Math.max(0, Math.min(1, progress));
        const quantized = Math.round(clamped * 100) / 100;
        const existing = this.getWatchEntry(videoId) || {};
        this.data.watchHistory[videoId] = Object.assign(existing, {
            progress: quantized,
            updatedAt: Date.now(),
            title: metadata.title || existing.title || null,
            author: metadata.author || existing.author || null,
            thumbnail: metadata.thumbnail || existing.thumbnail || null,
            watched: quantized >= 0.9 || existing.watched === true
        });
        this.save('watch');
    }

    markWatched(videoId, metadata = {}) {
        this.setWatchProgress(videoId, 1, metadata);
    }

    toggleLike(videoId, metadata = {}) {
        if (!videoId) return 'none';
        if (this.data.likes[videoId]) {
            delete this.data.likes[videoId];
        } else {
            this.data.likes[videoId] = this.buildVideoMetadata(Object.assign({}, metadata, { videoid: videoId }));
            delete this.data.dislikes[videoId];
        }
        this.save('reaction');
        return this.getReaction(videoId);
    }

    toggleDislike(videoId, metadata = {}) {
        if (!videoId) return 'none';
        if (this.data.dislikes[videoId]) {
            delete this.data.dislikes[videoId];
        } else {
            this.data.dislikes[videoId] = this.buildVideoMetadata(Object.assign({}, metadata, { videoid: videoId }));
            delete this.data.likes[videoId];
        }
        this.save('reaction');
        return this.getReaction(videoId);
    }

    getReaction(videoId) {
        if (this.data.likes[videoId]) return 'like';
        if (this.data.dislikes[videoId]) return 'dislike';
        return 'none';
    }

    toggleSubscription(channelId, metadata = {}) {
        if (!channelId) return false;
        if (this.data.subscriptions[channelId]) {
            delete this.data.subscriptions[channelId];
            this.save('subscription');
            return false;
        }
        this.data.subscriptions[channelId] = {
            channelId,
            name: metadata.name || metadata.title || channelId,
            channelUrl: metadata.channelUrl || null,
            subscribedAt: Date.now()
        };
        this.save('subscription');
        return true;
    }

    isSubscribed(channelId) {
        return Boolean(channelId && this.data.subscriptions[channelId]);
    }

    getStats() {
        return {
            likes: Object.keys(this.data.likes).length,
            dislikes: Object.keys(this.data.dislikes).length,
            playlists: Object.keys(this.data.playlists).length,
            subscriptions: Object.keys(this.data.subscriptions).length,
            watched: Object.keys(this.data.watchHistory).filter((id) => this.data.watchHistory[id].watched).length
        };
    }

    exportToString() {
        return JSON.stringify({
            exportedAt: new Date().toISOString(),
            data: this.getSnapshot()
        }, null, 2);
    }

    downloadExport(filename = 'viewtube-user-data.json') {
        const blob = new Blob([this.exportToString()], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }

    importFromString(jsonString) {
        try {
            const parsed = JSON.parse(jsonString);
            const data = parsed && parsed.data ? parsed.data : parsed;
            this.data = this.normalizeState(data || {});
            this.ensureDefaults();
            this.save('import');
            return true;
        } catch (error) {
            console.error('❌ Failed to import user data:', error);
            throw error;
        }
    }
}

if (typeof window !== 'undefined') {
    window.UserDataStore = UserDataStore;
    window.userDataStore = new UserDataStore();
}
