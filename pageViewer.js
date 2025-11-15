// Base Viewer Class
class BaseViewer {
    constructor(services) {
        this.services = services;
        this.container = null;
        this.videoData = null;
    }

    async init() {
        this.container = document.getElementById('app');
    }

    render() {
        throw new Error('render() must be implemented');
    }

    close() {
        if (this.container) {
            this.container.innerHTML = '';
        }
    }

    getSourceQualityValue(source) {
        if (!source) {
            return 0;
        }

        if (typeof source.height === 'number') {
            return source.height;
        }

        if (typeof source.qualityLabel === 'string') {
            const match = source.qualityLabel.match(/(\d{3,4})/);
            if (match) {
                return parseInt(match[1], 10);
            }
        }

        if (typeof source.formatId === 'string') {
            const match = source.formatId.match(/(\d{3,4})/);
            if (match) {
                return parseInt(match[1], 10);
            }
        }

        return 0;
    }

    getSourceLabel(source) {
        if (!source) {
            return 'Source';
        }

        if (source.qualityLabel) {
            return source.qualityLabel;
        }

        if (typeof source.height === 'number') {
            return `${source.height}p`;
        }

        if (typeof source.formatId === 'string') {
            return source.formatId;
        }

        return 'Source';
    }

    getSourceMimeType(source) {
        if (source && source.mimeType) {
            return source.mimeType;
        }
        return 'video/mp4';
    }

    sortSources(sources = []) {
        if (!Array.isArray(sources)) {
            return [];
        }
        return [...sources].sort((a, b) => {
            const qualityA = this.getSourceQualityValue(a);
            const qualityB = this.getSourceQualityValue(b);
            return qualityB - qualityA;
        });
    }

    sanitizeMediaUrl(url) {
        if (!url || typeof url !== 'string') {
            return null;
        }
        const trimmed = url.trim();
        if (!trimmed) {
            return null;
        }
        const lower = trimmed.toLowerCase();
        const isHttp = lower.startsWith('http://') || lower.startsWith('https://');
        const isRelative = trimmed.startsWith('/');
        const isBlob = lower.startsWith('blob:');
        if (!isHttp && !isRelative && !isBlob) {
            return null;
        }
        return trimmed;
    }

    normalizeLanguageCode(value) {
        if (!value || typeof value !== 'string') {
            return '';
        }
        const normalized = value.toLowerCase().replace(/[^a-z0-9-]/g, '').slice(0, 16);
        return normalized;
    }

    prepareMediaElement(player, sources = [], subtitleTracks = []) {
        if (!player) {
            return;
        }
        while (player.firstChild) {
            player.removeChild(player.firstChild);
        }
        this.appendSources(player, sources);
        this.appendTracks(player, subtitleTracks);
    }

    appendSources(player, sources) {
        if (!player || !Array.isArray(sources)) {
            return;
        }
        const sorted = this.sortSources(sources);
        sorted.forEach((source) => {
            const safeUrl = this.sanitizeMediaUrl(source?.url);
            if (!safeUrl) {
                return;
            }
            const element = document.createElement('source');
            element.src = safeUrl;
            const mime = this.getSourceMimeType(source);
            if (mime) {
                element.type = mime;
            }
            const label = this.getSourceLabel(source);
            if (label) {
                element.setAttribute('label', label);
            }
            player.appendChild(element);
        });
    }

    appendTracks(player, tracks) {
        if (!player || !Array.isArray(tracks)) {
            return;
        }
        tracks.forEach((track) => {
            const safeUrl = this.sanitizeMediaUrl(track?.url);
            if (!safeUrl) {
                return;
            }
            const element = document.createElement('track');
            element.kind = 'subtitles';
            element.src = safeUrl;
            const lang = this.normalizeLanguageCode(track?.code);
            if (lang) {
                element.srclang = lang;
            }
            const label =
                typeof track?.name === 'string' && track.name.trim()
                    ? track.name.trim()
                    : lang
                    ? lang.toUpperCase()
                    : 'Subtitles';
            element.label = label;
            player.appendChild(element);
        });
    }
}

// Regular Video Viewer
class VideoViewer extends BaseViewer {
    constructor(services, videoid, timestamp = 0) {
        super(services);
        this.videoid = videoid;
        this.timestamp = timestamp;
        this.player = null;
        this.userDataStore = typeof window !== 'undefined' ? window.userDataStore : null;
        this.likeButton = null;
        this.dislikeButton = null;
        this.saveButton = null;
        this.subscribeBtn = null;
    }

    async init() {
        await super.init();
        this.videoData = await this.services.getVideo(this.videoid);
        
        if (!this.videoData) {
            this.renderError('Video not found');
            return;
        }

        this.subtitles = await this.services.getSubtitles(this.videoid);
        this.comments = await this.services.getComments(this.videoid);
        this.render();
    }

    render() {
        const pageContainer = document.createElement('div');
        pageContainer.className = 'page-viewer';
        pageContainer.innerHTML = `
            <div class="viewer-layout">
                <div class="viewer-main">
                    <div class="video-player">
                        <video id="videoPlayer" controls></video>
                    </div>
                    <div class="video-info">
                        <h1 class="video-title">${this.escapeHtml(this.videoData.title)}</h1>
                        <div class="video-stats">
                            <span>${this.formatViews(this.videoData.views)} views</span>
                            <span>${this.formatDate(this.videoData.uploadDate)}</span>
                        </div>
                        <div class="video-actions">
                            <button class="action-btn like-btn" type="button">
                                <span class="action-icon">👍</span>
                                <span>${this.formatCount(this.videoData.likes)}</span>
                            </button>
                            <button class="action-btn dislike-btn" type="button">
                                <span class="action-icon">👎</span>
                                <span>${this.formatCount(this.videoData.dislikes)}</span>
                            </button>
                            <button class="action-btn save-btn" type="button">
                                <span class="action-icon">📁</span>
                                <span>Save</span>
                            </button>
                            <button class="action-btn share-btn" type="button">
                                <span class="action-icon">↗️</span>
                                <span>Share</span>
                            </button>
                        </div>
                        <div class="video-channel">
                            <div class="channel-avatar"></div>
                            <div class="channel-info">
                                <div class="channel-name">${this.escapeHtml(this.videoData.author)}</div>
                                <div class="channel-subs">${this.formatCount(this.videoData.subscriberCount)} subscribers</div>
                            </div>
                            <button class="subscribe-btn" type="button">Subscribe</button>
                        </div>
                        <div class="video-description">
                            <p>${this.escapeHtml(this.videoData.description)}</p>
                        </div>
                        <div class="video-data-note">Likes, dislikes, playlists, and subscriptions are stored securely in local storage. Export them from the You menu to sync another device.</div>
                    </div>
                    <div class="comments-section">
                        <h3>${this.comments.length} Comments</h3>
                        <div id="commentsList"></div>
                    </div>
                </div>
                <div class="viewer-sidebar">
                    <div class="related-videos">
                        <!-- Related videos would go here -->
                    </div>
                </div>
            </div>
        `;

        this.container.appendChild(pageContainer);
        this.populateVideoPlayerMedia();
        this.setupPlayer();
        this.setupActions();
        this.renderComments();
    }

    populateVideoPlayerMedia() {
        const player = document.getElementById('videoPlayer');
        if (!player) {
            return;
        }
        const tracks = Array.isArray(this.subtitles?.languages) ? this.subtitles.languages : [];
        this.prepareMediaElement(player, this.videoData?.sources || [], tracks);
    }

    getVideoMetadata() {
        const thumbnail = this.videoData?.thumbnailUrl || (Array.isArray(this.videoData?.thumbnails) ? this.videoData.thumbnails[0] : null);
        return {
            videoid: this.videoid,
            title: this.videoData?.title || null,
            author: this.videoData?.author || null,
            channelId: this.getChannelId(),
            thumbnail
        };
    }

    getChannelId() {
        if (this.videoData?.channelUrl) {
            return this.videoData.channelUrl;
        }
        if (this.videoData?.author) {
            return `channel:${this.videoData.author}`;
        }
        return `channel:${this.videoid}`;
    }

    setupActions() {
        const root = this.container.querySelector('.viewer-main');
        if (!root) {
            return;
        }
        this.likeButton = root.querySelector('.like-btn');
        this.dislikeButton = root.querySelector('.dislike-btn');
        this.saveButton = root.querySelector('.save-btn');
        this.subscribeBtn = root.querySelector('.subscribe-btn');

        if (this.likeButton) {
            this.likeButton.addEventListener('click', () => this.handleLike());
        }
        if (this.dislikeButton) {
            this.dislikeButton.addEventListener('click', () => this.handleDislike());
        }
        if (this.saveButton) {
            this.saveButton.addEventListener('click', () => this.handleSaveToPlaylist());
        }
        if (this.subscribeBtn) {
            this.subscribeBtn.addEventListener('click', () => this.handleSubscribe());
        }

        this.syncActionState();
    }

    syncActionState() {
        if (!this.userDataStore) {
            return;
        }
        const reaction = this.userDataStore.getReaction(this.videoid);
        if (this.likeButton) {
            this.likeButton.classList.toggle('active', reaction === 'like');
        }
        if (this.dislikeButton) {
            this.dislikeButton.classList.toggle('active', reaction === 'dislike');
        }
        if (this.subscribeBtn) {
            const isSubscribed = this.userDataStore.isSubscribed(this.getChannelId());
            this.subscribeBtn.classList.toggle('subscribed', isSubscribed);
            this.subscribeBtn.textContent = isSubscribed ? 'Subscribed' : 'Subscribe';
        }
    }

    handleLike() {
        if (!this.userDataStore) {
            return;
        }
        this.userDataStore.toggleLike(this.videoid, this.getVideoMetadata());
        this.syncActionState();
    }

    handleDislike() {
        if (!this.userDataStore) {
            return;
        }
        this.userDataStore.toggleDislike(this.videoid, this.getVideoMetadata());
        this.syncActionState();
    }

    handleSaveToPlaylist() {
        if (!this.userDataStore) {
            return;
        }
        const name = prompt('Save to which playlist? Leave blank for Favorites.', 'Favorites');
        if (name === null) {
            return;
        }
        const playlistName = name.trim() || 'Favorites';
        this.userDataStore.addToPlaylist(playlistName, this.videoid, this.getVideoMetadata());
        alert(`Added to "${playlistName}". This playlist lives locally until you export it from the You menu.`);
    }

    handleSubscribe() {
        if (!this.userDataStore) {
            return;
        }
        const subscribed = this.userDataStore.toggleSubscription(this.getChannelId(), {
            name: this.videoData?.author,
            channelUrl: this.videoData?.channelUrl
        });
        this.syncActionState();
        if (subscribed) {
            alert('Subscribed! This preference stays on this device unless you export/import your data.');
        } else {
            alert('Subscription removed from this device.');
        }
    }

    setupPlayer() {
        this.player = document.getElementById('videoPlayer');
        if (!this.player) {
            return;
        }

        if (this.timestamp > 0) {
            this.player.currentTime = this.timestamp;
        }

        this.player.addEventListener('loadedmetadata', () => this.restoreWatchProgress());
        this.player.addEventListener('timeupdate', () => this.recordWatchProgress());
        this.player.addEventListener('ended', () => this.handleVideoEnded());
    }

    recordWatchProgress() {
        if (!this.userDataStore || !this.player || !this.player.duration) {
            return;
        }
        const progress = this.player.currentTime / this.player.duration;
        this.userDataStore.setWatchProgress(this.videoid, progress, this.getVideoMetadata());
    }

    handleVideoEnded() {
        if (!this.userDataStore) {
            return;
        }
        this.userDataStore.markWatched(this.videoid, this.getVideoMetadata());
    }

    restoreWatchProgress() {
        if (!this.userDataStore || !this.player || !this.player.duration || this.timestamp > 0) {
            return;
        }
        const savedProgress = this.userDataStore.getWatchProgress(this.videoid);
        if (savedProgress > 0 && savedProgress < 0.98) {
            this.player.currentTime = savedProgress * this.player.duration;
        }
    }

    renderComments() {
        const commentsList = document.getElementById('commentsList');
        if (!commentsList || !this.comments) return;

        commentsList.innerHTML = this.comments.map(comment => `
            <div class="comment" data-id="${comment.id}">
                <div class="comment-avatar"></div>
                <div class="comment-content">
                    <div class="comment-author">
                        ${this.escapeHtml(comment.author)}
                        ${comment.status_likedbycreator ? '<span class="creator-heart">❤️</span>' : ''}
                    </div>
                    <div class="comment-time">${this.formatCommentTime(comment.timePosted)}</div>
                    <div class="comment-text">${this.escapeHtml(comment.text)}</div>
                    <div class="comment-actions">
                        <button class="comment-like">👍 ${this.formatCount(comment.likes)}</button>
                        <button class="comment-reply">Reply</button>
                    </div>
                    ${comment.replyCount > 0 ? `<button class="show-replies">${comment.replyCount} replies</button>` : ''}
                </div>
            </div>
        `).join('');
    }

    renderError(message) {
        this.container.innerHTML = `
            <div class="page-viewer">
                <div class="error-page">
                    <h1>Error</h1>
                    <p>${this.escapeHtml(message)}</p>
                </div>
            </div>
        `;
    }

    formatViews(views) {
        if (views >= 1000000) return (views / 1000000).toFixed(1) + 'M';
        if (views >= 1000) return (views / 1000).toFixed(1) + 'K';
        return views.toString();
    }

    formatCount(count) {
        if (!count) return '0';
        if (count >= 1000000) return (count / 1000000).toFixed(1) + 'M';
        if (count >= 1000) return (count / 1000).toFixed(1) + 'K';
        return count.toString();
    }

    formatDate(dateString) {
        const date = new Date(dateString);
        const now = new Date();
        const diff = now - date;
        const days = Math.floor(diff / (1000 * 60 * 60 * 24));
        
        if (days === 0) return 'Today';
        if (days === 1) return 'Yesterday';
        if (days < 7) return `${days} days ago`;
        if (days < 30) return `${Math.floor(days / 7)} weeks ago`;
        if (days < 365) return `${Math.floor(days / 30)} months ago`;
        return `${Math.floor(days / 365)} years ago`;
    }

    formatCommentTime(timestamp) {
        return this.formatDate(timestamp);
    }

    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Shorts Viewer
class ShortsViewer extends BaseViewer {
    constructor(services, videoid) {
        super(services);
        this.videoid = videoid;
        this.player = null;
        this.userDataStore = typeof window !== 'undefined' ? window.userDataStore : null;
        this.likeButton = null;
        this.dislikeButton = null;
        this.saveButton = null;
    }

    async init() {
        await super.init();
        this.videoData = await this.services.getShort(this.videoid);
        
        if (!this.videoData) {
            this.renderError('Short not found');
            return;
        }

        this.comments = await this.services.getComments(this.videoid);
        this.render();
    }

    render() {
        const pageContainer = document.createElement('div');
        pageContainer.className = 'page-viewer';
        pageContainer.innerHTML = `
            <div class="shorts-viewer">
                <div class="shorts-video-container">
                    <video id="shortsPlayer" loop></video>
                    <div class="shorts-overlay">
                        <div class="shorts-info">
                            <div class="shorts-author">@${this.escapeHtml(this.videoData.author)}</div>
                            <div class="shorts-title">${this.escapeHtml(this.videoData.title)}</div>
                        </div>
                        <div class="shorts-actions">
                            <button class="shorts-action-btn like-btn" type="button">
                                <span class="action-icon">👍</span>
                                <span>${this.formatCount(this.videoData.likes)}</span>
                            </button>
                            <button class="shorts-action-btn dislike-btn" type="button">
                                <span class="action-icon">👎</span>
                                <span>Dislike</span>
                            </button>
                            <button class="shorts-action-btn save-btn" type="button">
                                <span class="action-icon">📁</span>
                                <span>Save</span>
                            </button>
                            <button class="shorts-action-btn" type="button">
                                <span class="action-icon">💬</span>
                                <span>${this.comments.length}</span>
                            </button>
                            <button class="shorts-action-btn" type="button">
                                <span class="action-icon">↗️</span>
                                <span>Share</span>
                            </button>
                        </div>
                    </div>
                    <div class="shorts-navigation">
                        <button class="nav-btn prev">↑</button>
                        <button class="nav-btn next">↓</button>
                    </div>
                </div>
            </div>
        `;

        this.container.appendChild(pageContainer);
        this.populateShortsPlayerMedia();
        this.setupPlayer();
        this.setupShortActions();
    }

    populateShortsPlayerMedia() {
        const player = document.getElementById('shortsPlayer');
        if (!player) {
            return;
        }
        this.prepareMediaElement(player, this.videoData?.sources || [], []);
    }

    setupPlayer() {
        this.player = document.getElementById('shortsPlayer');
        if (this.player) {
            this.player.play().catch(err => console.log('Autoplay prevented:', err));
            this.player.addEventListener('timeupdate', () => this.recordShortProgress());
            this.player.addEventListener('ended', () => this.markShortWatched());
        }
    }

    getShortMetadata() {
        return {
            videoid: this.videoid,
            title: this.videoData?.title || null,
            author: this.videoData?.author || null,
            channelId: this.videoData?.author ? `shorts:${this.videoData.author}` : `shorts:${this.videoid}`,
            thumbnail: this.videoData?.thumbnailUrl || null
        };
    }

    setupShortActions() {
        const actions = this.container.querySelector('.shorts-actions');
        if (!actions) {
            return;
        }
        this.likeButton = actions.querySelector('.like-btn');
        this.dislikeButton = actions.querySelector('.dislike-btn');
        this.saveButton = actions.querySelector('.save-btn');

        if (this.likeButton) {
            this.likeButton.addEventListener('click', () => this.handleShortLike());
        }
        if (this.dislikeButton) {
            this.dislikeButton.addEventListener('click', () => this.handleShortDislike());
        }
        if (this.saveButton) {
            this.saveButton.addEventListener('click', () => this.handleShortSave());
        }

        this.syncShortActions();
    }

    syncShortActions() {
        if (!this.userDataStore) {
            return;
        }
        const reaction = this.userDataStore.getReaction(this.videoid);
        if (this.likeButton) {
            this.likeButton.classList.toggle('active', reaction === 'like');
        }
        if (this.dislikeButton) {
            this.dislikeButton.classList.toggle('active', reaction === 'dislike');
        }
    }

    handleShortLike() {
        if (!this.userDataStore) {
            return;
        }
        this.userDataStore.toggleLike(this.videoid, this.getShortMetadata());
        this.syncShortActions();
    }

    handleShortDislike() {
        if (!this.userDataStore) {
            return;
        }
        this.userDataStore.toggleDislike(this.videoid, this.getShortMetadata());
        this.syncShortActions();
    }

    handleShortSave() {
        if (!this.userDataStore) {
            return;
        }
        const name = prompt('Save this Short to which playlist?', 'Favorites');
        if (name === null) {
            return;
        }
        const playlistName = name.trim() || 'Favorites';
        this.userDataStore.addToPlaylist(playlistName, this.videoid, this.getShortMetadata());
        alert(`Short saved to "${playlistName}" on this device.`);
    }

    recordShortProgress() {
        if (!this.userDataStore || !this.player || !this.player.duration) {
            return;
        }
        const progress = this.player.currentTime / this.player.duration;
        this.userDataStore.setWatchProgress(this.videoid, progress, this.getShortMetadata());
    }

    markShortWatched() {
        if (this.userDataStore) {
            this.userDataStore.markWatched(this.videoid, this.getShortMetadata());
        }
    }

    renderError(message) {
        this.container.innerHTML = `
            <div class="page-viewer">
                <div class="error-page">
                    <h1>Error</h1>
                    <p>${this.escapeHtml(message)}</p>
                </div>
            </div>
        `;
    }

    formatCount(count) {
        if (!count) return '0';
        if (count >= 1000000) return (count / 1000000).toFixed(1) + 'M';
        if (count >= 1000) return (count / 1000).toFixed(1) + 'K';
        return count.toString();
    }

    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Viewer Page Class
class ViewerPage {
    constructor(services = {}) {
        this.services = Object.assign({
            ready: () => Promise.resolve(),
            getVideo: () => Promise.resolve(null),
            getShort: () => Promise.resolve(null),
            getSubtitles: () => Promise.resolve(null),
            getComments: () => Promise.resolve([]),
            getCommentReplies: () => Promise.resolve([])
        }, services);
        this.currentViewer = null;
        this.type = null; // 'video' or 'short'
    }

    async init() {
        if (typeof this.services.ready === 'function') {
            await this.services.ready();
        }
        
        // Parse URL to determine viewer type
        const path = window.location.pathname;
        const params = new URLSearchParams(window.location.search);

        if (path.startsWith('/watch')) {
            // Regular video viewer
            const videoid = params.get('v');
            const timestamp = parseInt(params.get('t')) || 0;
            
            if (!videoid) {
                this.renderError('No video ID provided');
                return;
            }

            this.type = 'video';
            this.currentViewer = new VideoViewer(this.services, videoid, timestamp);
            await this.currentViewer.init();

        } else if (path.startsWith('/shorts/')) {
            // Shorts viewer
            const videoid = path.split('/shorts/')[1];
            
            if (!videoid) {
                this.renderError('No short ID provided');
                return;
            }

            this.type = 'short';
            this.currentViewer = new ShortsViewer(this.services, videoid);
            await this.currentViewer.init();

        } else {
            this.renderError('Invalid URL');
        }
    }

    renderError(message) {
        const app = document.getElementById('app');
        app.innerHTML = `
            <div class="page-viewer">
                <div class="error-page">
                    <h1>Error</h1>
                    <p>${message}</p>
                    <a href="/">Go back to home</a>
                </div>
            </div>
        `;
    }

    close() {
        if (this.currentViewer) {
            this.currentViewer.close();
        }
    }

    refresh() {
        if (this.currentViewer && this.currentViewer.render) {
            this.currentViewer.render();
        }
    }
}

if (typeof module !== 'undefined' && module.exports) {
    module.exports = { VideoViewer, ShortsViewer, ViewerPage };
}
