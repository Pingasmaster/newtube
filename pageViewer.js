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
}

// Regular Video Viewer
class VideoViewer extends BaseViewer {
    constructor(services, videoid, timestamp = 0) {
        super(services);
        this.videoid = videoid;
        this.timestamp = timestamp;
        this.player = null;
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
                        <video id="videoPlayer" controls>
                            ${this.renderSources()}
                            ${this.renderSubtitles()}
                        </video>
                    </div>
                    <div class="video-info">
                        <h1 class="video-title">${this.escapeHtml(this.videoData.title)}</h1>
                        <div class="video-stats">
                            <span>${this.formatViews(this.videoData.views)} views</span>
                            <span>${this.formatDate(this.videoData.uploadDate)}</span>
                        </div>
                        <div class="video-actions">
                            <button class="action-btn">
                                <span class="action-icon">👍</span>
                                <span>${this.formatCount(this.videoData.likes)}</span>
                            </button>
                            <button class="action-btn">
                                <span class="action-icon">👎</span>
                                <span>${this.formatCount(this.videoData.dislikes)}</span>
                            </button>
                            <button class="action-btn">
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
                            <button class="subscribe-btn">Subscribe</button>
                        </div>
                        <div class="video-description">
                            <p>${this.escapeHtml(this.videoData.description)}</p>
                        </div>
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
        this.setupPlayer();
        this.renderComments();
    }

    renderSources() {
        if (!this.videoData.sources || this.videoData.sources.length === 0) {
            return '';
        }

        // Sort sources by quality (highest first)
        const sortedSources = [...this.videoData.sources].sort((a, b) => {
            const qualityA = parseInt(a.quality) || 0;
            const qualityB = parseInt(b.quality) || 0;
            return qualityB - qualityA;
        });

        return sortedSources.map(source => 
            `<source src="${source.url}" type="video/mp4" label="${source.quality}">`
        ).join('');
    }

    renderSubtitles() {
        if (!this.subtitles || !this.subtitles.languages) {
            return '';
        }

        return this.subtitles.languages.map(sub => 
            `<track kind="subtitles" src="${sub.url}" srclang="${sub.code}" label="${sub.name}">`
        ).join('');
    }

    setupPlayer() {
        this.player = document.getElementById('videoPlayer');
        if (this.player && this.timestamp > 0) {
            this.player.currentTime = this.timestamp;
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
                    <video id="shortsPlayer" loop>
                        ${this.renderSources()}
                    </video>
                    <div class="shorts-overlay">
                        <div class="shorts-info">
                            <div class="shorts-author">@${this.escapeHtml(this.videoData.author)}</div>
                            <div class="shorts-title">${this.escapeHtml(this.videoData.title)}</div>
                        </div>
                        <div class="shorts-actions">
                            <button class="shorts-action-btn">
                                <span class="action-icon">👍</span>
                                <span>${this.formatCount(this.videoData.likes)}</span>
                            </button>
                            <button class="shorts-action-btn">
                                <span class="action-icon">👎</span>
                                <span>Dislike</span>
                            </button>
                            <button class="shorts-action-btn">
                                <span class="action-icon">💬</span>
                                <span>${this.comments.length}</span>
                            </button>
                            <button class="shorts-action-btn">
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
        this.setupPlayer();
    }

    renderSources() {
        if (!this.videoData.sources || this.videoData.sources.length === 0) {
            return '';
        }

        return this.videoData.sources.map(source => 
            `<source src="${source.url}" type="video/mp4">`
        ).join('');
    }

    setupPlayer() {
        this.player = document.getElementById('shortsPlayer');
        if (this.player) {
            this.player.play().catch(err => console.log('Autoplay prevented:', err));
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
