class AdminPage {
    constructor(services = {}) {
        this.services = Object.assign(
            {
                ready: () => Promise.resolve(),
                getSettings: () => Promise.resolve(null),
                updateSettings: () => Promise.resolve(null)
            },
            services || {}
        );
        this.container = null;
        this.statusEl = null;
        this.saveBtn = null;
        this.form = null;
    }

    async init() {
        this.container = document.getElementById('app');
        this.container.innerHTML = '';
        const page = this.render();
        this.container.appendChild(page);

        try {
            await this.services.ready();
        } catch {
            // Ignore readiness issues for admin.
        }

        await this.loadSettings();
    }

    render() {
        const wrapper = document.createElement('div');
        wrapper.className = 'page-admin';
        wrapper.innerHTML = `
            <header class="admin-header">
                <div class="admin-title">
                    <span class="admin-badge">Admin</span>
                    <h1>Instance Settings</h1>
                </div>
                <a class="admin-home" href="/">Back to Home</a>
            </header>
            <main class="admin-main">
                <section class="admin-card">
                    <h2>Playback behavior</h2>
                    <div class="admin-field">
                        <label>Missing media</label>
                        <div class="admin-options" role="radiogroup" aria-label="Missing media behavior">
                            <label class="admin-option">
                                <input type="radio" name="missingMediaBehavior" value="not_found" />
                                <span>Show a 404 like today</span>
                            </label>
                            <label class="admin-option">
                                <input type="radio" name="missingMediaBehavior" value="prompt" />
                                <span>Prompt to download missing videos</span>
                            </label>
                        </div>
                        <p class="admin-help">
                            You can also set this before startup with <code>NEWTUBE_MISSING_MEDIA_BEHAVIOR</code>.
                        </p>
                    </div>
                    <div class="admin-actions">
                        <button class="admin-save" type="button">Save changes</button>
                        <span class="admin-status"></span>
                    </div>
                </section>
                <section class="admin-card admin-note">
                    <h2>Security</h2>
                    <p>
                        This admin page does not require authentication. Restrict access to <code>/admin</code>
                        with your reverse proxy if the instance is public.
                    </p>
                </section>
            </main>
        `;

        this.statusEl = wrapper.querySelector('.admin-status');
        this.saveBtn = wrapper.querySelector('.admin-save');
        this.form = wrapper.querySelector('.admin-options');

        if (this.saveBtn) {
            this.saveBtn.addEventListener('click', () => this.handleSave());
        }

        return wrapper;
    }

    async loadSettings() {
        if (!this.services || typeof this.services.getSettings !== 'function') {
            this.setStatus('Settings API unavailable.', true);
            return;
        }

        try {
            const settings = await this.services.getSettings();
            const behavior = settings?.missingMediaBehavior || 'not_found';
            const input = this.container.querySelector(
                `input[name="missingMediaBehavior"][value="${behavior}"]`
            );
            if (input) {
                input.checked = true;
            }
            this.setStatus('Loaded current settings.');
        } catch (error) {
            this.setStatus(`Failed to load settings: ${error.message}`, true);
        }
    }

    async handleSave() {
        if (!this.services || typeof this.services.updateSettings !== 'function') {
            this.setStatus('Settings API unavailable.', true);
            return;
        }

        const selection = this.container.querySelector('input[name="missingMediaBehavior"]:checked');
        const missingMediaBehavior = selection ? selection.value : 'not_found';

        try {
            this.saveBtn.disabled = true;
            this.setStatus('Saving settings...');
            const updated = await this.services.updateSettings({
                missingMediaBehavior
            });
            const effective = updated?.missingMediaBehavior || missingMediaBehavior;
            this.setStatus(`Saved. Missing media behavior is now ${effective.replace('_', ' ')}.`);
        } catch (error) {
            this.setStatus(`Failed to save settings: ${error.message}`, true);
        } finally {
            this.saveBtn.disabled = false;
        }
    }

    setStatus(message, isError = false) {
        if (!this.statusEl) {
            return;
        }
        this.statusEl.textContent = message;
        this.statusEl.classList.toggle('error', isError);
    }

    close() {
        if (this.container) {
            this.container.innerHTML = '';
        }
    }
}

if (typeof window !== 'undefined') {
    window.AdminPage = AdminPage;
}
