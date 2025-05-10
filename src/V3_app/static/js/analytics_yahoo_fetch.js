class YahooFetch {
    constructor() {
        this.sourceSelect = document.getElementById('yahoo-source-select');
        this.previewArea = document.getElementById('yahoo-preview');
        this.tickerCount = document.getElementById('yahoo-ticker-count');
        this.tickerList = document.getElementById('yahoo-ticker-list');
        this.fetchButton = document.getElementById('run-yahoo-fetch-btn');
        this.fetchStatus = document.getElementById('yahoo-fetch-status');
        this.fileInput = document.getElementById('yahoo-ticker-file-input');
        this.dropZone = document.getElementById('yahoo-drop-zone');
        this.fileUploadArea = document.getElementById('yahoo-file-upload-area');
        this.currentTickers = [];
        this.jobId = null;
        this.pollInterval = null;

        this.initializeEventListeners();
        this.handleSourceChange(); // Initial state
    }

    initializeEventListeners() {
        this.sourceSelect.addEventListener('change', () => this.handleSourceChange());
        this.fetchButton.addEventListener('click', () => this.handleFetchYahooData());
        this.dropZone.addEventListener('click', () => this.fileInput.click());
        this.fileInput.addEventListener('change', (e) => this.handleFileSelect(e));
        this.dropZone.addEventListener('dragover', (e) => { e.preventDefault(); this.dropZone.classList.add('dragover'); });
        this.dropZone.addEventListener('dragleave', () => this.dropZone.classList.remove('dragover'));
        this.dropZone.addEventListener('drop', (e) => {
            e.preventDefault();
            this.dropZone.classList.remove('dragover');
            if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
                this.fileInput.files = e.dataTransfer.files;
                this.handleFileSelect({ target: { files: e.dataTransfer.files } });
                e.dataTransfer.clearData();
            }
        });
    }

    async handleSourceChange() {
        const source = this.sourceSelect.value;
        this.currentTickers = [];
        this.previewArea.style.display = 'none';
        this.tickerList.innerHTML = '';
        this.tickerCount.textContent = '';
        this.fetchButton.disabled = true;
        this.fetchStatus.textContent = '';
        if (source === 'file') {
            this.fileUploadArea.style.display = '';
        } else {
            this.fileUploadArea.style.display = 'none';
        }
        if (source === 'portfolio' || source === 'screener') {
            // Fetch from backend
            try {
                const resp = await fetch(`/api/yahoo/fetch/sources?source=${source}`);
                const data = await resp.json();
                this.currentTickers = Array.isArray(data.tickers) ? data.tickers : [];
                this.showTickerPreview();
            } catch (e) {
                this.showError('Failed to load tickers from backend.');
            }
        } else if (source === 'pretransform') {
            // Get from frontend pre-transform state
            if (window.AnalyticsMainModule && typeof window.AnalyticsMainModule.getPreTransformTickers === 'function') {
                this.currentTickers = window.AnalyticsMainModule.getPreTransformTickers();
            } else if (window.AnalyticsMainModule && typeof window.AnalyticsMainModule.getCurrentTickers === 'function') {
                this.currentTickers = window.AnalyticsMainModule.getCurrentTickers();
            } else {
                this.currentTickers = [];
            }
            this.showTickerPreview();
        } else if (source === 'posttransform') {
            // Get from frontend post-transform state
            if (window.AnalyticsPostTransformModule && typeof window.AnalyticsPostTransformModule.getPostTransformTickers === 'function') {
                this.currentTickers = window.AnalyticsPostTransformModule.getPostTransformTickers();
            } else {
                this.currentTickers = [];
            }
            this.showTickerPreview();
        } else if (source === 'file') {
            // Wait for file upload
            this.showTickerPreview();
        }
    }

    async handleFileSelect(event) {
        const files = event.target.files;
        if (!files || files.length === 0) {
            this.currentTickers = [];
            this.showTickerPreview();
            return;
        }
        const file = files[0];
        if (!file.name.toLowerCase().endsWith('.txt')) {
            this.showError('Please upload a .txt file.');
            this.currentTickers = [];
            this.showTickerPreview();
            return;
        }
        const reader = new FileReader();
        reader.onload = (e) => {
            const text = e.target.result;
            // Split by line or comma, trim, filter empty
            let tickers = text.split(/\r?\n|,|;/).map(t => t.trim().toUpperCase()).filter(Boolean);
            // Remove duplicates
            tickers = Array.from(new Set(tickers));
            this.currentTickers = tickers;
            this.showTickerPreview();
        };
        reader.onerror = () => {
            this.showError('Error reading file.');
            this.currentTickers = [];
            this.showTickerPreview();
        };
        reader.readAsText(file);
    }

    showTickerPreview() {
        this.tickerList.innerHTML = '';
        if (this.currentTickers.length > 0) {
            this.previewArea.style.display = '';
            this.tickerCount.textContent = this.currentTickers.length;
            this.currentTickers.slice(0, 100).forEach(ticker => {
                const li = document.createElement('li');
                li.className = 'list-group-item py-1';
                li.textContent = ticker;
                this.tickerList.appendChild(li);
            });
            this.fetchButton.disabled = false;
        } else {
            this.previewArea.style.display = 'none';
            this.tickerCount.textContent = '';
            this.fetchButton.disabled = true;
        }
    }

    showError(message) {
        this.fetchStatus.textContent = message;
        this.fetchStatus.className = 'mt-2 small text-danger';
    }

    async handleFetchYahooData() {
        if (!this.currentTickers || this.currentTickers.length === 0) {
            this.showError('No tickers to fetch.');
            return;
        }
        this.setFetchingState(true);
        try {
            const resp = await fetch('/api/yahoo/fetch', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ tickers: this.currentTickers })
            });
            const data = await resp.json();
            if (data && data.status === 'started') {
                this.showFetchSuccess('Fetch started.');
            } else {
                this.showFetchError(data && data.message ? data.message : 'Fetch failed.');
            }
        } catch (e) {
            this.showFetchError('Fetch request failed.');
        } finally {
            this.setFetchingState(false);
        }
    }

    setFetchingState(isFetching) {
        this.fetchButton.disabled = isFetching;
        if (isFetching) {
            this.fetchStatus.textContent = 'Fetching...';
            this.fetchStatus.className = 'mt-2 small text-info';
        }
    }

    showFetchError(message) {
        this.fetchStatus.textContent = message;
        this.fetchStatus.className = 'mt-2 small text-danger';
    }

    showFetchSuccess(message) {
        this.fetchStatus.textContent = message;
        this.fetchStatus.className = 'mt-2 small text-success';
    }
}

document.addEventListener('DOMContentLoaded', () => {
    window.YahooFetchInstance = new YahooFetch();
}); 