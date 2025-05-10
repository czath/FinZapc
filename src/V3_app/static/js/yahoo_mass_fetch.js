// Yahoo Mass Fetch UI and Ticker Source Logic
(function() {
    // --- Helper: Create DOM elements ---
    function createEl(tag, attrs = {}, ...children) {
        const el = document.createElement(tag);
        for (const [k, v] of Object.entries(attrs)) {
            if (k === 'class') el.className = v;
            else if (k === 'style') el.style.cssText = v;
            else el.setAttribute(k, v);
        }
        for (const child of children) {
            if (typeof child === 'string') el.appendChild(document.createTextNode(child));
            else if (child) el.appendChild(child);
        }
        return el;
    }

    // --- UI State Management ---
    let currentTickers = [];
    let isFetching = false;

    // --- UI Elements ---
    const sourceSelect = document.getElementById('yahoo-mf-source');
    const dropZone = document.getElementById('yahoo-mf-dropzone');
    const fileInput = document.getElementById('yahoo-mf-file');
    const fetchBtn = document.getElementById('yahoo-mf-fetch');
    const statusDiv = document.getElementById('yahoo-mf-status');

    // Remove previewBtn and previewArea logic
    // Add a new div for ticker summary
    const tickerSummaryDiv = document.createElement('div');
    tickerSummaryDiv.id = 'yahoo-mf-ticker-summary';
    tickerSummaryDiv.className = 'mt-2 small text-muted';
    fetchBtn.parentNode.insertBefore(tickerSummaryDiv, fetchBtn.nextSibling);

    function showTickerSummary(tickers) {
        if (!tickers || !tickers.length) {
            tickerSummaryDiv.textContent = 'No tickers found.';
            fetchBtn.disabled = true;
            return;
        }
        const exampleCount = Math.min(5, tickers.length);
        const exampleTickers = tickers.slice(0, exampleCount).join(', ');
        const source = sourceSelect.value;
        let message = `Tickers found: ${tickers.length}. Example: ${exampleTickers}${tickers.length > exampleCount ? ', ...' : ''}`;
        
        // Add source-specific context
        if (source === 'pretrans') {
            message += ' (from loaded data)';
        } else if (source === 'posttrans') {
            const mainModule = window.AnalyticsMainModule;
            if (!mainModule) {
                message += ' (analytics module not available)';
            } else {
                const preData = mainModule.getFullProcessedData() || [];
                const postData = mainModule.getFinalDataForAnalysis() || [];
                const hasTransformations = preData.length !== postData.length || 
                    JSON.stringify(preData.map(x => x.ticker).sort()) !== JSON.stringify(postData.map(x => x.ticker).sort());
                message += hasTransformations ? ' (after transformations)' : ' (no transformations applied yet)';
            }
        }
        
        tickerSummaryDiv.textContent = message;
        fetchBtn.disabled = false;
    }

    // --- Helper Functions ---
    function setDropZoneEnabled(enabled) {
        dropZone.style.pointerEvents = enabled ? 'auto' : 'none';
        dropZone.style.opacity = enabled ? '1' : '0.5';
    }

    function showSpinner(button) {
        const spinner = button.querySelector('.spinner-border');
        const text = button.querySelector('.button-text');
        if (spinner) spinner.style.display = 'inline-block';
        if (text) text.style.display = 'none';
        button.disabled = true;
    }

    function hideSpinner(button) {
        const spinner = button.querySelector('.spinner-border');
        const text = button.querySelector('.button-text');
        if (spinner) spinner.style.display = 'none';
        if (text) text.style.display = 'inline';
        button.disabled = false;
    }

    function showStatus(message, isError = false) {
        statusDiv.textContent = message;
        statusDiv.className = `mt-2 small ${isError ? 'text-danger' : 'text-muted'}`;
    }

    // --- Event Handlers ---
    async function handleSourceChange() {
        const source = sourceSelect.value;
        currentTickers = [];
        tickerSummaryDiv.textContent = '';
        setDropZoneEnabled(source === 'upload');
        dropZone.style.display = source === 'upload' ? 'block' : 'none';
        fetchBtn.disabled = true;
        if (source !== 'upload') {
            // Immediately load tickers for non-upload sources
            try {
                let tickers = [];
                if (source === 'screener') {
                    const resp = await fetch('/api/screener/tickers');
                    if (!resp.ok) throw new Error('Failed to fetch screener tickers');
                    tickers = await resp.json();
                } else if (source === 'portfolio') {
                    const resp = await fetch('/api/portfolio/tickers');
                    if (!resp.ok) throw new Error('Failed to fetch portfolio tickers');
                    tickers = await resp.json();
                } else if (source === 'yahoo_master') {
                    const resp = await fetch('/api/yahoo/master_tickers');
                    if (!resp.ok) throw new Error('Failed to fetch Yahoo master tickers');
                    tickers = await resp.json();
                } else if (source === 'pretrans') {
                    const mainModule = window.AnalyticsMainModule;
                    if (!mainModule) throw new Error('Analytics module not available');
                    const data = mainModule.getFullProcessedData();
                    if (!data || !data.length) throw new Error('No pre-transformation data available');
                    tickers = data.map(x => x.ticker).filter(Boolean);
                } else if (source === 'posttrans') {
                    const mainModule = window.AnalyticsMainModule;
                    if (!mainModule) throw new Error('Analytics module not available');
                    const data = mainModule.getFinalDataForAnalysis();
                    if (!data || !data.length) throw new Error('No post-transformation data available');
                    tickers = data.map(x => x.ticker).filter(Boolean);
                }
                tickers = Array.from(new Set(tickers)).filter(Boolean);
                currentTickers = tickers;
                showTickerSummary(tickers);
            } catch (error) {
                tickerSummaryDiv.textContent = error.message;
                fetchBtn.disabled = true;
            }
        }
    }

    async function handleFetch() {
        if (isFetching || !currentTickers.length) return;
        isFetching = true;
        showSpinner(fetchBtn);
        showStatus('Starting fetch...');

        try {
            // 1. Start the mass fetch job
            const resp = await fetch('/api/yahoo/mass_fetch', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ tickers: currentTickers })
            });

            if (!resp.ok) {
                const error = await resp.json();
                throw new Error(error.message || 'Fetch failed');
            }

            const data = await resp.json();
            const jobId = data.job_id;
            if (!jobId) throw new Error('No job ID returned from backend.');

            // 2. Poll for progress
            let lastCurrent = 0;
            let lastTotal = currentTickers.length;
            let pollInterval = setInterval(async () => {
                try {
                    const statusResp = await fetch(`/api/yahoo/mass_fetch/status/${jobId}`);
                    if (!statusResp.ok) throw new Error('Failed to get job status');
                    const statusData = await statusResp.json();
                    const { current, total, last_ticker, status, errors } = statusData;
                    lastCurrent = current;
                    lastTotal = total;
                    let processed = typeof statusData.success_count === 'number' ? statusData.success_count : (current || 0);
                    let errorCount = typeof statusData.error_count === 'number' ? statusData.error_count : ((errors && errors.length) ? errors.length : 0);
                    if (status === 'running') {
                        showStatus(`Processing ticker ${processed + errorCount}/${total}${last_ticker ? ': ' + last_ticker : ''}`);
                    } else if (status === 'completed' || status === 'partial_failure' || status === 'failed') {
                        let msg = `Successfully processed tickers: ${processed} / Tickers with errors: ${errorCount}`;
                        showStatus(msg, errorCount > 0);
                        clearInterval(pollInterval);
                        isFetching = false;
                        hideSpinner(fetchBtn);
                        fetchBtn.disabled = true;
                    } else {
                        showStatus(`Job status: ${status}`);
                        clearInterval(pollInterval);
                        isFetching = false;
                        hideSpinner(fetchBtn);
                        fetchBtn.disabled = true;
                    }
                } catch (err) {
                    showStatus('Error polling job status: ' + err.message, true);
                    clearInterval(pollInterval);
                    isFetching = false;
                    hideSpinner(fetchBtn);
                    fetchBtn.disabled = true;
                }
            }, 1000);
        } catch (error) {
            showStatus(error.message, true);
            isFetching = false;
            hideSpinner(fetchBtn);
        }
    }

    async function handleFileSelect(event) {
        const file = event.target.files[0];
        if (!file) return;
        if (!file.name.toLowerCase().endsWith('.txt')) {
            showStatus('Please upload a .txt file.', true);
            tickerSummaryDiv.textContent = '';
            fetchBtn.disabled = true;
            return;
        }
        dropZone.querySelector('.status-text').textContent = file.name;
        // Parse file and show summary
        try {
            const text = await file.text();
            const tickers = text.split(/\r?\n|,|;/).map(t => t.trim().toUpperCase()).filter(Boolean);
            currentTickers = Array.from(new Set(tickers)).filter(Boolean);
            showTickerSummary(currentTickers);
        } catch (e) {
            tickerSummaryDiv.textContent = 'Error reading file.';
            fetchBtn.disabled = true;
        }
    }

    // Update event listeners
    sourceSelect.removeEventListener('change', handleSourceChange);
    sourceSelect.addEventListener('change', handleSourceChange);
    dropZone.removeEventListener('click', () => fileInput.click());
    dropZone.addEventListener('click', () => fileInput.click());
    fileInput.removeEventListener('change', handleFileSelect);
    fileInput.addEventListener('change', handleFileSelect);

    // On load, trigger source change to show summary if not upload
    if (sourceSelect.value !== 'upload') {
        handleSourceChange();
    }

    // --- Initialize Event Listeners ---
    function initializeEventListeners() {
        sourceSelect.addEventListener('change', handleSourceChange);
        fetchBtn.addEventListener('click', handleFetch);

        // Drag and drop handlers
        dropZone.addEventListener('dragover', e => {
            e.preventDefault();
            dropZone.style.backgroundColor = 'var(--bs-primary-bg-subtle)';
        });

        dropZone.addEventListener('dragleave', e => {
            e.preventDefault();
            dropZone.style.backgroundColor = 'var(--bs-tertiary-bg)';
        });

        dropZone.addEventListener('drop', e => {
            e.preventDefault();
            dropZone.style.backgroundColor = 'var(--bs-tertiary-bg)';
            if (e.dataTransfer.files.length) {
                fileInput.files = e.dataTransfer.files;
                handleFileSelect({ target: { files: e.dataTransfer.files } });
            }
        });
    }

    // --- Initialize on DOMContentLoaded ---
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initializeEventListeners);
    } else {
        initializeEventListeners();
    }
})(); 