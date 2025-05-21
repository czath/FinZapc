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
        const source = document.getElementById('yahoo-mf-source').value;
        const dropzone = document.getElementById('yahoo-mf-dropzone');
        const fetchBtn = document.getElementById('yahoo-mf-fetch');
        const userDefinedContainer = document.getElementById('user-defined-tickers-container');
        const userDefinedInput = document.getElementById('user-defined-tickers');

        // Reset UI state
        setDropZoneEnabled(false);
        fetchBtn.disabled = true;
        showStatus('');
        userDefinedContainer.style.display = 'none';
        userDefinedInput.value = '';

        switch (source) {
            case 'upload':
                setDropZoneEnabled(true);
                break;
            case 'user_defined':
                userDefinedContainer.style.display = 'block';
                // Enable fetch button when user types something
                userDefinedInput.addEventListener('input', () => {
                    const tickers = userDefinedInput.value.trim();
                    fetchBtn.disabled = !tickers;
                });
                break;
            case 'screener':
            case 'portfolio':
            case 'pretrans':
            case 'posttrans':
            case 'yahoo_master':
                // These sources don't need the dropzone
                fetchBtn.disabled = false;
                break;
        }
    }

    async function handleFetch() {
        const source = document.getElementById('yahoo-mf-source').value;
        const fetchBtn = document.getElementById('yahoo-mf-fetch');
        const userDefinedInput = document.getElementById('user-defined-tickers');
        let tickers = [];

        try {
            showSpinner(fetchBtn);
            showStatus('');

            switch (source) {
                case 'upload':
                    const fileInput = document.getElementById('yahoo-mf-file');
                    if (!fileInput.files.length) {
                        throw new Error('Please select a file first');
                    }
                    const file = fileInput.files[0];
                    const text = await file.text();
                    tickers = text.split('\n')
                        .map(line => line.trim())
                        .filter(line => line && !line.startsWith('#'));
                    break;

                case 'user_defined':
                    const inputText = userDefinedInput.value.trim();
                    if (!inputText) {
                        throw new Error('Please enter at least one ticker symbol');
                    }
                    // Split by comma, trim each ticker, and filter out empty strings
                    tickers = inputText.split(',')
                        .map(ticker => ticker.trim().toUpperCase())
                        .filter(ticker => ticker);
                    break;

                case 'screener':
                    const screenerResp = await fetch('/api/screener/tickers');
                    if (!screenerResp.ok) throw new Error('Failed to fetch screener tickers');
                    tickers = await screenerResp.json();
                    break;

                case 'portfolio':
                    const portfolioResp = await fetch('/api/portfolio/tickers');
                    if (!portfolioResp.ok) throw new Error('Failed to fetch portfolio tickers');
                    tickers = await portfolioResp.json();
                    break;

                case 'pretrans':
                    const pretransModule = window.AnalyticsMainModule;
                    if (!pretransModule) throw new Error('Analytics module not available');
                    const pretransData = pretransModule.getFullProcessedData();
                    if (!pretransData || !pretransData.length) throw new Error('No pre-transformation data available');
                    tickers = pretransData.map(x => x.ticker).filter(Boolean);
                    break;

                case 'posttrans':
                    const posttransModule = window.AnalyticsMainModule;
                    if (!posttransModule) throw new Error('Analytics module not available');
                    const posttransData = posttransModule.getFinalDataForAnalysis();
                    if (!posttransData || !posttransData.length) throw new Error('No post-transformation data available');
                    tickers = posttransData.map(x => x.ticker).filter(Boolean);
                    break;

                case 'yahoo_master':
                    const masterResp = await fetch('/api/yahoo/master_tickers');
                    if (!masterResp.ok) throw new Error('Failed to fetch Yahoo master tickers');
                    tickers = await masterResp.json();
                    break;
            }

            if (!tickers.length) {
                throw new Error('No valid tickers found');
            }

            // Show summary of tickers to be processed
            showTickerSummary(tickers);

            // Make the API call to fetch data
            const response = await fetch('/api/yahoo/mass_fetch', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    tickers: tickers,
                    source: source
                })
            });

            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.detail || 'Failed to fetch data');
            }

            const result = await response.json();
            // Update status message to match backend response format
            if (result.job_id) {
                showStatus('Fetch job started. Processing tickers...', false);
                // Start polling for status
                pollJobStatus(result.job_id, source === 'user_defined' ? userDefinedInput : null);
            } else {
                throw new Error('No job ID returned from server');
            }

        } catch (error) {
            showStatus(error.message, true);
            // Clear input on error if it was user defined
            if (source === 'user_defined') {
                userDefinedInput.value = '';
            }
        } finally {
            hideSpinner(fetchBtn);
        }
    }

    // Update polling function to accept input element
    async function pollJobStatus(jobId, inputElementToClear = null) {
        const statusInterval = setInterval(async () => {
            try {
                const statusResp = await fetch(`/api/yahoo/mass_fetch/status/${jobId}`);
                if (!statusResp.ok) {
                    throw new Error('Failed to get job status');
                }
                const statusData = await statusResp.json();
                const { current, total, last_ticker, status, success_count, error_count } = statusData;

                if (status === 'running') {
                    showStatus(`Processing ticker ${current}/${total}${last_ticker ? ': ' + last_ticker : ''}`, false);
                } else if (status === 'completed' || status === 'partial_failure' || status === 'failed') {
                    clearInterval(statusInterval);
                    const message = `Successfully processed: ${success_count}, Errors: ${error_count}`;
                    showStatus(message, status !== 'completed');
                    // Clear input if provided and job is done
                    if (inputElementToClear) {
                        inputElementToClear.value = '';
                    }
                }
            } catch (error) {
                clearInterval(statusInterval);
                showStatus('Error checking job status: ' + error.message, true);
                // Clear input on error if provided
                if (inputElementToClear) {
                    inputElementToClear.value = '';
                }
            }
        }, 1000); // Poll every second
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