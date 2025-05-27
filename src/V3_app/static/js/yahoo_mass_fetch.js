// Yahoo Mass Fetch UI and Ticker Source Logic
(function() {
    // --- UI State Management ---
    const fixedJobId = "yahoo_mass_fetch_main_v2";
    let isFetching = false;
    let currentTickers = [];
    let jobPollIntervalId = null;

    // --- UI Elements (ensure these are fetched after DOM is ready) ---
    let sourceSelect, dropZone, fileInput, fetchBtn, statusDiv, tickerSummaryDiv, progressBarContainer, progressBar;
    const originalFetchButtonText = "Fetch Yahoo Data"; // Store original button text

    function initializeDOMElements() {
        sourceSelect = document.getElementById('yahoo-mf-source');
        dropZone = document.getElementById('yahoo-mf-dropzone');
        fileInput = document.getElementById('yahoo-mf-file');
        fetchBtn = document.getElementById('yahoo-mf-fetch');
        statusDiv = document.getElementById('yahoo-mf-status');
        progressBarContainer = document.getElementById('yahoo-mf-progress-container');
        progressBar = document.getElementById('yahoo-mf-progress-bar');

        // Create tickerSummaryDiv if it doesn't exist (idempotent)
        tickerSummaryDiv = document.getElementById('yahoo-mf-ticker-summary');
        if (!tickerSummaryDiv && fetchBtn && fetchBtn.parentNode) {
            tickerSummaryDiv = createEl('div', { id: 'yahoo-mf-ticker-summary', class: 'mt-2 small text-muted' });
            fetchBtn.parentNode.insertBefore(tickerSummaryDiv, fetchBtn.nextSibling);
        }
        // Create progress bar dynamically if it doesn't exist (idempotent)
        if (!progressBarContainer && statusDiv && statusDiv.parentNode) {
            progressBarContainer = createEl('div', { id: 'yahoo-mf-progress-container', class: 'progress mt-2', style: 'height: 20px; display: none;' });
            progressBar = createEl('div', { 
                id: 'yahoo-mf-progress-bar', 
                class: 'progress-bar progress-bar-striped progress-bar-animated', 
                role: 'progressbar', 
                style: 'width: 0%;',
                'aria-valuenow': '0', 
                'aria-valuemin': '0', 
                'aria-valuemax': '100' 
            });
            progressBarContainer.appendChild(progressBar);
            statusDiv.parentNode.insertBefore(progressBarContainer, statusDiv.nextSibling);
        } else if (progressBarContainer) {
            // Ensure it starts hidden
             progressBarContainer.style.display = 'none';
        }
    }

    // --- Helper: Create DOM elements (if not already defined in this scope) ---
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

    function showTickerSummary(tickers) {
        if (!tickerSummaryDiv) return;
        if (!tickers || !tickers.length) {
            tickerSummaryDiv.textContent = 'No tickers found.';
            if(fetchBtn) fetchBtn.disabled = true;
            return;
        }
        const exampleCount = Math.min(5, tickers.length);
        const exampleTickers = tickers.slice(0, exampleCount).join(', ');
        const selectedSource = sourceSelect ? sourceSelect.value : 'unknown';
        let message = `Tickers found: ${tickers.length}. Example: ${exampleTickers}${tickers.length > exampleCount ? ', ...' : ''}`;
        
        if (selectedSource === 'pretrans') {
            message += ' (from loaded data)';
        } else if (selectedSource === 'posttrans') {
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
        if(fetchBtn) fetchBtn.disabled = false;
    }

    function setDropZoneEnabled(enabled) {
        if (!dropZone) return;
        dropZone.style.pointerEvents = enabled ? 'auto' : 'none';
        dropZone.style.opacity = enabled ? '1' : '0.5';
    }

    function showSpinner(button) {
        if (!button) return;
        const spinner = button.querySelector('.spinner-border');
        const text = button.querySelector('.button-text');
        if (spinner) spinner.style.display = 'inline-block';
        if (text) text.style.display = 'none';
        button.disabled = true;
    }

    function hideSpinner(button) {
        if (!button) return;
        const spinner = button.querySelector('.spinner-border');
        const text = button.querySelector('.button-text');
        if (spinner) spinner.style.display = 'none';
        if (text) text.style.display = 'inline';
        button.disabled = false;
    }

    // NEW: disableButton and enableButton functions
    function disableButton(button, text = "Processing...") {
        if (!button) return;
        const spinnerEl = button.querySelector('.spinner-border');
        const textEl = button.querySelector('.button-text');
        
        if (textEl) {
            // Store original text if not already stored, or if it's different (e.g. after an error state)
            if (!button.dataset.originalText || button.dataset.originalText !== textEl.textContent) {
                button.dataset.originalText = textEl.textContent;
            }
            textEl.textContent = text; // Set new text
            textEl.style.display = 'inline'; // Ensure text is visible initially for sizing
        }
        if (spinnerEl) spinnerEl.style.display = 'inline-block';
        // if (textEl && spinnerEl) textEl.style.marginLeft = '5px'; // Optional: add some space
        
        button.disabled = true;
    }

    function enableButton(button) {
        if (!button) return;
        const spinnerEl = button.querySelector('.spinner-border');
        const textEl = button.querySelector('.button-text');

        if (spinnerEl) spinnerEl.style.display = 'none';
        if (textEl) {
            textEl.textContent = button.dataset.originalText || originalFetchButtonText; // Restore original or default text
            textEl.style.display = 'inline';
            // textEl.style.marginLeft = '0'; // Reset margin if set
        }
        button.disabled = false;
    }

    function showStatus(message, messageType = 'info') {
        if (!statusDiv) return;
        statusDiv.textContent = message;
        let className = 'text-muted';
        if (messageType === 'error') {
            className = 'text-danger';
        } else if (messageType === 'success') {
            className = 'text-success';
        }
        statusDiv.className = `mt-2 small ${className}`;
    }
    
    function showProgressBarContainer() {
        if (progressBarContainer) {
            progressBarContainer.style.display = 'flex';
        }
    }

    // --- NEW: Function to update UI elements based on job data (MOVED INSIDE IIFE) ---
    function updateUIFromJobData(data) {
        const uiUpdateEntryTime = new Date().toISOString();
        console.log(`[updateUIFromJobData ENTRY @ ${uiUpdateEntryTime}] Called with data (raw):`, data, `Current isFetching before update: ${isFetching}`);
        // Deep copy for logging to avoid showing stale data if 'data' object is mutated elsewhere later by reference (though it shouldn't be)
        // console.log(`[updateUIFromJobData ENTRY @ ${uiUpdateEntryTime}] Called with data (cloned):`, JSON.parse(JSON.stringify(data)), `Current isFetching before update: ${isFetching}`);

        if (!fetchBtn || !progressBar || !progressBarContainer || !statusDiv) {
            console.warn(`[updateUIFromJobData @ ${new Date().toISOString()}] Required UI elements not yet available. Cannot update UI.`);
            return;
        }
        // Log incoming data and current isFetching state
        // console.log(`[updateUIFromJobData] Called with data:`, JSON.parse(JSON.stringify(data)), `Current isFetching before update: ${isFetching}`);

        let displayMessage = data.message || data.progress_message || `Status: ${data.status}`;
        let msgType = 'info'; // Default message type

        // Ensure critical UI elements are updated reliably
        if (data.status === 'queued' || data.status === 'running') {
            if (progressBarContainer) {
                progressBarContainer.style.display = 'flex';
                void progressBarContainer.offsetHeight; // Force reflow
                // console.log(`[updateUIFromJobData @ ${new Date().toISOString()}] Progress bar container set to flex.`);
            }
            if (progressBar) {
                const percent = data.progress_percent !== undefined && data.progress_percent !== null ? data.progress_percent : (data.current_count && data.total_count ? Math.round((data.current_count / data.total_count) * 100) : 0);
                progressBar.style.width = percent + '%';
                progressBar.textContent = percent + '%';
                // console.log(`[updateUIFromJobData @ ${new Date().toISOString()}] Progress bar updated to ${percent}%.`);
            }
            if (fetchBtn && !fetchBtn.disabled) { // Only disable if not already, to avoid redundant spinner logic
                disableButton(fetchBtn, "Fetching...");
                // console.log(`[updateUIFromJobData @ ${new Date().toISOString()}] Fetch button disabled.`);
            }
            isFetching = true; // Mark as fetching
            // displayMessage = data.progress_message || data.message || `Job is ${data.status}...`; // Already set above
            msgType = 'info';
            // console.log(`[updateUIFromJobData @ ${new Date().toISOString()}] Status is running/queued. Set isFetching to true.`);

        } else if (data.status === 'completed' || data.status === 'failed' || data.status === 'error') {
            if (fetchBtn && fetchBtn.disabled) { // Only enable if disabled
                enableButton(fetchBtn); // Restore original text, remove spinner
                console.log(`[updateUIFromJobData @ ${new Date().toISOString()}] Fetch button enabled.`);
            }
            if (progressBarContainer) {
                progressBarContainer.style.display = 'none';
                console.log(`[updateUIFromJobData @ ${new Date().toISOString()}] Progress bar container hidden.`);
            }
            // displayMessage remains as set above
            if (data.status === 'failed' || data.status === 'error') {
                msgType = 'error';
                } else {
                msgType = 'success'; // for 'completed'
            }
            isFetching = false; // Mark as not fetching
            console.log(`[updateUIFromJobData @ ${new Date().toISOString()}] Status is terminal (${data.status}). Set isFetching to false.`);

            // If job is terminal and polling interval exists, clear it.
            if (jobPollIntervalId) {
                console.log(`[updateUIFromJobData @ ${new Date().toISOString()}] Job is terminal and jobPollIntervalId exists. Clearing interval: ${jobPollIntervalId}`);
                clearInterval(jobPollIntervalId);
                jobPollIntervalId = null;
            }
        }

        // Update status message
        if (statusDiv) {
            statusDiv.textContent = displayMessage;
            statusDiv.className = `status-message ${msgType}`; // Apply class for styling (info, success, error)
            console.log(`[updateUIFromJobData @ ${new Date().toISOString()}] Status message set to: "${displayMessage}", type: ${msgType}`);
        }
        const uiUpdateExitTime = new Date().toISOString();
        console.log(`[updateUIFromJobData EXIT @ ${uiUpdateExitTime}] Finished. isFetching after update: ${isFetching}`);
    }

    // --- NEW: Helper to reset UI for a new fetch (MOVED INSIDE IIFE) ---
    function resetUIForNewFetch() {
        if(fetchBtn) {
            hideSpinner(fetchBtn);
            fetchBtn.disabled = false;
        }
        if (progressBarContainer) {
            progressBarContainer.style.display = 'none';
        }
        isFetching = false;
        showStatus("Ready to fetch Yahoo data.", "info");
    }

    // --- NEW: Function to fetch and apply current job status (MOVED INSIDE IIFE) ---
    async function fetchCurrentJobStatus() {
        console.log(`[fetchCurrentJobStatus @ ${new Date().toISOString()}] Fetching current status for job ${fixedJobId}...`);
        try {
            const response = await fetch(`/api/analytics/yahoo-job/details/${fixedJobId}`);
            if (!response.ok) {
                const errorText = await response.text();
                console.error(`[fetchCurrentJobStatus @ ${new Date().toISOString()}] Failed to fetch job details for ${fixedJobId}. Status: ${response.status}, Msg: ${errorText}`);
                showStatus(`Error fetching current job status: ${response.statusText}`, 'error');
                resetUIForNewFetch(); 
                return;
            }
            const data = await response.json();
            console.log(`[fetchCurrentJobStatus @ ${new Date().toISOString()}] Received job details:`, data);
            updateUIFromJobData(data); // This will set isFetching and update UI elements

            // If the job is active (running or queued), start the polling interval.
            // isFetching is set by updateUIFromJobData based on data.status.
            if (isFetching) { 
                console.log(`[fetchCurrentJobStatus @ ${new Date().toISOString()}] Job ${fixedJobId} is ${data.status}. Initiating polling interval (3000ms).`);
                if (jobPollIntervalId) clearInterval(jobPollIntervalId); // Clear any existing interval
                jobPollIntervalId = setInterval(() => pollJobStatus(fixedJobId), 3000); // Use 3000ms interval
            } else {
                // If job is not active (e.g. completed, failed), ensure any old interval is cleared.
                // updateUIFromJobData already handles clearing jobPollIntervalId for terminal states it processes.
                // This is an additional safeguard if fetchCurrentJobStatus is called and finds a terminal job when an interval might have existed.
                if (jobPollIntervalId) {
                    console.log(`[fetchCurrentJobStatus @ ${new Date().toISOString()}] Job ${fixedJobId} is ${data.status} (not active). Clearing any existing interval: ${jobPollIntervalId}`);
                    clearInterval(jobPollIntervalId);
                    jobPollIntervalId = null;
                }
            }
        } catch (error) {
            console.error(`[fetchCurrentJobStatus @ ${new Date().toISOString()}] Error in fetchCurrentJobStatus for ${fixedJobId}:`, error);
            showStatus(`Network error fetching job status: ${error.message}`, 'error');
            resetUIForNewFetch();
        }
    }

    // --- Event Handlers (handleSourceChange, handleFetch, handleFileSelect) ... (MOVED INSIDE IIFE) ---
    // Ensure they use the IIFE-scoped variables (fetchBtn, statusDiv, etc.)
    // Minor adjustments might be needed if they were relying on global fixedJobId etc.
    
    async function handleSourceChange() {
        if (!sourceSelect) return;
        const source = sourceSelect.value;
        const userDefinedContainer = document.getElementById('user-defined-tickers-container');
        const userDefinedInput = document.getElementById('user-defined-tickers');

        setDropZoneEnabled(false);
        if(fetchBtn) fetchBtn.disabled = true;
        showStatus('');
        if(userDefinedContainer) userDefinedContainer.style.display = 'none';
        if(userDefinedInput) userDefinedInput.value = '';

        switch (source) {
            case 'upload': setDropZoneEnabled(true); break;
            case 'user_defined':
                if(userDefinedContainer) userDefinedContainer.style.display = 'block';
                if(userDefinedInput) userDefinedInput.addEventListener('input', () => { if(fetchBtn) fetchBtn.disabled = !userDefinedInput.value.trim(); });
                break;
            case 'screener':
            case 'portfolio':
            case 'pretrans':
            case 'posttrans':
            case 'yahoo_master':
                if(fetchBtn) fetchBtn.disabled = false;
                break;
            default: if(fetchBtn) fetchBtn.disabled = false; break;
        }
    }

    async function handleFetch() {
        if (!sourceSelect || !fetchBtn) return;

        if (tickerSummaryDiv) { // Clear ticker summary message
            tickerSummaryDiv.textContent = '';
        }

        // Initial UI state for starting a fetch
        updateUIFromJobData({
            job_id: fixedJobId,
            status: 'queued', // Representing the frontend action of queueing the request
            progress_message: 'Preparing to fetch tickers...',
            total_count: 0, 
            current_count: 0,
            progress_percent: 0
        });
        // isFetching should be true here due to status 'queued'
        // console.log(`[handleFetch] After initial 'Preparing' update, isFetching: ${isFetching}`);

        const source = sourceSelect.value;
        const userDefinedInputValue = document.getElementById('user-defined-tickers') ? document.getElementById('user-defined-tickers').value : '';
        let tickersToFetch = [];

        try {
            switch (source) {
                case 'upload':
                    if (!fileInput || !fileInput.files.length) throw new Error('Please select a file first');
                    const file = fileInput.files[0];
                    const text = await file.text();
                    tickersToFetch = text.split('\n').map(line => line.trim()).filter(line => line && !line.startsWith('#'));
                    break;
                case 'user_defined':
                    if (!userDefinedInputValue.trim()) throw new Error('Please enter at least one ticker symbol');
                    tickersToFetch = userDefinedInputValue.split(',').map(t => t.trim().toUpperCase()).filter(Boolean);
                    break;
                case 'screener':
                    const screenerResp = await fetch('/api/screener/tickers');
                    if (!screenerResp.ok) throw new Error(`Failed to fetch screener tickers: ${screenerResp.statusText}`);
                    tickersToFetch = await screenerResp.json();
                    break;
                case 'portfolio':
                    console.log('[Debug Portfolio] Fetching /api/portfolio/tickers...');
                    const portfolioResp = await fetch('/api/portfolio/tickers');
                    if (!portfolioResp.ok) {
                        const errorText = await portfolioResp.text();
                        throw new Error(`Failed to fetch portfolio tickers: ${portfolioResp.statusText} - ${errorText}`);
                    }
                    const portfolioData = await portfolioResp.json();
                    if (Array.isArray(portfolioData) && portfolioData.every(item => typeof item === 'string')) {
                        tickersToFetch = portfolioData.filter(Boolean);
                    } else if (Array.isArray(portfolioData)) {
                        tickersToFetch = portfolioData.map(t => t.ticker).filter(Boolean);
                    } else {
                        tickersToFetch = [];
                    }
                    break;
                case 'pretrans':
                    const pretransModule = window.AnalyticsMainModule;
                    if (!pretransModule) throw new Error('Analytics module not available');
                    const pretransData = pretransModule.getFullProcessedData();
                    if (!pretransData || !pretransData.length) throw new Error('No pre-transformation data available');
                    tickersToFetch = pretransData.map(x => x.ticker).filter(Boolean);
                    break;
                case 'posttrans':
                    const posttransModule = window.AnalyticsMainModule;
                    if (!posttransModule) throw new Error('Analytics module not available');
                    const posttransData = posttransModule.getFinalDataForAnalysis();
                    if (!posttransData || !posttransData.length) throw new Error('No post-transformation data available');
                    tickersToFetch = posttransData.map(x => x.ticker).filter(Boolean);
                    break;
                case 'yahoo_master':
                    const masterResp = await fetch('/api/yahoo/master_tickers');
                    if (!masterResp.ok) throw new Error(`Failed to fetch Yahoo master tickers: ${masterResp.statusText}`);
                    tickersToFetch = await masterResp.json();
                    break;
                default: throw new Error(`Unsupported source: ${source}`);
            }

            if (!tickersToFetch || tickersToFetch.length === 0) {
                throw new Error('No tickers found for the selected source.');
            }
            
            updateUIFromJobData({
                job_id: fixedJobId,
                status: 'queued',
                progress_message: `Triggering Yahoo mass fetch for ${tickersToFetch.length} tickers...`,
                total_count: tickersToFetch.length,
                current_count: 0,
                progress_percent: 0
            });

            const response = await fetch('/api/analytics/yahoo-job/trigger', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ tickers: tickersToFetch })
            });
            const triggerResultJobDetails = await response.json(); // Parse the response from /trigger

            if (!response.ok) {
                const errorMsg = triggerResultJobDetails.detail || `Failed to trigger job: ${response.statusText}`;
                resetUIForNewFetch();
                showStatus(errorMsg, 'error');
                throw new Error(errorMsg);
            }
            
            console.log('[Job Triggered] Received initial job details from /trigger POST:', triggerResultJobDetails);
            updateUIFromJobData(triggerResultJobDetails); // Update UI with the immediate response from trigger

            if (triggerResultJobDetails.job_id) {
                // Force isFetching to true before calling fetchCurrentJobStatus in the click-and-wait path
                // to ensure the polling interval is set up by fetchCurrentJobStatus.
                // The subsequent call to updateUIFromJobData within fetchCurrentJobStatus will then set it accurately.
                isFetching = true; 
                console.log(`[handleFetch @ ${new Date().toISOString()}] Manually set isFetching=true. Trigger successful for ${triggerResultJobDetails.job_id}. Immediately calling fetchCurrentJobStatus().`);
                fetchCurrentJobStatus(); // This will handle UI update and start setInterval if job is active.

            } else {
                console.warn("[handleFetch] No job_id in triggerResultJobDetails. Cannot call fetchCurrentJobStatus.");
                // UI should have been set to an error state by updateUIFromJobData with triggerResultJobDetails
            }

            if (source === 'upload' && fileInput) fileInput.value = '';

        } catch (error) {
            console.error('[Handle Fetch Error] Error during fetch operation:', error);
            if (typeof response === 'undefined' || (response && response.ok)) {
                showStatus(`Error: ${error.message}`, 'error');
            }
            resetUIForNewFetch();
        }
    }

    // --- NEW: Polling function ---
    async function pollJobStatus(jobId) {
        const pollStartTime = new Date().toISOString();
        // console.log(`[pollJobStatus ENTRY @ ${pollStartTime}] JobId: ${jobId}. isFetching (global): ${isFetching}, jobPollIntervalId: ${jobPollIntervalId}`);

        // If isFetching is false (meaning a terminal state was likely processed by another poll)
        // AND the jobPollIntervalId has been cleared, then this is likely a zombie poll.
        // Exception: The very first poll called by setTimeout when jobPollIntervalId might not be set yet, but isFetching is true.
        if (!isFetching && jobPollIntervalId === null) {
            // Only log and exit if this is NOT the scenario where fetchCurrentJobStatus on page load found a completed job.
            // In that specific case, isFetching is false, and jobPollIntervalId is null, but we *do* want that one updateUIFromJobData call.
            // We can infer this isn't from initial page load's fetchCurrentJobStatus if a fetch button exists and is disabled (active job) or enabled (just finished).
            // This check is imperfect but aims to prevent zombie polls after a job triggered by user action has finished.
            if (fetchBtn) { // Check if fetchBtn is initialized, meaning not the initial page load before DOM ready for fetchCurrentJobStatus.
                 console.log(`[pollJobStatus @ ${pollStartTime}] Aborting zombie poll: isFetching is false AND jobPollIntervalId is null. JobId: ${jobId}`);
                 return;
            }
        }

        console.log(`[pollJobStatus START @ ${pollStartTime}] Polling for job ${jobId}. Making fetch call.`);

        try {
            const response = await fetch(`/api/analytics/yahoo-job/details/${jobId}`);
            const receivedDetailsTime = new Date().toISOString();
            if (!response.ok) {
                console.error(`[pollJobStatus @ ${receivedDetailsTime}] Error fetching job details for ${jobId}: ${response.statusText}`);
                // Potentially stop polling on certain types of errors, or implement retry logic
                // For now, we'll let it continue polling or be stopped by isFetching logic in updateUI.
                try {
                    const errorData = await response.json();
                    console.error(`[pollJobStatus @ ${new Date().toISOString()}] Error details:`, errorData);
                    updateUIFromJobData({ job_id: jobId, status: 'error', message: errorData.detail || `Polling failed: ${response.statusText}` });
                } catch (e) {
                    updateUIFromJobData({ job_id: jobId, status: 'error', message: `Polling failed: ${response.statusText}. Could not parse error response.` });
                }
                return; // Return early after handling error
            }
            const details = await response.json();
            console.log(`[pollJobStatus @ ${receivedDetailsTime}] Received details for ${jobId}:`, JSON.parse(JSON.stringify(details)));
            updateUIFromJobData(details);

            // If the job is terminal, isFetching will be set to false by updateUIFromJobData.
            // The setInterval in handleFetch checks isFetching and clears itself.
            // No need to explicitly clear jobPollIntervalId here anymore as it might clear the one from handleFetch
            // if this pollJobStatus was called by the setTimeout.
            // console.log(`[pollJobStatus @ ${new Date().toISOString()}] After updateUIFromJobData. Current isFetching: ${isFetching}`);

        } catch (error) {
            console.error(`[pollJobStatus @ ${new Date().toISOString()}] Error in pollJobStatus for ${jobId}:`, error);
            // Consider how to update UI on such errors
            updateUIFromJobData({ job_id: jobId, status: 'error', message: `Polling error: ${error.message}` });
        } finally {
            // REMOVED: Setting isPollingInProgress = false or isFetching = false here.
            // isFetching is managed by updateUIFromJobData based on actual job status.
        }
        // console.log(`[pollJobStatus EXIT @ ${new Date().toISOString()}] JobId: ${jobId}.`);
    }

    async function handleFileSelect(event) {
        const file = event.target.files[0];
        if (!file) return;
        if (!file.name.toLowerCase().endsWith('.txt')) {
            showStatus('Please upload a .txt file.', 'error');
            if(tickerSummaryDiv) tickerSummaryDiv.textContent = '';
            if(fetchBtn) fetchBtn.disabled = true;
            return;
        }
        if(dropZone) dropZone.querySelector('.status-text').textContent = file.name;
        try {
            const text = await file.text();
            const parsedTickers = text.split(/\r?\n|,|;/).map(t => t.trim().toUpperCase()).filter(Boolean);
            currentTickers = Array.from(new Set(parsedTickers)).filter(Boolean);
            showTickerSummary(currentTickers);
        } catch (e) { /* ... */ }
    }

    // --- Initialize Event Listeners & DOM Elements ---
    function initializeApp() {
        initializeDOMElements();

        if (!sourceSelect || !fetchBtn || !dropZone || !fileInput) {
            console.error("Failed to initialize critical UI elements for Yahoo Mass Fetch. Aborting setup.");
            return;
        }

        sourceSelect.addEventListener('change', handleSourceChange);
        fetchBtn.addEventListener('click', handleFetch);
        dropZone.addEventListener('click', () => fileInput.click());
        fileInput.addEventListener('change', handleFileSelect);

        dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.style.backgroundColor = 'var(--bs-primary-bg-subtle)'; });
        dropZone.addEventListener('dragleave', e => { e.preventDefault(); dropZone.style.backgroundColor = 'var(--bs-tertiary-bg)'; });
        dropZone.addEventListener('drop', e => {
            e.preventDefault();
            dropZone.style.backgroundColor = 'var(--bs-tertiary-bg)';
            if (e.dataTransfer.files.length) {
                fileInput.files = e.dataTransfer.files;
                handleFileSelect({ target: { files: e.dataTransfer.files } });
            }
        });
        
        handleSourceChange();
        fetchCurrentJobStatus();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initializeApp);
    } else {
        initializeApp();
    }
})(); 