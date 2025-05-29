// Yahoo Mass Fetch UI and Ticker Source Logic
(function() {
    // --- UI State Management ---
    const fixedJobId = "yahoo_mass_fetch_main_v2";
    let isFetching = false;
    let currentTickers = [];
    let jobPollIntervalId = null;
    let activeYahooJobTriggerTime = null; // For filtering stale poll data

    // --- UI Elements (ensure these are fetched after DOM is ready) ---
    let sourceSelect, dropZone, fileInput, fetchBtn, statusDiv, tickerSummaryDiv, progressBarContainer, progressBar;
    const originalFetchButtonText = "Fetch Yahoo Data"; // Store original button text
    let yahooSourceHelpText; // Added for the help text
    let dropZoneTextSpan; // Added for the text span inside dropzone

    function initializeDOMElements() {
        sourceSelect = document.getElementById('yahoo-mf-source');
        dropZone = document.getElementById('yahoo-mf-dropzone');
        fileInput = document.getElementById('yahoo-mf-file');
        fetchBtn = document.getElementById('yahoo-mf-fetch');
        statusDiv = document.getElementById('yahoo-mf-status');
        progressBarContainer = document.getElementById('yahoo-mf-progress-container');
        progressBar = document.getElementById('yahoo-mf-progress-bar');
        // Ensure dropZoneTextSpan is correctly identified or created if needed.
        // The selector looks for .dropzone-text first, then .status-text as a fallback.
        if (dropZone) {
            dropZoneTextSpan = dropZone.querySelector('.dropzone-text');
            if (!dropZoneTextSpan) { // If .dropzone-text is not found, try .status-text
                dropZoneTextSpan = dropZone.querySelector('.status-text');
            }
            // If neither is found, and we need one, it should be created. 
            // For now, assuming one of these classes is present in the HTML span inside the dropzone.
        } else {
            dropZoneTextSpan = null;
        }

        // Create tickerSummaryDiv if it doesn't exist (idempotent)
        tickerSummaryDiv = document.getElementById('yahoo-mf-ticker-summary');
        if (!tickerSummaryDiv && fetchBtn && fetchBtn.parentNode) {
            tickerSummaryDiv = createEl('div', { id: 'yahoo-mf-ticker-summary', class: 'mt-2 small text-muted' });
            fetchBtn.parentNode.insertBefore(tickerSummaryDiv, fetchBtn.nextSibling);
        }

        // Create yahooSourceHelpText if it doesn't exist (idempotent)
        yahooSourceHelpText = document.getElementById('yahoo-source-help-text');
        if (!yahooSourceHelpText && sourceSelect && sourceSelect.parentNode) {
            yahooSourceHelpText = createEl('div', { id: 'yahoo-source-help-text', class: 'form-text small mt-1 mb-2', style: 'color: var(--bs-secondary-color);'});
            // Insert it after the sourceSelect dropdown, or its direct parent if the parent is a wrapper.
            // Assuming sourceSelect.parentNode is a good place to append relative to the dropdown.
            sourceSelect.parentNode.insertBefore(yahooSourceHelpText, sourceSelect.nextSibling);
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
            statusDiv.parentNode.insertBefore(progressBarContainer, statusDiv);
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
        // This function is now primarily for enabling/disabling the fetch button based on ticker count.
        // The display of ticker summary is handled directly in handleFileSelect within the dropzone.
        if (!fetchBtn) return;
        fetchBtn.disabled = !tickers || tickers.length === 0;

        // Clear or hide tickerSummaryDiv as it's no longer the primary display for file parse results.
        if (tickerSummaryDiv) {
            tickerSummaryDiv.textContent = ''; // Clear it
            // Alternatively, hide it: tickerSummaryDiv.style.display = 'none';
        }
    }

    function setDropZoneEnabled(enabled, source) {
        if (!dropZone) return;
        const textSpan = dropZoneTextSpan || (dropZone.querySelector('.dropzone-text') || dropZone.querySelector('.status-text'));

        if (enabled) {
            dropZone.style.pointerEvents = 'auto';
            dropZone.style.opacity = '1';
            dropZone.style.backgroundColor = 'var(--bs-tertiary-bg)';
            if (textSpan) textSpan.textContent = "Drag & drop .txt file(s) or click to select";
        } else {
            dropZone.style.pointerEvents = 'none';
            dropZone.style.opacity = '0.6';
            dropZone.style.backgroundColor = 'var(--bs-secondary-bg)'; // Or var(--bs-light-bg-subtle)
            if (textSpan) {
                if (source && source !== 'upload') {
                    textSpan.textContent = "File upload not applicable for this source.";
            } else {
                    // Default disabled text if source isn't specified or is upload but still disabled for other reasons
                    textSpan.textContent = "File upload disabled."; 
                }
            }
        }
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
            // Ensure we store the specific original text of *this* button if not already set.
            if (!button.dataset.originalText) {
                 // If the current text is already the 'text' param (e.g. "Fetching..."), 
                 // and original is not set, try to fall back to a known default for this button if applicable
                 // For now, we rely on originalFetchButtonText if textEl.textContent is a processing state.
                 // This logic is a bit tricky; ideally originalText is set when button is first enabled with its proper text.
                 // Let's simplify: if originalText is not there, set it to current text *unless* current text is the incoming 'text'
                if (textEl.textContent !== text) {
                button.dataset.originalText = textEl.textContent;
                } else {
                    // If current text IS the processing text, and we have no original,
                    // we must use the module-level constant.
                    // This handles the case where disableButton is called before enableButton ever sets an originalText.
                    button.dataset.originalText = originalFetchButtonText; 
                }
            }
            textEl.textContent = text; // Set new text
            textEl.style.display = 'inline'; 
        }
        if (spinnerEl) spinnerEl.style.display = 'inline-block';
        button.disabled = true;
    }

    function enableButton(button) {
        if (!button) return;
        const spinnerEl = button.querySelector('.spinner-border');
        const textEl = button.querySelector('.button-text');

        if (spinnerEl) spinnerEl.style.display = 'none';
        if (textEl) {
            textEl.textContent = button.dataset.originalText || originalFetchButtonText; 
            textEl.style.display = 'inline';
        }
        button.disabled = false;
        // Clear originalText after restoring so it can be freshly captured next time if needed
        // delete button.dataset.originalText; // Let's not delete it, it should be stable unless button text changes meaningfully
    }

    function formatDisplayTimestamp(dateInput) {
        if (!dateInput) return new Date().toLocaleTimeString(); // Fallback, though job data should have timestamps
        const date = new Date(dateInput);
        const day = String(date.getDate()).padStart(2, '0');
        const month = String(date.getMonth() + 1).padStart(2, '0'); // Months are 0-indexed
        const year = date.getFullYear();
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        return `${day}/${month}/${year} ${hours}:${minutes}`;
    }

    function getIconForAlertType(type) {
        switch (type) {
            case 'success': return '<i class="bi bi-check-circle-fill me-2"></i>';
            case 'error': return '<i class="bi bi-x-octagon-fill me-2"></i>';
            case 'warning': return '<i class="bi bi-exclamation-triangle-fill me-2"></i>';
            case 'info':
            default: return '<i class="bi bi-info-circle-fill me-2"></i>';
        }
    }

    function showYahooStatusAlert(message, type = 'info', timestampSource = null) {
        if (!statusDiv) {
            console.warn("[showYahooStatusAlert] statusDiv not initialized.");
            return;
        }
        let alertClass = 'alert-info';
        if (type === 'success') alertClass = 'alert-success';
        else if (type === 'error') alertClass = 'alert-danger';
        else if (type === 'warning') alertClass = 'alert-warning';

        const displayTimestamp = formatDisplayTimestamp(timestampSource || new Date());
        const icon = getIconForAlertType(type);

        statusDiv.innerHTML = 
            `<div class="alert ${alertClass} alert-dismissible fade show" role="alert">
                ${icon}[${displayTimestamp}] ${message}
                <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
            </div>`;
        console.log(`[Yahoo Status Alert @ ${new Date().toISOString()}] ${type.toUpperCase()}: ${message}`);
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

        // --- Universal Progress Bar Update (before deciding to hide it) ---
        // Update progress bar visuals if the data is available, regardless of current job status.
        // This ensures that if a terminal state (e.g., completed, partial_failure) has 100% progress, it's shown briefly.
        if (progressBar) {
            let percentValue = 0;
            if (data.progress_percent !== undefined && data.progress_percent !== null) {
                percentValue = data.progress_percent;
            } else if (data.current_count !== undefined && data.total_count !== undefined && data.total_count > 0) {
                percentValue = Math.round((data.current_count / data.total_count) * 100);
            }
            progressBar.style.width = percentValue + '%';
            progressBar.textContent = percentValue + '%';
            progressBar.setAttribute('aria-valuenow', percentValue);
            // Apply Bootstrap classes and height for progress bar and container
            if (progressBarContainer) {
                progressBarContainer.classList.add('progress', 'mt-2');
                progressBarContainer.style.height = '20px';
            }
            progressBar.classList.add('progress-bar', 'progress-bar-striped', 'progress-bar-animated');
            // Conditional coloring (though it might be hidden quickly for terminal states)
            if (data.status === 'partial_failure') {
                progressBar.classList.remove('bg-primary', 'bg-danger'); // Remove others
                progressBar.classList.add('bg-warning');
            } else if (data.status === 'failed' || data.status === 'error') {
                progressBar.classList.remove('bg-primary', 'bg-warning');
                progressBar.classList.add('bg-danger');
            } else {
                progressBar.classList.remove('bg-warning', 'bg-danger');
                // progressBar.classList.add('bg-primary'); // Default Bootstrap blue, often no specific class needed
            }
        }

        // Log incoming data and current isFetching state
        // console.log(`[updateUIFromJobData] Called with data:`, JSON.parse(JSON.stringify(data)), `Current isFetching before update: ${isFetching}`);

        let displayMessage = '';
        let msgType = 'info';
        const jobStatus = data.status || 'unknown';
        const timestampForAlert = data.last_updated || data.job_end_time || data.last_triggered_time || new Date();

        if (jobStatus === 'completed' || jobStatus === 'failed' || jobStatus === 'error' || jobStatus === 'partial_failure') {
            msgType = (jobStatus === 'completed') ? 'success' : 'error';
            if (jobStatus === 'partial_failure') msgType = 'warning';

            let statusText = `Status: ${jobStatus}.`;
            let countsText = ''; // Ensure it starts empty
            
            // Determine Tickers count
            const tickersDisplayCount = data.total_processed_count !== undefined 
                ? data.total_processed_count 
                : (data.total_count !== undefined ? data.total_count : 0);
            countsText += ` Tickers: ${tickersDisplayCount}.`; // Append Tickers count
            
            // Determine Errors count
            const errorDisplayCount = data.failed_count !== undefined ? data.failed_count : 0;
            countsText += ` Errors: ${errorDisplayCount}.`; // Append Errors count
            
            displayMessage = statusText + countsText; // Combine status with counts

            if (errorDisplayCount > 0 && data.sample_error_tickers && data.sample_error_tickers.length > 0) {
                const MAX_SAMPLE_ERRORS = 5; 
                const sampleTickers = data.sample_error_tickers.slice(0, MAX_SAMPLE_ERRORS).join(', ');
                displayMessage += ` Errored tickers sample: ${sampleTickers}${data.sample_error_tickers.length > MAX_SAMPLE_ERRORS ? ', ...' : ''}.`;
            }

        } else if (jobStatus === 'running' && data.progress_message) {
            displayMessage = data.progress_message;
            msgType = 'info';
        } else if (data.message) { // General message for other states (queued, idle)
            displayMessage = data.message;
            msgType = 'info';
        } else if (jobStatus === 'queued' && data.progress_message) {
             displayMessage = data.progress_message;
             msgType = 'info';
        } else {
            displayMessage = `Status: ${jobStatus}`;
            msgType = 'info';
        }

        if (jobStatus === 'queued' || jobStatus === 'running') {
            if (progressBarContainer) progressBarContainer.style.display = 'flex';
            if (fetchBtn && !fetchBtn.disabled) disableButton(fetchBtn, "Fetching...");
            isFetching = true;
        } else { // completed, failed, error, partial_failure, idle
            if (progressBarContainer) progressBarContainer.style.display = 'none';
            if (fetchBtn && fetchBtn.disabled) enableButton(fetchBtn);
            isFetching = false;
            if (jobPollIntervalId) {
                clearInterval(jobPollIntervalId);
                jobPollIntervalId = null;
            }
        }
        showYahooStatusAlert(displayMessage, msgType, timestampForAlert);
        const uiUpdateExitTime = new Date().toISOString();
        console.log(`[updateUIFromJobData EXIT @ ${uiUpdateExitTime}] Finished. isFetching after update: ${isFetching}`);
    }

    // --- NEW: Helper to reset UI for a new fetch (MOVED INSIDE IIFE) ---
    function resetUIForNewFetch() {
        if(fetchBtn) {
            enableButton(fetchBtn);
        }
        if (progressBarContainer) {
            progressBarContainer.style.display = 'none';
        }
        if (statusDiv) statusDiv.innerHTML = ''; // Clear old alerts
        isFetching = false;
        activeYahooJobTriggerTime = null; // Reset active job trigger time
        if (jobPollIntervalId) {
            clearInterval(jobPollIntervalId);
            jobPollIntervalId = null;
        }
    }

    // --- NEW: Function to fetch and apply current job status (MOVED INSIDE IIFE) ---
    async function fetchCurrentJobStatus() {
        console.log(`[fetchCurrentJobStatus @ ${new Date().toISOString()}] Fetching current status for job ${fixedJobId}...`);
        try {
            const response = await fetch(`/api/analytics/yahoo-job/details/${fixedJobId}`);
            if (!response.ok) {
                const errorText = await response.text();
                console.error(`[fetchCurrentJobStatus @ ${new Date().toISOString()}] Failed to fetch job details for ${fixedJobId}. Status: ${response.status}, Msg: ${errorText}`);
                showYahooStatusAlert(`Error fetching job status: ${response.statusText} - ${errorText}`, 'error');
                resetUIForNewFetch(); 
                return;
            }
            const data = await response.json();
            console.log(`[fetchCurrentJobStatus @ ${new Date().toISOString()}] Received job details:`, data);
            
            if (data && data.last_triggered_time) {
                activeYahooJobTriggerTime = data.last_triggered_time;
            } else if (!data.status || data.status === 'idle') {
                 // If job is idle or has no trigger time, effectively no active job to track for polling purposes
                activeYahooJobTriggerTime = null;
            }
            // No time-based filtering here, as this is to establish initial state or recover.
            updateUIFromJobData(data); 

            if (isFetching) { 
                if (jobPollIntervalId) clearInterval(jobPollIntervalId);
                jobPollIntervalId = setInterval(() => pollJobStatus(fixedJobId), 3000);
            }
        } catch (error) {
            console.error(`[fetchCurrentJobStatus @ ${new Date().toISOString()}] Error in fetchCurrentJobStatus for ${fixedJobId}:`, error);
            showYahooStatusAlert(`Network error fetching job status: ${error.message}`, 'error');
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
        let userDefinedInput = document.getElementById('user-defined-tickers');

        // Update help text based on selected source
        if (yahooSourceHelpText) {
            switch (source) {
                case 'upload':
                    yahooSourceHelpText.textContent = "Upload a .txt file (one ticker per line, or comma/semicolon separated).";
                    break;
                case 'user_defined':
                    yahooSourceHelpText.textContent = "Enter ticker symbols separated by commas.";
                    break;
                case 'screener':
                    yahooSourceHelpText.textContent = "Fetches tickers from the general screener.";
                    break;
                case 'portfolio':
                    yahooSourceHelpText.textContent = "Fetches tickers from your saved portfolio.";
                    break;
                case 'pretrans':
                    yahooSourceHelpText.textContent = "Uses tickers from the current analytics data table (pre-transformation).";
                    break;
                case 'posttrans':
                    yahooSourceHelpText.textContent = "Uses tickers from the current analytics data table (post-transformation).";
                    break;
                case 'yahoo_master':
                    yahooSourceHelpText.textContent = "Fetches all tickers from the Yahoo master list.";
                    break;
                default:
                    yahooSourceHelpText.textContent = "Select a data source."; // Default or clear
            }
        }

        if (source === 'upload') {
            setDropZoneEnabled(true, source);
            if (tickerSummaryDiv) tickerSummaryDiv.textContent = ''; // Clear previous summary
            if (fetchBtn) fetchBtn.disabled = true; // Disabled until a file is selected
        } else {
            setDropZoneEnabled(false, source);
            if (tickerSummaryDiv) tickerSummaryDiv.textContent = ''; // Clear ticker summary if not upload
            if (fetchBtn) {
                // Enable button for non-upload sources that don't require further input before fetching
                fetchBtn.disabled = source === 'user_defined' ? !userDefinedInput.value.trim() : false;
            }
        }
        
        showYahooStatusAlert(''); // Clear general status messages
        if(userDefinedContainer) {
            userDefinedContainer.style.display = source === 'user_defined' ? 'block' : 'none';
        }
        if(userDefinedInput) {
            userDefinedInput.value = ''; // Clear user input when source changes
             if (source === 'user_defined') {
                userDefinedInput.addEventListener('input', () => { 
                    if(fetchBtn) fetchBtn.disabled = !userDefinedInput.value.trim(); 
                });
            }
        }
         // Original switch logic for enabling fetch button for non-upload sources
        // This is now partially handled by the setDropZoneEnabled logic block above for fetchBtn.disabled
        // Re-check and simplify this part.
        switch (source) {
            // case 'upload': // Handled above
            //     break;
            case 'user_defined':
                if(userDefinedContainer) userDefinedContainer.style.display = 'block';
                if(userDefinedInput) {
                    // Clear previous listener to avoid multiple triggers
                    const newUserDefinedInput = userDefinedInput.cloneNode(true);
                    userDefinedInput.parentNode.replaceChild(newUserDefinedInput, userDefinedInput);
                    userDefinedInput = newUserDefinedInput;
                    userDefinedInput.addEventListener('input', () => { 
                        if(fetchBtn) fetchBtn.disabled = !userDefinedInput.value.trim(); 
                    });
                }
                if(fetchBtn) fetchBtn.disabled = !userDefinedInput.value.trim();
                break;
            case 'screener':
            case 'portfolio':
            case 'pretrans':
            case 'posttrans':
            case 'yahoo_master':
                if(fetchBtn) fetchBtn.disabled = false;
                break;
            default:
                // For unknown sources, keep button disabled or based on specific logic
                if(fetchBtn) fetchBtn.disabled = true; 
                break;
        }
    }

    async function handleFetch() {
        if (!sourceSelect || !fetchBtn) return;

        // **** Critical Debug Log ****
        console.log('[handleFetch] Entry. sourceSelect.id:', sourceSelect.id, 'sourceSelect.value IS:', sourceSelect.value);

        resetUIForNewFetch(); // Clear previous status and prepare for new fetch

        if (tickerSummaryDiv) { 
            tickerSummaryDiv.textContent = '';
        }
        // Initial UI state for starting a fetch handled by resetUIForNewFetch and subsequent updates
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

        // **** Critical Debug Log ****
        console.log('[handleFetch] Source variable set to:', source, "Proceeding to switch.");

        try {
            switch (source) {
                case 'upload':
                    console.log('[handleFetch] Entered "upload" case.'); // Log case entry
                    // Check currentTickers, which is populated by handleFileSelect
                    if (!currentTickers || currentTickers.length === 0) {
                        console.error('[handleFetch] "upload" case: currentTickers is empty. A file should have been processed by handleFileSelect.');
                        throw new Error('No tickers processed from file. Please select a valid file.');
                    }
                    tickersToFetch = currentTickers; // Use the already parsed tickers
                    // No need to re-read fileInput.files[0] as it's cleared by handleFileSelect
                    console.log(`[handleFetch] "upload" case: Using ${currentTickers.length} pre-parsed tickers.`);
                    break;
                case 'user_defined':
                    if (!userDefinedInputValue.trim()) throw new Error('Please enter at least one ticker symbol');
                    tickersToFetch = userDefinedInputValue.split(',').map(t => t.trim().toUpperCase()).filter(Boolean);
                    break;
                case 'screener':
                    const screenerResp = await fetch('/api/screener/tickers');
                    if (!screenerResp.ok) throw new Error(`Failed to fetch screener tickers: ${screenerResp.statusText}`);
                    tickersToFetch = await screenerResp.json();
                    tickersToFetch = tickersToFetch.map(t => typeof t === 'string' ? t.toUpperCase() : t); // Ensure uppercase
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
                        tickersToFetch = portfolioData.filter(Boolean).map(t => t.toUpperCase()); // Ensure uppercase
                    } else if (Array.isArray(portfolioData)) {
                        tickersToFetch = portfolioData.map(t => t.ticker).filter(Boolean).map(t => t.toUpperCase()); // Ensure .ticker is uppercase
                    } else {
                        tickersToFetch = [];
                    }
                    break;
                case 'pretrans':
                    const pretransModule = window.AnalyticsMainModule;
                    if (!pretransModule) throw new Error('Analytics module not available');
                    const pretransData = pretransModule.getFullProcessedData();
                    if (!pretransData || !pretransData.length) throw new Error('No pre-transformation data available');
                    tickersToFetch = pretransData.map(x => x.ticker).filter(Boolean).map(t => t.toUpperCase()); // Ensure uppercase
                    break;
                case 'posttrans':
                    const posttransModule = window.AnalyticsMainModule;
                    if (!posttransModule) throw new Error('Analytics module not available');
                    const posttransData = posttransModule.getFinalDataForAnalysis();
                    if (!posttransData || !posttransData.length) throw new Error('No post-transformation data available');
                    tickersToFetch = posttransData.map(x => x.ticker).filter(Boolean).map(t => t.toUpperCase()); // Ensure uppercase
                    break;
                case 'yahoo_master':
                    const masterResp = await fetch('/api/yahoo/master_tickers');
                    if (!masterResp.ok) throw new Error(`Failed to fetch Yahoo master tickers: ${masterResp.statusText}`);
                    tickersToFetch = await masterResp.json();
                    tickersToFetch = tickersToFetch.map(t => typeof t === 'string' ? t.toUpperCase() : t); // Ensure uppercase
                    break;
                default: throw new Error(`Unsupported source: ${source}`);
            }

            if (!tickersToFetch || tickersToFetch.length === 0) {
                showYahooStatusAlert('No tickers found for the selected source.', 'warning');
                resetUIForNewFetch(); // Ensure UI is reset
                return; // Stop execution if no tickers
            }
            
            showYahooStatusAlert(`Triggering Yahoo mass fetch for ${tickersToFetch.length} tickers...`, 'info');
            // Update UI to show queued state immediately, before API call
            updateUIFromJobData({
                job_id: fixedJobId,
                status: 'queued',
                message: `Triggering Yahoo mass fetch for ${tickersToFetch.length} tickers...`,
                total_count: tickersToFetch.length, // For progress bar context
                current_count: 0,
                progress_percent: 0
            });

            const response = await fetch('/api/analytics/yahoo-job/trigger', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ source: source, tickers: tickersToFetch })
            });
            const triggerResultJobDetails = await response.json(); // Parse the response from /trigger

            if (!response.ok) {
                const errorMsg = triggerResultJobDetails.detail || `Failed to trigger job: ${response.statusText}`;
                // resetUIForNewFetch(); // Done by catch block or initial call
                showYahooStatusAlert( typeof errorMsg === 'string' ? errorMsg : JSON.stringify(errorMsg), 'error');
                // throw new Error(JSON.stringify(errorMsg)); // Let error handling below manage UI reset
                updateUIFromJobData({ job_id: fixedJobId, status: 'failed', message: typeof errorMsg === 'string' ? errorMsg : JSON.stringify(errorMsg) });
                return; // Stop if trigger fails
            }
            
            // console.log('[Job Triggered] Received initial job details from /trigger POST:', triggerResultJobDetails);
            if (triggerResultJobDetails && triggerResultJobDetails.last_triggered_time) {
                activeYahooJobTriggerTime = triggerResultJobDetails.last_triggered_time;
            } else {
                console.warn("[handleFetch] Trigger API success but no last_triggered_time in response. Polling filtering might be affected.");
                activeYahooJobTriggerTime = null; // Ensure it's reset if not present
            }
            updateUIFromJobData(triggerResultJobDetails); 

            if (triggerResultJobDetails.job_id && isFetching) { 
                // isFetching should be true if triggerResultJobDetails.status is queued/running
                // console.log(`[handleFetch @ ${new Date().toISOString()}] Trigger successful for ${triggerResultJobDetails.job_id}. Initiating polling.`);
                if (jobPollIntervalId) clearInterval(jobPollIntervalId); // Clear existing before starting new
                jobPollIntervalId = setInterval(() => pollJobStatus(fixedJobId), 3000);
            } else {
                // console.warn("[handleFetch] No job_id in triggerResultJobDetails or job not active. Cannot start polling.");
                if (jobPollIntervalId) { // Clear interval if job isn't active post-trigger
                    clearInterval(jobPollIntervalId);
                    jobPollIntervalId = null;
                }
            }

            if (source === 'upload') { // No specific clearing needed here for fileInput as currentTickers is used
                // if (fileInput) fileInput.value = ''; // This was done in handleFileSelect already
            }

        } catch (error) {
            console.error('[Handle Fetch Error] Error during fetch operation:', error);
            // Show error using the new alert function
            const errorMessage = error.message.includes("{") ? JSON.parse(error.message).detail || error.message : error.message;
            showYahooStatusAlert(`Error: ${errorMessage}`, 'error');
            resetUIForNewFetch();
        }
    }

    // --- NEW: Polling function ---
    async function pollJobStatus(jobId) {
        const pollStartTime = new Date().toISOString();
        if (!activeYahooJobTriggerTime) {
            // console.log(`[pollJobStatus @ ${pollStartTime}] Aborting poll: activeYahooJobTriggerTime is not set. JobId: ${jobId}`);
            // If no active job time, this poll is likely for a previous, now irrelevant job, or page just loaded with idle job.
            // Clearing interval here might be too aggressive if fetchCurrentJobStatus is meant to start it.
            // Let isFetching guard in updateUI handle interval clearing.
                 return;
        }

        // console.log(`[pollJobStatus START @ ${pollStartTime}] Polling for job ${jobId}. Active Trigger Time: ${activeYahooJobTriggerTime}. Making fetch call.`);

        try {
            const response = await fetch(`/api/analytics/yahoo-job/details/${jobId}`);
            const receivedDetailsTime = new Date().toISOString();
            if (!response.ok) {
                let errorMsg = `Polling failed: ${response.statusText}`;
                try {
                    const errorData = await response.json();
                    errorMsg = errorData.detail || errorMsg;
                } catch (e) { /* Ignore parsing error, use original statusText */ }
                showYahooStatusAlert(errorMsg, 'error');
                // updateUIFromJobData will set isFetching to false if error implies terminal state
                updateUIFromJobData({ job_id: jobId, status: 'error', message: errorMsg, last_triggered_time: activeYahooJobTriggerTime });
                return; 
            }
            const details = await response.json();
            // console.log(`[pollJobStatus @ ${receivedDetailsTime}] Received details for ${jobId}:`, JSON.parse(JSON.stringify(details)));

            if (details.last_triggered_time !== activeYahooJobTriggerTime) {
                console.warn(`[pollJobStatus @ ${receivedDetailsTime}] Stale data received for job ${jobId}. Expected trigger time ${activeYahooJobTriggerTime}, got ${details.last_triggered_time}. IGNORING UPDATE.`);
                // If it's stale, but the job is terminal, we might still want to stop polling.
                if (details.status === 'completed' || details.status === 'failed' || details.status === 'error' || details.status === 'partial_failure') {
                     if (jobPollIntervalId) {
                        console.log(`[pollJobStatus @ ${receivedDetailsTime}] Stale data indicated terminal state. Clearing interval ${jobPollIntervalId}.`);
                        clearInterval(jobPollIntervalId);
                        jobPollIntervalId = null;
                        isFetching = false; // Ensure isFetching is also false
                    }
                }
                return;
            }
            updateUIFromJobData(details);

        } catch (error) {
            // console.error(`[pollJobStatus @ ${new Date().toISOString()}] Error in pollJobStatus for ${jobId}:`, error);
            showYahooStatusAlert(`Polling error: ${error.message}`, 'error');
            updateUIFromJobData({ job_id: jobId, status: 'error', message: `Polling error: ${error.message}`, last_triggered_time: activeYahooJobTriggerTime });
        }
    }

    async function handleFileSelect(event) {
        const file = event.target.files[0];
        // const textSpan = dropZoneTextSpan || (dropZone.querySelector('.dropzone-text') || dropZone.querySelector('.status-text'));
        // Use the module-scoped dropZoneTextSpan directly, ensured by initializeDOMElements

        if (!file) {
            if (dropZoneTextSpan) dropZoneTextSpan.innerHTML = "Drag & drop .txt file(s) or click to select"; // Use innerHTML for <br>
            // if (tickerSummaryDiv) tickerSummaryDiv.textContent = ''; // showTickerSummary handles this
            showTickerSummary([]); // Pass empty array to disable button
            currentTickers = [];
            return;
        }

        if (!file.name.toLowerCase().endsWith('.txt')) {
            showYahooStatusAlert('Invalid file: Please upload a .txt file.', 'error'); 
            if (dropZoneTextSpan) dropZoneTextSpan.innerHTML = "Drag & drop .txt file(s) or click to select";
            // if (tickerSummaryDiv) tickerSummaryDiv.textContent = '';
            showTickerSummary([]); // Pass empty array to disable button
            if (fileInput) fileInput.value = ''; // Clear the file input
            currentTickers = [];
            return;
        }

        if (dropZoneTextSpan) dropZoneTextSpan.innerHTML = `Selected file: ${file.name} <br> Processing file...`;
        // if (fetchBtn) fetchBtn.disabled = true; // showTickerSummary will manage this based on results
        // if (tickerSummaryDiv) tickerSummaryDiv.textContent = 'Processing file...'; // Handled in dropzone
        currentTickers = []; // Reset before parsing
        showTickerSummary(currentTickers); // Disable button while processing

        try {
            const text = await file.text();
            const parsedTickers = text
                .split(/[\n,;]+/)
                .map(t => t.trim().toUpperCase())
                .filter(t => t.length > 0);
            currentTickers = [...new Set(parsedTickers)];
            
            let summaryMessage = `Selected file: ${file.name}<br>`;
            if (currentTickers.length > 0) {
                const exampleCount = Math.min(5, currentTickers.length);
                const exampleTickers = currentTickers.slice(0, exampleCount).join(', ');
                summaryMessage += `Tickers found: ${currentTickers.length}. Examples: ${exampleTickers}${currentTickers.length > exampleCount ? ', ...' : ''}`;
            } else {
                summaryMessage += 'No valid tickers found.';
            }
            if (dropZoneTextSpan) dropZoneTextSpan.innerHTML = summaryMessage;
            showTickerSummary(currentTickers); // Update button state based on results

        } catch (e) {
            console.error("Error reading or parsing file:", e);
            showYahooStatusAlert(`Error processing file: ${e.message}`, 'error'); 
            if (dropZoneTextSpan) dropZoneTextSpan.innerHTML = `Selected file: ${file.name}<br>File processing error.`;
            // if (tickerSummaryDiv) tickerSummaryDiv.textContent = 'File processing error.';
            currentTickers = [];
            showTickerSummary(currentTickers); // Ensure button is disabled on error
        }
        if (fileInput) fileInput.value = ''; 
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

        dropZone.addEventListener('dragover', e => { 
            e.preventDefault(); 
            dropZone.classList.add('dragover'); 
            // dropZone.style.backgroundColor = 'var(--bs-primary-bg-subtle)'; // Replaced by class
        });
        dropZone.addEventListener('dragleave', e => { 
            e.preventDefault(); 
            dropZone.classList.remove('dragover');
            // dropZone.style.backgroundColor = 'var(--bs-tertiary-bg)'; // Replaced by class
        });
        dropZone.addEventListener('drop', e => {
            e.preventDefault();
            dropZone.classList.remove('dragover');
            // dropZone.style.backgroundColor = 'var(--bs-tertiary-bg)'; // Replaced by class
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