// Finviz Mass Fetch UI Interaction Script

// --- Module-level variables and constants ---
let isFetchingFinviz = false;
let finvizEventSource = null;
const FINVIZ_JOB_API_BASE = '/api/v3/jobs/finviz';
let selectedFinvizFile = null; // To store the selected file for upload
let parsedFinvizTickers = []; // NEW: To store tickers parsed from the uploaded file

// --- UI Element Variables (will be assigned in initializeFinvizMassFetchUI) ---
let fetchBtnFinviz = null;
let spinnerFinviz = null;
let progressBarContainerFinviz = null;
let progressBarFinviz = null;
let statusDivFinviz = null;
let dataSourceSelectFinviz = null;
let finvizMfDropzone = null; // Renamed and will be used
let finvizMfFile = null;     // Renamed and will be used
let finvizSourceHelpText = null; // For the help text below dropdown

// NEW: Store the last_triggered_time of the job instance the UI is currently tracking
let activeFinvizJobTriggerTime = null;

// --- NEW HELPER FUNCTIONS (similar to Yahoo's) ---
function formatDisplayTimestamp(dateInput) {
    if (!dateInput) return new Date().toLocaleTimeString(); // Fallback
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

// --- Helper Function to Show Status Messages (Finviz specific) ---
function showFinvizStatus(message, type = 'info', timestampSource = null, jobId = null) {
    // Ensure statusDivFinviz is available (it's assigned in initialize function)
    if (!statusDivFinviz) {
        console.warn("[showFinvizStatus] statusDivFinviz not initialized yet.");
        return;
    }
    let alertClass = 'alert-info';
    if (type === 'success') alertClass = 'alert-success';
    if (type === 'error') alertClass = 'alert-danger';
    if (type === 'warning') alertClass = 'alert-warning';

    const displayTimestamp = formatDisplayTimestamp(timestampSource || new Date());
    const icon = getIconForAlertType(type);

    // Create the alert div element to attach listener later
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert ${alertClass} alert-dismissible fade show`; // Base classes
    alertDiv.setAttribute('role', 'alert');

    if (jobId) { // Only make it clickable if jobId is provided
        alertDiv.classList.add('clickable-job-summary');
        alertDiv.setAttribute('data-job-id', jobId); // Use the passed jobId
        alertDiv.style.cursor = 'pointer';
        // Add click listener (ensure handleFinvizSummaryClick is defined correctly)
        alertDiv.addEventListener('click', handleFinvizSummaryClick);
    } else {
        console.warn("[showFinvizStatus] No jobId provided for Finviz status, summary will not be clickable.");
    }

    alertDiv.innerHTML = `
        ${icon}[${displayTimestamp}] ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;

    // Clear previous status and append new one
    statusDivFinviz.innerHTML = ''; 
    statusDivFinviz.appendChild(alertDiv);

    console.log(`[Finviz Status @ ${new Date().toISOString()}] ${type.toUpperCase()}: ${message}`);
}

// --- NEW: Click handler for the summary message ---
function handleFinvizSummaryClick() {
    const jobId = this.dataset.jobId;
    if (!jobId) {
        console.error("Finviz Job ID not found on clicked summary element.");
        displayFinvizJobDetailModal("Error: Could not retrieve Job ID for Finviz detailed log.");
        return;
    }
    fetchAndShowDetailedFinvizLog(jobId);
}

// --- NEW: Fetch and display detailed log for Finviz ---
async function fetchAndShowDetailedFinvizLog(jobId) {
    displayFinvizJobDetailModal("Loading Finviz job details...", true);
    try {
        const response = await fetch(`/api/jobs/${jobId}/detailed_log`);
        if (!response.ok) {
            let errorDetail = `Failed to fetch details. Status: ${response.status}`;
            try {
                const errorData = await response.json();
                errorDetail = errorData.detail || errorDetail;
            } catch (e) { /* Ignore if parsing error response body fails */ }
            throw new Error(errorDetail);
        }
        const result = await response.json();
        if (result.detailed_log) {
            displayFinvizJobDetailModal(result.detailed_log);
        } else if (result.error) {
            displayFinvizJobDetailModal(`Error: ${result.error}`);
        } else {
            displayFinvizJobDetailModal("No detailed log available or an unexpected error occurred.");
        }
    } catch (error) {
        console.error("Failed to fetch Finviz job details:", error);
        displayFinvizJobDetailModal(`Failed to load Finviz job details: ${error.message}`);
    }
}

// --- Helper function to get theme preference from cookie ---
function getThemePreferenceCookie() {
    const name = "theme_preference=";
    const decodedCookie = decodeURIComponent(document.cookie);
    const ca = decodedCookie.split(';');
    for(let i = 0; i <ca.length; i++) {
        let c = ca[i];
        while (c.charAt(0) === ' ') {
            c = c.substring(1);
        }
        if (c.indexOf(name) === 0) {
            return c.substring(name.length, c.length);
        }
    }
    return "light"; // Default if not found or cookie is not set
}

// --- NEW: Modal display function for Finviz Job Details ---
function displayFinvizJobDetailModal(content, isLoading = false) {
    let modal = document.getElementById('finvizJobDetailModal');
    const currentTheme = getThemePreferenceCookie();

    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'finvizJobDetailModal';
        modal.style.position = 'fixed';
        modal.style.left = '50%';
        modal.style.top = '50%';
        modal.style.transform = 'translate(-50%, -50%)';
        // modal.style.backgroundColor = 'white'; // Theme dependent
        modal.style.padding = '20px';
        // modal.style.border = '1px solid #ccc'; // Theme dependent
        modal.style.boxShadow = '0 4px 8px rgba(0,0,0,0.1)';
        modal.style.zIndex = '10001';
        modal.style.minWidth = '300px';
        modal.style.maxWidth = '80%';
        modal.style.maxHeight = '70vh';
        modal.style.overflowY = 'auto';

        const contentArea = document.createElement('pre');
        contentArea.id = 'finvizJobDetailModalContent';
        contentArea.style.whiteSpace = 'pre-wrap';
        contentArea.style.wordBreak = 'break-word';
        // Text color will be theme dependent

        const closeButton = document.createElement('button');
        closeButton.textContent = 'Close';
        // Basic Bootstrap classes, will adjust further if needed based on theme
        closeButton.className = 'btn btn-sm btn-secondary mt-3'; 
        closeButton.style.display = 'block';
        closeButton.onclick = function() {
            modal.style.display = 'none';
        };

        modal.appendChild(contentArea);
        modal.appendChild(closeButton);
        document.body.appendChild(modal);
    }

    const contentArea = document.getElementById('finvizJobDetailModalContent');

    // Apply theme-specific styles
    if (currentTheme === 'dark') {
        modal.style.backgroundColor = '#2b2b2b'; // Dark background for modal
        modal.style.border = '1px solid #555';
        contentArea.style.color = '#f0f0f0'; // Light text for content area
        // For the button, Bootstrap's btn-secondary might be okay on dark, 
        // or you might need to adjust its text/background too if it looks bad.
        // Example: closeButton might need specific styling if default btn-secondary is not good.
    } else {
        modal.style.backgroundColor = 'white'; // Light background for modal
        modal.style.border = '1px solid #ccc';
        contentArea.style.color = '#333'; // Dark text for content area
    }

    if (isLoading) {
        contentArea.textContent = "Loading...";
    } else {
        contentArea.textContent = content;
    }
    modal.style.display = 'block';
}

// --- Helper to Enable/Disable Fetch Button (Finviz specific) ---
function setFinvizButtonState(enabled, buttonText = "Fetch Finviz Data") {
    if (!fetchBtnFinviz || !spinnerFinviz) { // Check spinnerFinviz as well
        console.warn("[setFinvizButtonState] fetchBtnFinviz or spinnerFinviz not initialized yet.");
        return;
    }
    fetchBtnFinviz.disabled = !enabled;
    const textSpan = fetchBtnFinviz.querySelector('span.button-text'); // More specific selector
    if (textSpan) textSpan.textContent = buttonText;
    // else fetchBtnFinviz.textContent = buttonText; // Fallback removed, assume span.button-text exists

    if (enabled) {
        spinnerFinviz.style.display = 'none'; // Use style.display as per HTML
    } else {
        // spinnerFinviz.style.display = 'inline-block'; // Or 'block' depending on desired layout
    }
}

// --- UI Update from Job Data (Finviz specific) ---
function updateFinvizUIFromJobData(data) {
    const uiUpdateEntryTime = new Date().toISOString();
    console.log(`[updateFinvizUIFromJobData ENTRY @ ${uiUpdateEntryTime}] Called with data:`, data);

    if (!fetchBtnFinviz || !progressBarFinviz || !progressBarContainerFinviz || !statusDivFinviz || !spinnerFinviz) {
        console.warn(`[updateFinvizUIFromJobData @ ${new Date().toISOString()}] Required Finviz UI elements not yet available/initialized.`);
        return;
    }
    
    // MODIFIED: Extract currentJobId to pass to showFinvizStatus
    const currentJobId = data.job_id; // Assuming job_id is always present in job data from backend

    const status = data.status || 'unknown';
    // MODIFIED: Changed timestampForAlert to use data.timestamp (as per backend structure) or fallback
    const timestampForAlert = data.timestamp || data.last_completion_time || data.last_started_time || new Date();

    // Ensure progressPercent is calculated safely
    let progressPercentCalc = 0;
    if (data.total_count && data.total_count > 0) {
        progressPercentCalc = ( (data.current_count || 0) / data.total_count) * 100;
    }
    const progressPercent = data.progress_percent !== undefined && data.progress_percent !== null ? data.progress_percent : progressPercentCalc;
    const currentCount = data.current_count || 0;
    const totalCount = data.total_count || 0; // Used for concise message and progress bar visibility check

    // Manage Progress Bar Visibility & Styling
    // Apply Bootstrap classes for consistent styling with Yahoo
    if (progressBarContainerFinviz) {
        progressBarContainerFinviz.classList.add('progress', 'mt-2');
        progressBarContainerFinviz.style.height = '20px';
    }
    if (progressBarFinviz) {
        progressBarFinviz.classList.add('progress-bar', 'progress-bar-striped', 'progress-bar-animated');
    }

    if (status === 'running' || status === 'queued') {
        if (progressBarContainerFinviz) progressBarContainerFinviz.style.display = 'flex'; // Use flex
    } else { // idle, failed, completed, partial_failure, interrupted
        if (progressBarContainerFinviz) progressBarContainerFinviz.style.display = 'none';
    }

    // Update Progress Bar Visuals
    const saneProgressPercent = Math.min(100, Math.max(0, isNaN(progressPercent) ? 0 : progressPercent));
    if (progressBarFinviz) {
        progressBarFinviz.style.width = `${saneProgressPercent}%`;
        progressBarFinviz.textContent = `${Math.round(saneProgressPercent)}%`;
        progressBarFinviz.setAttribute('aria-valuenow', saneProgressPercent);

        // Conditional coloring similar to Yahoo
        progressBarFinviz.classList.remove('bg-primary', 'bg-danger', 'bg-warning'); // Clear existing
        if (status === 'partial_failure') {
            progressBarFinviz.classList.add('bg-warning');
        } else if (status === 'failed' || status === 'error') { // Assuming 'error' status might come
            progressBarFinviz.classList.add('bg-danger');
        } else if (status === 'completed') {
            // No specific color for completed, or use 'bg-success' if preferred
            // progressBarFinviz.classList.add('bg-success');
        } else {
            // progressBarFinviz.classList.add('bg-primary'); // Default Bootstrap blue often requires no class
        }
    }
    
    // Construct and Display Status Message
    let composedMessage = '';
    let messageType = 'info';

    if (status === 'completed' || status === 'failed' || status === 'error' || status === 'partial_failure' || status === 'interrupted') {
        messageType = (status === 'completed') ? 'success' : 'error';
        if (status === 'partial_failure' || status === 'interrupted') messageType = 'warning';

        let statusText = `Status: ${status}.`;
        let countsText = '';
        
        const tickersDisplayCount = data.total_count !== undefined ? data.total_count : 0;
        countsText += ` Tickers: ${tickersDisplayCount}.`;
        
        const errorDisplayCount = data.failed_count !== undefined ? data.failed_count : 0;
        countsText += ` Errors: ${errorDisplayCount}.`;

        composedMessage = statusText + countsText;

        if (errorDisplayCount > 0 && data.sample_error_tickers && data.sample_error_tickers.length > 0) {
            const MAX_SAMPLE_ERRORS = 5;
            const sampleTickers = data.sample_error_tickers.slice(0, MAX_SAMPLE_ERRORS).join(', ');
            composedMessage += ` Errored tickers sample: ${sampleTickers}${data.sample_error_tickers.length > MAX_SAMPLE_ERRORS ? ', ...' : ''}.`;
        }
        // Ensure data.message or data.last_run_summary are NOT appended for terminal states to keep it concise.

    } else if (status === 'running' && data.progress_message) {
        composedMessage = data.progress_message;
        messageType = 'info';
    } else if (data.message) { // General message for other states (e.g. queued)
        composedMessage = data.message;
        messageType = 'info';
    } else {
        composedMessage = `Status: ${status}.`; // Default message
        messageType = 'info';
    }
    // MODIFIED: Pass currentJobId to showFinvizStatus
    showFinvizStatus(composedMessage, messageType, timestampForAlert, currentJobId);

    // Manage Fetch Button State and isFetchingFinviz flag
    if (status === 'running' || status === 'queued') {
        isFetchingFinviz = true;
        setFinvizButtonState(false, "Fetching...");
        spinnerFinviz.style.display = 'inline-block';
    } else {
        isFetchingFinviz = false;
        setFinvizButtonState(true, "Fetch Finviz Data");
        spinnerFinviz.style.display = 'none';
        if (finvizEventSource) {
            finvizEventSource.close();
            finvizEventSource = null;
            console.log("[Finviz SSE] EventSource closed due to job completion/failure.");
        }
    }
    console.log(`[updateFinvizUIFromJobData EXIT @ ${new Date().toISOString()}] isFetchingFinviz: ${isFetchingFinviz}`);
}

// --- Connect to SSE for Finviz Job Updates ---
function connectFinvizSSE() {
    if (finvizEventSource && finvizEventSource.readyState !== EventSource.CLOSED) {
        console.log("[Finviz SSE] Closing existing EventSource connection.");
        finvizEventSource.close();
    }
    finvizEventSource = new EventSource(`${FINVIZ_JOB_API_BASE}/sse`);
    console.log("[Finviz SSE] Attempting to connect to SSE stream...");

    finvizEventSource.onopen = () => {
        console.log("[Finviz SSE] Connection opened.");
        // showFinvizStatus("Connected to Finviz job status stream.", "info"); // Message can be verbose
    };

    finvizEventSource.onmessage = function(event) {
        console.log('[Finviz SSE ONMESSAGE] Event received. Raw event.data:', event.data);

        if (!event.data) {
            console.warn('[Finviz SSE ONMESSAGE] Received event with no data. Skipping.');
            return;
        }
        if (!event.data.startsWith('{')) {
            console.log('[Finviz SSE ONMESSAGE] Received non-JSON event (likely heartbeat):', event.data);
            return;
        }

        let jobData;
        try {
            console.log('[Finviz SSE ONMESSAGE] Attempting to parse event.data...');
            jobData = JSON.parse(event.data);
            console.log('[Finviz SSE ONMESSAGE] Successfully parsed jobData:', jobData);
        } catch (e) {
            console.error('[Finviz SSE ONMESSAGE] Error parsing event.data JSON:', e, 'Raw data:', event.data);
            return;
        }

        // MODIFIED: Filter messages by job_id, job_type, and activeFinvizJobTriggerTime
        if (jobData &&
            jobData.job_id === "finviz_mass_fetch_main_v1" &&
            jobData.job_type === "finviz_mass_fetch" &&
            jobData.last_triggered_time === activeFinvizJobTriggerTime) {
            updateFinvizUIFromJobData(jobData);
        } else {
            console.warn('[Finviz SSE ONMESSAGE] Received data for irrelevant job or mismatched trigger time. IGNORING.', {
                expectedJobId: "finviz_mass_fetch_main_v1",
                expectedJobType: "finviz_mass_fetch",
                expectedTriggerTime: activeFinvizJobTriggerTime,
                receivedJobData: jobData
            });
        }
    };

    finvizEventSource.onerror = (error) => {
        console.error("[Finviz SSE] EventSource failed:", error);
        showFinvizStatus("Lost connection to Finviz job status stream. Will attempt to reconnect or fetch details.", "error");
        if (finvizEventSource) finvizEventSource.close();
        finvizEventSource = null;
        if (isFetchingFinviz) {
            setTimeout(getFinvizJobDetails, 3000);
        }
    };
}

// --- Get Current Finviz Job Details ---
async function getFinvizJobDetails() {
    console.log("[getFinvizJobDetails] Fetching current Finviz job details...");
    try {
        const response = await fetch(`${FINVIZ_JOB_API_BASE}/details`);
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Failed to fetch Finviz job details: ${response.status} ${errorText}`);
        }
        const data = await response.json();
        console.log("[getFinvizJobDetails] Received job details:", data);

        if (data && data.last_triggered_time) {
            activeFinvizJobTriggerTime = data.last_triggered_time; // Set/update based on fetched details
            console.log(`[getFinvizJobDetails] activeFinvizJobTriggerTime set to: ${activeFinvizJobTriggerTime}`);
        } else {
            console.warn("[getFinvizJobDetails] Fetched details do not have last_triggered_time. activeFinvizJobTriggerTime not updated.");
        }
        
        updateFinvizUIFromJobData(data);

        // If the job is active, try to connect to SSE
        if (isFetchingFinviz && data.status && !["completed", "failed", "partial_failure", "cancelled"].includes(data.status)) {
            console.log("[getFinvizJobDetails] Job is active, connecting to SSE.");
            connectFinvizSSE();
        } else {
            console.log("[getFinvizJobDetails] Job is not active or in a terminal state. Not connecting to SSE.");
        }
        return data;
    } catch (error) {
        console.error("[getFinvizJobDetails] Exception during fetch:", error);
        showFinvizStatus("Exception fetching job status.", "error");
        // activeFinvizJobTriggerTime = null; // Consider if reset is needed on exception
        return null; // Return null on exception
    }
}

// --- Handle Finviz Data Source Change ---
function handleFinvizDataSourceChange() {
    if (!dataSourceSelectFinviz || !finvizMfDropzone || !fetchBtnFinviz || !finvizSourceHelpText) {
        console.warn("[handleFinvizDataSourceChange] UI elements not ready.");
        return;
    }
    selectedFinvizFile = null; // Reset selected file on source change
    parsedFinvizTickers = []; // Reset parsed tickers
    const dropZoneText = finvizMfDropzone.querySelector('span.status-text'); // Cache selector

    const selectedSource = dataSourceSelectFinviz.value;
    if (selectedSource === 'upload_finviz_txt') {
        finvizMfDropzone.style.display = 'block'; // Or 'flex' if that's its default active display style
        finvizMfDropzone.style.opacity = '1';
        finvizMfDropzone.style.pointerEvents = 'auto';
        finvizMfDropzone.style.backgroundColor = 'var(--bs-tertiary-bg)'; // Assuming this is the active background
        finvizSourceHelpText.textContent = "Upload a .txt file (one ticker per line).";
        if (dropZoneText) dropZoneText.textContent = "Drag & drop .txt file or click";
        setFinvizButtonState(false); // Disable button until file is selected
    } else { // finviz_screener
        finvizMfDropzone.style.display = 'block'; // Ensure it's visible to apply opacity etc.
        finvizMfDropzone.style.opacity = '0.6';
        finvizMfDropzone.style.pointerEvents = 'none';
        finvizMfDropzone.style.backgroundColor = 'var(--bs-secondary-bg)'; // Visually disabled background
        finvizSourceHelpText.textContent = "Fetches all data from the main Finviz source.";
        if (dropZoneText) dropZoneText.textContent = "File upload not applicable for this source.";
        setFinvizButtonState(true); // Enable button as no file is needed
    }
}

// --- Handle File Selection (for Finviz) ---
async function handleFinvizFileSelect(file) { // Made async to handle await file.text()
    const dropZoneText = finvizMfDropzone ? finvizMfDropzone.querySelector('span.status-text') : null;
    selectedFinvizFile = null; // Reset first
    parsedFinvizTickers = [];
    setFinvizButtonState(false); // Disable button initially

    if (!dropZoneText) {
        console.warn("[handleFinvizFileSelect] Dropzone text element not found.");
        return; // Should not happen if HTML is correct
    }

    if (!file) {
        dropZoneText.innerHTML = "Drag & drop .txt file or click";
        // setFinvizButtonState(false); // Already set
        return;
    }

    if (!file.name.endsWith('.txt')) {
        showFinvizStatus("Invalid file type. Please upload a .txt file.", "error");
        dropZoneText.innerHTML = "Drag & drop .txt file or click";
        // setFinvizButtonState(false); // Already set
        if (finvizMfFile) finvizMfFile.value = ''; // Clear the file input
        return;
    }

    selectedFinvizFile = file; // Store the File object itself
    dropZoneText.innerHTML = `Selected file: ${file.name}<br>Processing file...`;

    try {
        const fileContent = await file.text();
        const tickers = fileContent
            .split('\n')
            .map(t => t.trim().toUpperCase())
            .filter(t => t.length > 0);
        parsedFinvizTickers = [...new Set(tickers)]; // Store unique tickers

        let summaryMessage = `Selected file: ${file.name}<br>`;
        if (parsedFinvizTickers.length > 0) {
            const exampleCount = Math.min(5, parsedFinvizTickers.length);
            const exampleTickers = parsedFinvizTickers.slice(0, exampleCount).join(', ');
            summaryMessage += `Tickers found: ${parsedFinvizTickers.length}. Examples: ${exampleTickers}${parsedFinvizTickers.length > exampleCount ? ', ...' : ''}`;
            setFinvizButtonState(true); // Enable button if tickers found
        } else {
            summaryMessage += 'No valid tickers found.';
            // setFinvizButtonState(false); // Already disabled or kept disabled
        }
        dropZoneText.innerHTML = summaryMessage;

    } catch (error) {
        console.error("[handleFinvizFileSelect] Error reading file:", error);
        showFinvizStatus(`Error reading file: ${error.message}`, "error");
        dropZoneText.innerHTML = `Selected file: ${file.name}<br>Error reading file.`;
        parsedFinvizTickers = [];
        // setFinvizButtonState(false); // Ensure button remains disabled
    }
    if (finvizMfFile) finvizMfFile.value = ''; // Clear the file input after processing
}

// --- Handle Finviz Fetch Button Click ---
async function handleFinvizFetch() {
    if (!dataSourceSelectFinviz || !fetchBtnFinviz || !spinnerFinviz){
        console.error("[handleFinvizFetch] Core UI elements (select, button, spinner) are null. UI not properly initialized.");
        showFinvizStatus("UI Error: Cannot initiate fetch.", "error");
        return;
    }
    if (isFetchingFinviz) {
        showFinvizStatus("A Finviz fetch operation is already in progress.", "warning");
        return;
    }
    
    const selectedSource = dataSourceSelectFinviz.value;
    // let tickersFromFile = []; // Replaced by parsedFinvizTickers

    if (selectedSource === 'upload_finviz_txt') {
        // File is now parsed in handleFinvizFileSelect. Use parsedFinvizTickers.
        if (!parsedFinvizTickers || parsedFinvizTickers.length === 0) {
            showFinvizStatus("No tickers loaded from file, or file not yet processed. Please select a valid TXT file.", "warning");
            // Ensure button is in correct state if somehow submission happens without tickers
            setFinvizButtonState(!!(parsedFinvizTickers && parsedFinvizTickers.length > 0)); 
            return;
        }
        console.log("[handleFinvizFetch] Using parsed tickers:", parsedFinvizTickers);
        // No "Reading file..." state needed here as file is pre-processed
    }

    console.log("[handleFinvizFetch] Clicked. Source:", selectedSource, "Using Parsed File Tickers:", parsedFinvizTickers.length);
    isFetchingFinviz = true;
    setFinvizButtonState(false, "Starting...");
    spinnerFinviz.style.display = 'inline-block';
    showFinvizStatus("Initiating Finviz data fetch...", "info");
    
    if(progressBarContainerFinviz) progressBarContainerFinviz.style.display = 'block';
    if(progressBarFinviz) {
        progressBarFinviz.style.width = '0%';
        progressBarFinviz.textContent = '0%';
        progressBarFinviz.setAttribute('aria-valuenow', 0);
    }

    let apiUrl = `${FINVIZ_JOB_API_BASE}/trigger`;
    const bodyPayload = {
        source: selectedSource
    };

    if (selectedSource === 'finviz_screener') {
        apiUrl += '?source_type=finviz_screener';
        bodyPayload.tickers = ['_DUMMY_FOR_VALIDATION_']; // Satisfy TickerListPayload min_items=1
    } else if (selectedSource === 'upload_finviz_txt') {
        apiUrl += '?source_type=upload_finviz_txt';
        bodyPayload.tickers = parsedFinvizTickers; // Use the pre-parsed tickers
    } else {
        // Fallback or error for unknown source if necessary
        showFinvizStatus(`Unknown data source selected: ${selectedSource}`, "error");
        isFetchingFinviz = false;
        setFinvizButtonState(true);
        if(spinnerFinviz) spinnerFinviz.style.display = 'none';
        return;
    }

    try {
        const csrfTokenElement = document.querySelector('meta[name="csrf-token"]');
        const csrfToken = csrfTokenElement ? csrfTokenElement.getAttribute('content') : null;
        if (!csrfToken && selectedSource === 'upload_finviz_txt') { // CSRF might be more critical for uploads if enforced
             console.warn("[handleFinvizFetch] CSRF token not found. POST request for file upload might fail if CSRF is enforced.");
             // Depending on server config, might not be an issue for GET or simple POSTs without file data.
        }

        const response = await fetch(apiUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...(csrfToken && { 'X-CSRFToken': csrfToken })
            },
            body: JSON.stringify(bodyPayload)
        });

        const responseData = await response.json();
        if (!response.ok) {
            const errorMsg = responseData.detail || `HTTP error ${response.status}`;
            console.error("[handleFinvizFetch] Trigger API error (raw detail):", errorMsg, "Full response data:", responseData);
            throw new Error(JSON.stringify(errorMsg));
        }

        console.log("[handleFinvizFetch] Trigger API success:", responseData);

        if (responseData && responseData.last_triggered_time) {
            activeFinvizJobTriggerTime = responseData.last_triggered_time; // CRITICAL: Set for the new job
            console.log(`[handleFinvizFetch] activeFinvizJobTriggerTime SET to: ${activeFinvizJobTriggerTime}`);
        } else {
            console.error("[handleFinvizFetch] Trigger API success but no last_triggered_time in response. SSE filtering might fail.", responseData);
            // Set a fallback or handle this error case, perhaps by not connecting to SSE or showing an error.
            // For now, we'll let updateFinvizUIFromJobData handle the UI based on the (possibly incomplete) responseData.
        }

        updateFinvizUIFromJobData(responseData);

        if (isFetchingFinviz) { // Check if the job status from trigger is not terminal
            console.log("[handleFinvizFetch] Job triggered and is active, connecting to SSE.");
            connectFinvizSSE();
        } else {
            console.log("[handleFinvizFetch] Job triggered but is already in a terminal state. Not connecting to SSE.");
        }

    } catch (error) {
        console.error("[handleFinvizFetch] Error during Finviz fetch operation (error object):", error);
        const displayError = error.message && error.message.includes("{") ? error.message : "Network or unexpected error during Finviz fetch.";
        showFinvizStatus(`Error starting Finviz fetch: ${displayError}`, "error");
        setFinvizButtonState(true); // Re-enable button
        isFetchingFinviz = false;
        if(progressBarContainerFinviz) progressBarContainerFinviz.style.display = 'none';
        // activeFinvizJobTriggerTime = null; // Reset on failure
    }
}

// --- Main Initialization Function (to be called by other scripts) ---
function initializeFinvizMassFetchUI() {
    console.log("[finviz_mass_fetch.js] initializeFinvizMassFetchUI() called.");

    // --- Assign Finviz UI Elements (scoped to this module now) ---
    fetchBtnFinviz = document.getElementById('fetch-finviz-data-btn');
    spinnerFinviz = document.getElementById('finviz-fetch-spinner');
    progressBarContainerFinviz = document.getElementById('finviz-progress-bar-container');
    progressBarFinviz = document.getElementById('finviz-progress-bar');
    statusDivFinviz = document.getElementById('finviz-status-div');
    dataSourceSelectFinviz = document.getElementById('finviz-data-source-select');
    finvizMfDropzone = document.getElementById('finviz-mf-dropzone'); // Assign new ID
    finvizMfFile = document.getElementById('finviz-mf-file');         // Assign new ID
    finvizSourceHelpText = document.getElementById('finviz-source-help-text');

    if (!fetchBtnFinviz) {
        console.warn("[finviz_mass_fetch.js] Finviz fetch button ('fetch-finviz-data-btn') not found during initialization.");
    } else {
        fetchBtnFinviz.addEventListener('click', handleFinvizFetch);
    }

    if (!dataSourceSelectFinviz) {
        console.warn("[finviz_mass_fetch.js] Finviz data source select ('finviz-data-source-select') not found.");
    } else {
        dataSourceSelectFinviz.addEventListener('change', handleFinvizDataSourceChange);
        // Initial call to set up UI based on default dropdown value
        handleFinvizDataSourceChange();
    }

    if (!finvizMfDropzone) {
        console.warn("[finviz_mass_fetch.js] Finviz drop zone ('finviz-mf-dropzone') not found.");
    } else {
        finvizMfDropzone.addEventListener('click', () => {
            if (finvizMfFile) finvizMfFile.click();
        });
        finvizMfDropzone.addEventListener('dragover', (event) => {
            event.preventDefault();
            finvizMfDropzone.classList.add('dragover');
        });
        finvizMfDropzone.addEventListener('dragleave', () => {
            finvizMfDropzone.classList.remove('dragover');
        });
        finvizMfDropzone.addEventListener('drop', (event) => {
            event.preventDefault();
            finvizMfDropzone.classList.remove('dragover');
            if (event.dataTransfer.files.length > 0) {
                handleFinvizFileSelect(event.dataTransfer.files[0]);
            }
        });
    }

    if (!finvizMfFile) {
        console.warn("[finviz_mass_fetch.js] Finviz file input ('finviz-mf-file') not found.");
    } else {
        finvizMfFile.addEventListener('change', (event) => {
            if (event.target.files.length > 0) {
                handleFinvizFileSelect(event.target.files[0]);
            } else {
                handleFinvizFileSelect(null); // No file selected
            }
        });
    }

    if (!statusDivFinviz) {
        console.warn("[finviz_mass_fetch.js] Finviz status div ('finviz-status-div') not found, skipping initial job status check.");
    } else {
        getFinvizJobDetails(); // Fetch initial job status on load
    }
     if (!spinnerFinviz) {
        console.warn("[finviz_mass_fetch.js] Finviz spinner ('finviz-fetch-spinner') not found.");
    } else {
        spinnerFinviz.style.display = 'none'; // Ensure spinner is hidden initially
    }

    console.log("[finviz_mass_fetch.js] Script initialized via initializeFinvizMassFetchUI.");
}

// Expose the initialization function to be called from analytics.js or similar
window.FinvizMassFetchModule = {
    initialize: initializeFinvizMassFetchUI,
    // Potentially expose other functions if needed by other modules
};

// Fallback for CSRF token if not using a meta tag
let csrfToken = null; 
if (typeof globalCsrfToken !== 'undefined') { // Check for a globally defined CSRF token
    csrfToken = globalCsrfToken;
    console.log("[finviz_mass_fetch.js] Using global CSRF token.");
} else {
    const csrfMetaTag = document.querySelector('meta[name="csrf-token"]');
    if (csrfMetaTag) {
        csrfToken = csrfMetaTag.getAttribute('content');
        console.log("[finviz_mass_fetch.js] CSRF token found in meta tag.");
    } else {
        console.warn("[finviz_mass_fetch.js] CSRF token meta tag or global not found. POST requests might fail if CSRF is enforced.");
    }
}
console.log("[finviz_mass_fetch.js] Script loaded."); 