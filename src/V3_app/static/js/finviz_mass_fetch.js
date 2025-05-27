// Finviz Mass Fetch UI Interaction Script

// --- Module-level variables and constants ---
let isFetchingFinviz = false;
let finvizEventSource = null;
const FINVIZ_JOB_API_BASE = '/api/v3/jobs/finviz';
let selectedFinvizFile = null; // To store the selected file for upload

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

// --- Helper Function to Show Status Messages (Finviz specific) ---
function showFinvizStatus(message, type = 'info') {
    // Ensure statusDivFinviz is available (it's assigned in initialize function)
    if (!statusDivFinviz) {
        console.warn("[showFinvizStatus] statusDivFinviz not initialized yet.");
        return;
    }
    let alertClass = 'alert-info';
    if (type === 'success') alertClass = 'alert-success';
    if (type === 'error') alertClass = 'alert-danger';
    if (type === 'warning') alertClass = 'alert-warning';

    const timestamp = new Date().toLocaleTimeString();
    statusDivFinviz.innerHTML =
        `<div class="alert ${alertClass} alert-dismissible fade show" role="alert">
            [${timestamp}] ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        </div>`;
    console.log(`[Finviz Status @ ${new Date().toISOString()}] ${type.toUpperCase()}: ${message}`);
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

    const status = data.status || 'unknown';
    const message = data.progress_message || data.message || data.last_run_summary || 'No message.';
    // Ensure progressPercent is calculated safely
    let progressPercentCalc = 0;
    if (data.total_count && data.total_count > 0) {
        progressPercentCalc = ( (data.current_count || 0) / data.total_count) * 100;
    }
    const progressPercent = data.progress_percent !== undefined && data.progress_percent !== null ? data.progress_percent : progressPercentCalc;
    const currentCount = data.current_count || 0;
    const totalCount = data.total_count || 0;

    // Show/Hide Progress Bar
    if (status === 'running' || status === 'queued' || status === 'partial_failure' || (status === 'completed' && totalCount > 0)) {
        progressBarContainerFinviz.style.display = 'block';
    } else if (status === 'idle' || status === 'failed' || (status === 'completed' && totalCount === 0)) {
        progressBarContainerFinviz.style.display = 'none';
    }

    // Update Progress Bar
    const saneProgressPercent = Math.min(100, Math.max(0, isNaN(progressPercent) ? 0 : progressPercent));
    progressBarFinviz.style.width = `${saneProgressPercent}%`;
    progressBarFinviz.textContent = `${Math.round(saneProgressPercent)}%`;
    progressBarFinviz.setAttribute('aria-valuenow', saneProgressPercent);

    // Display Status Message
    let messageType = 'info';
    if (status === 'completed') messageType = 'success';
    else if (status === 'failed' || status === 'partial_failure') messageType = 'error';
    else if (status === 'interrupted') messageType = 'warning';
    showFinvizStatus(message, messageType);

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

    const selectedSource = dataSourceSelectFinviz.value;
    if (selectedSource === 'upload_finviz_txt') {
        finvizMfDropzone.style.display = 'block';
        finvizSourceHelpText.textContent = "Upload a .txt file (one ticker per line).";
        const dropZoneText = finvizMfDropzone.querySelector('span.status-text');
        if (dropZoneText) dropZoneText.textContent = "Drag & drop .txt file or click";
        setFinvizButtonState(false); // Disable button until file is selected
    } else { // finviz_screener
        finvizMfDropzone.style.display = 'none';
        finvizSourceHelpText.textContent = "Fetches all data from the main Finviz source.";
        setFinvizButtonState(true); // Enable button as no file is needed
    }
}

// --- Handle File Selection (for Finviz) ---
function handleFinvizFileSelect(file) {
    if (!file) {
        selectedFinvizFile = null;
        if(finvizMfDropzone) {
            const dropZoneText = finvizMfDropzone.querySelector('span.status-text');
            if (dropZoneText) dropZoneText.textContent = "Drag & drop .txt file or click";
        }
        setFinvizButtonState(false);
        return;
    }
    if (!file.name.endsWith('.txt')) {
        showFinvizStatus("Invalid file type. Please upload a .txt file.", "error");
        selectedFinvizFile = null;
        if(finvizMfDropzone) {
            const dropZoneText = finvizMfDropzone.querySelector('span.status-text');
            if (dropZoneText) dropZoneText.textContent = "Drag & drop .txt file or click";
        }
        setFinvizButtonState(false);
        return;
    }
    selectedFinvizFile = file;
    if(finvizMfDropzone) {
        const dropZoneText = finvizMfDropzone.querySelector('span.status-text');
        if (dropZoneText) dropZoneText.textContent = `File selected: ${file.name}`;
    }
    setFinvizButtonState(true);
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
    let tickersFromFile = [];

    if (selectedSource === 'upload_finviz_txt') {
        if (!selectedFinvizFile) {
            showFinvizStatus("Please select a TXT file to upload.", "warning");
            return;
        }
        // Start file reading process
        setFinvizButtonState(false, "Reading file...");
        spinnerFinviz.style.display = 'inline-block';
        try {
            const fileContent = await selectedFinvizFile.text();
            tickersFromFile = fileContent.split('\n').map(t => t.trim()).filter(t => t.length > 0);
            if (tickersFromFile.length === 0) {
                showFinvizStatus("The selected file is empty or contains no valid tickers.", "warning");
                setFinvizButtonState(true, "Fetch Finviz Data"); // Re-enable button
                spinnerFinviz.style.display = 'none';
                isFetchingFinviz = false; // Reset fetching state
                return;
            }
            console.log("[handleFinvizFetch] Tickers from file:", tickersFromFile);
        } catch (error) {
            console.error("[handleFinvizFetch] Error reading file:", error);
            showFinvizStatus(`Error reading file: ${error.message}`, "error");
            setFinvizButtonState(true, "Fetch Finviz Data"); // Re-enable button
            spinnerFinviz.style.display = 'none';
            isFetchingFinviz = false; // Reset fetching state
            return;
        }
    }

    console.log("[handleFinvizFetch] Clicked. Source:", selectedSource, "File Tickers:", tickersFromFile.length);
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
        bodyPayload.tickers = tickersFromFile;
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