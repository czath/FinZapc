document.addEventListener('DOMContentLoaded', function() { // No longer needs to be async
    console.log("Analytics.js: DOMContentLoaded event fired. Script execution started."); // Keep script load log
    // --- Global variables ---
    let fullProcessedData = [];
    let currentFilters = [];
    // let fieldWeights = {};      // {fieldName: weight (0-100)} // REMOVED
    let availableFields = [];
    let fieldMetadata = {};
    let fieldEnabledStatus = {}; // {fieldName: true/false}
    let fieldNumericFormats = {}; // <<< ADDED: {fieldName: format ('default', 'percent', 'million', 'billion', 'integer')}
    let uploadedTickers = []; // To store tickers from uploaded file
    let currentSortKey = 'name'; // Default sort
    let currentSortDirection = 'asc';
    let outputDataTable = null; // <<< ADDED: For DataTable instance
    let lastAppliedHeaders = []; // <<< ADDED: To track changes for DataTable re-init
    let filteredDataForChart = []; // Holds the data currently used by the chart (before transformation)
    let finalDataForAnalysis = []; // Holds the data AFTER transformations, used by Analyze tab
    // <<< NEW: Declare post-transform state variables globally >>>
    let finalAvailableFields = [];
    let finalFieldMetadata = {};
    // <<< END NEW >>>
    // <<< ADDED: To store previous chart selections >>>
    let previousXValue = null;
    let previousYValue = null;
    let previousSizeValue = null;

    const FILTER_STORAGE_KEY = 'analyticsAnalyticsFilters';
    // const WEIGHT_STORAGE_KEY = 'analyticsAnalyticsFieldWeights'; // REMOVED
    const FIELD_ENABLED_STORAGE_KEY = 'analyticsAnalyticsFieldEnabled'; // New key
    const FIELD_NUMERIC_FORMAT_STORAGE_KEY = 'analyticsNumericFieldFormats'; // <<< ADDED
    const FIELD_INFO_TIPS_STORAGE_KEY = 'analyticsFieldInfoTips'; // New storage key
    let fieldInfoTips = {}; // New global variable for in-memory storage
    const TEXT_FILTER_DROPDOWN_THRESHOLD = 30; // <<< ADD THIS CONSTANT

    // --- Element References ---
    // Import Tab
    const finvizButton = document.getElementById('run-finviz-btn');
    // const finvizStatus = document.getElementById('finviz-status'); // REMOVE COMMENT, will be moved lower
    const dropZone = document.getElementById('drop-zone');
    const tickerFileInput = document.getElementById('ticker-file-input');
    const runFinvizUploadBtn = document.getElementById('run-finviz-upload-btn');
    // const finvizUploadStatus = document.getElementById('finviz-upload-status'); // REMOVE COMMENT, will be moved lower

    // Preparation Tab
    const processButton = document.getElementById('process-analytics-data-btn');
    // const processStatus = document.getElementById('process-analytics-status'); // <<< MOVE DECLARATION LOWER
    const outputArea = document.getElementById('processed-analytics-output'); // <<< Keep for now? applyFilters checks output-table
    const filterControlsContainer = document.getElementById('filter-controls-container');
    const addFilterBtn = document.getElementById('add-filter-btn');
    const applyFiltersBtn = document.getElementById('apply-filters-btn');
    const resetFiltersBtn = document.getElementById('reset-filters-btn');
    const filterResultsCount = document.getElementById('filter-results-count');
    const fieldConfigContainer = document.getElementById('field-config-container');

    // Report Tab (NEW)
    const reportFieldSelector = document.getElementById('report-field-selector'); // Y-axis
    const reportXAxisSelector = document.getElementById('report-x-axis-selector'); // NEW X-axis
    const reportChartCanvas = document.getElementById('report-chart-canvas');
    const chartStatus = document.getElementById('chart-status');
    const reportColorSelector = document.getElementById('report-color-selector');
    const reportChartTypeSelector = document.getElementById('report-chart-type-selector'); // NEW
    let reportChartInstance = null;
    const reportSizeSelector = document.getElementById('report-size-selector'); // NEW Size selector
    const resetChartBtn = document.getElementById('reset-chart-btn'); // NEW Reset button
    const swapAxesBtn = document.getElementById('swap-axes-btn'); // NEW Swap button
    const dataVisualTabTrigger = document.getElementById('data-visual-tab'); // <<< ADDED for tab refresh

    // --- NEW: Chart Interaction State ---
    let currentChartPlotData = []; // Holds plot data ({x, y, ticker,...}) for current chart
    let highlightedPointIndex = -1;
    let originalPointStyle = null;
    let chartSearchTimeout;
    const HIGHLIGHT_COLOR = 'rgba(255, 255, 0, 1)'; // Bright yellow
    const HIGHLIGHT_RADIUS_INCREASE = 3;

    // --- File Handling Logic (Import Tab) ---
    function handleFile(file) {
        const dropZoneP = dropZone.querySelector('p');
        const iconSpan = dropZoneP ? dropZoneP.querySelector('span.status-icon') : null;
        const textSpan = dropZoneP ? dropZoneP.querySelector('span.status-text') : null; // Target text span

        if (!file) {
            if (dropZoneP && iconSpan && textSpan) {
                textSpan.textContent = 'Drag & drop ticker file here (.txt only), or click to select'; // Use textContent
                iconSpan.className = 'status-icon bi bi-cloud-arrow-up me-2 align-middle text-muted'; // Default icon + color
            }
            runFinvizUploadBtn.disabled = true; // Disable button if no file
            uploadedTickers = [];
            return;
        }

        if (!file.name.endsWith('.txt')) {
            if (dropZoneP && iconSpan && textSpan) {
                textSpan.textContent = `Invalid file: ${file.name}. Please use a .txt file.`; // Use textContent
                iconSpan.className = 'status-icon bi bi-x-octagon-fill me-2 align-middle text-danger'; // Error icon + color
            }
            runFinvizUploadBtn.disabled = true; // Disable button if invalid file
            uploadedTickers = [];
            tickerFileInput.value = ''; // Reset file input
            return;
        }

        const reader = new FileReader();
        reader.onload = function(event) {
            const content = event.target.result;
            const lines = content.split(/\r?\n/);
            uploadedTickers = lines
                .map(line => line.trim().toUpperCase())
                .filter(line => line.length > 0 && line.length <= 10);

            if (dropZoneP && iconSpan && textSpan) {
                if (uploadedTickers.length > 0) {
                    textSpan.textContent = `File: ${file.name} (${uploadedTickers.length} tickers found)`; // Use textContent
                    iconSpan.className = 'status-icon bi bi-check-circle-fill me-2 align-middle text-success'; // Success icon + color
                    
                    // Enable upload button ONLY if the other button is not currently running
                    if (!finvizButton.disabled || finvizButton.querySelector('.spinner-border').style.display === 'none') {
                        runFinvizUploadBtn.disabled = false;
                    }
                    
                    // finvizUploadStatus.textContent = '';
                    // console.log("Tickers from file:", uploadedTickers); // Remove detail log
                } else {
                    textSpan.textContent = `File: ${file.name} - No valid tickers found.`; // Use textContent
                    iconSpan.className = 'status-icon bi bi-exclamation-triangle-fill me-2 align-middle text-warning'; // Warning icon + color
                    runFinvizUploadBtn.disabled = true; // Disable button if no valid tickers
                    uploadedTickers = [];
                }
            }
        };
        reader.onerror = function(event) {
            console.error("File reading error:", event);
            if (dropZoneP && iconSpan && textSpan) {
                textSpan.textContent = `Error reading file: ${file.name}`; // Use textContent
                iconSpan.className = 'status-icon bi bi-shield-exclamation me-2 align-middle text-danger'; // Error icon + color
            }
            runFinvizUploadBtn.disabled = true; // Disable button on error
            uploadedTickers = [];
        };
        reader.readAsText(file);
    }

    if (dropZone && tickerFileInput) {
        // Click drop zone triggers file input
        dropZone.addEventListener('click', () => tickerFileInput.click());

        // Drag events
        dropZone.addEventListener('dragover', (event) => {
            event.preventDefault(); // Prevent default browser behavior
            dropZone.classList.add('dragover');
        });
        dropZone.addEventListener('dragleave', (event) => {
            dropZone.classList.remove('dragover');
        });
        dropZone.addEventListener('drop', (event) => {
            event.preventDefault(); // Prevent default browser behavior
            dropZone.classList.remove('dragover');
            if (event.dataTransfer.files && event.dataTransfer.files.length > 0) {
                const file = event.dataTransfer.files[0];
                tickerFileInput.files = event.dataTransfer.files; // Assign files to input for consistency
                handleFile(file);
                event.dataTransfer.clearData();
            }
        });

        // File input change event
        tickerFileInput.addEventListener('change', (event) => {
            if (event.target.files && event.target.files.length > 0) {
                handleFile(event.target.files[0]);
            }
        });
    }

    // --- Button Listeners (Import Tab) ---
    // 1. Fetch for Screened Tickers
    if (finvizButton) { // Simplified check, only button needed
        // <<< Declare finvizStatus HERE >>>
        const finvizStatus = document.getElementById('finviz-status');
        if (!finvizStatus) {
            console.error("Could not find finviz-status element (#finviz-status). Listener not attached."); // Updated message
        } else {
            finvizButton.addEventListener('click', async function() {
                // Disable both buttons and show spinner on the clicked one
                showSpinner(finvizButton, runFinvizUploadBtn);
                finvizStatus.textContent = 'Starting fetch job...';
                finvizStatus.className = 'ms-2 text-info';
                let eventSource = null; // Variable to hold EventSource connection

                try {
                    console.log("Calling /api/analytics/start-finviz-fetch-screener endpoint...");
                    const response = await fetch('/api/analytics/start-finviz-fetch-screener', {
                        method: 'POST',
                        headers: { 'Accept': 'application/json' }
                    });
                    const result = await response.json();

                    if (!response.ok) {
                        const errorDetail = result.detail || `Fetch trigger failed with status ${response.status} - ${response.statusText}`;
                        console.error("Error response from start-finviz-fetch-screener:", result);
                        throw new Error(errorDetail);
                    }

                    console.log("Fetch job triggered:", result);

                    // --- SSE Integration --- 
                    if (result.job_id) {
                        console.log(`Received job_id: ${result.job_id}. Establishing SSE connection.`);
                        finvizStatus.textContent = `${result.message || 'Fetch job triggered.'} Waiting for completion...`;
                        finvizStatus.className = 'ms-2 text-info';

                        eventSource = new EventSource(`/api/analytics/stream-job-status/${result.job_id}`);

                        eventSource.onmessage = function(event) {
                            console.log("SSE message received:", event.data);
                            try {
                                const data = JSON.parse(event.data);
                                let isFinalStatus = false; // Flag to check if it's a terminal state

                                if (data.status === 'completed') {
                                    finvizStatus.textContent = data.message || 'Job completed successfully.';
                                    finvizStatus.className = 'ms-2 text-success';
                                    console.log("Job completed via SSE.");
                                    isFinalStatus = true;
                                } else if (data.status === 'failed') {
                                    finvizStatus.textContent = `Error: ${data.message || 'Job failed.'}`;
                                    finvizStatus.className = 'ms-2 text-danger';
                                    console.error("Job failed via SSE:", data.message);
                                    isFinalStatus = true;
                                } else if (data.status === 'partial_failure') { // <<< ADD HANDLING FOR PARTIAL FAILURE
                                    finvizStatus.textContent = data.message || 'Job completed with some failures.';
                                    finvizStatus.className = 'ms-2 text-warning'; // Use warning color
                                    console.warn("Job completed with partial failure via SSE:", data.message);
                                    isFinalStatus = true;
                                } else {
                                    // Handle intermediate statuses if backend sends them
                                    finvizStatus.textContent = data.message || 'Job in progress...';
                                    finvizStatus.className = 'ms-2 text-info';
                                }
                                // Close connection and re-enable button on final status
                                if (isFinalStatus) { // <<< Check the flag
                                    eventSource.close();
                                    console.log("SSE connection closed.");
                                    // Re-enable both buttons (conditionally for upload button)
                                    hideSpinner(finvizButton, runFinvizUploadBtn);
                                }
                            } catch (e) {
                                console.error("Error parsing SSE message:", e);
                                finvizStatus.textContent = 'Error processing status update.';
                                finvizStatus.className = 'ms-2 text-warning';
                                if (eventSource) eventSource.close(); // Close on parsing error
                                hideSpinner(finvizButton, runFinvizUploadBtn); // Re-enable both
                            }
                        };

                        eventSource.onerror = function(error) {
                            console.error("SSE connection error:", error);
                            // Update status only if it hasn't already shown completion/failure
                            if (!finvizStatus.classList.contains('text-success') && !finvizStatus.classList.contains('text-danger')) {
                                 finvizStatus.textContent = 'Error receiving status updates. Check console.';
                                 finvizStatus.className = 'ms-2 text-warning';
                            }
                            if (eventSource) eventSource.close(); // Ensure connection is closed
                            hideSpinner(finvizButton, runFinvizUploadBtn); // Re-enable both
                        };

                    } else {
                        // If no job_id received, handle as before (show initial message)
                        console.warn("No job_id received in response. Cannot track completion status.");
                        finvizStatus.textContent = result.message || 'Fetch job triggered successfully.';
                        finvizStatus.className = 'ms-2 text-success';
                        hideSpinner(finvizButton, runFinvizUploadBtn); // Re-enable both
                    }
                    // --- End SSE Integration ---

                } catch (error) {
                    console.error('Error triggering Finviz fetch for screened tickers:', error);
                    finvizStatus.textContent = `Error: ${error.message || 'An unknown error occurred.'}`;
                    finvizStatus.className = 'ms-2 text-danger';
                    if (eventSource) eventSource.close(); // Close SSE if open
                    hideSpinner(finvizButton, runFinvizUploadBtn); // Re-enable both
                } // No finally block needed as button re-enabled within logic
            });
        } // <<< Closing brace for 'else' associated with 'if (!finvizStatus)'
    } else {
        // This else corresponds to if (finvizButton)
        console.error("Could not find finvizButton element (#run-finviz-btn). Listener not attached."); 
    }

    // 2. Fetch for Uploaded Tickers
    if (runFinvizUploadBtn) { // Simplified check
        // <<< Declare finvizUploadStatus HERE >>>
        const finvizUploadStatus = document.getElementById('finviz-upload-status');
        if (!finvizUploadStatus) {
            console.error("Could not find finviz-upload-status element.");
        } else {
             runFinvizUploadBtn.addEventListener('click', async function() {
                 if (uploadedTickers.length === 0) {
                     finvizUploadStatus.textContent = 'No tickers loaded from file.';
                     finvizUploadStatus.className = 'ms-2 text-warning';
                     return;
                 }

                 // Disable both buttons and show spinner on the clicked one
                 showSpinner(runFinvizUploadBtn, finvizButton);
                 finvizUploadStatus.textContent = `Starting fetch job for ${uploadedTickers.length} tickers...`;
                 finvizUploadStatus.className = 'ms-2 text-info';
                 let eventSourceUpload = null; // Variable for this button's EventSource

                 try {
                     console.log("Calling endpoint /api/analytics/start-finviz-fetch-upload...");

                     const response = await fetch('/api/analytics/start-finviz-fetch-upload', { // <-- Use new endpoint
                         method: 'POST',
                         headers: {
                             'Content-Type': 'application/json',
                             'Accept': 'application/json'
                         },
                         body: JSON.stringify({ tickers: uploadedTickers }) // Send tickers in correct format
                     });
                     const result = await response.json();

                     if (!response.ok) {
                         const errorDetail = result.detail || `Fetch trigger failed with status ${response.status} - ${response.statusText}`;
                         console.error("Error response from start-finviz-fetch-upload:", result);
                         throw new Error(errorDetail);
                     }
                     console.log("Fetch job triggered successfully:", result);

                     // --- SSE Integration for Upload --- 
                     if (result.job_id) {
                          console.log(`Received job_id: ${result.job_id}. Establishing SSE connection for upload.`);
                          finvizUploadStatus.textContent = `${result.message || 'Fetch job triggered.'} Waiting for completion...`;
                          finvizUploadStatus.className = 'ms-2 text-info';

                          eventSourceUpload = new EventSource(`/api/analytics/stream-job-status/${result.job_id}`);

                          eventSourceUpload.onmessage = function(event) {
                              console.log("SSE message received (upload):", event.data);
                              try {
                                  const data = JSON.parse(event.data);
                                  let isFinalStatus = false; // Flag

                                  if (data.status === 'completed') {
                                      finvizUploadStatus.textContent = data.message || 'Job completed successfully.';
                                      finvizUploadStatus.className = 'ms-2 text-success';
                                      console.log("Upload job completed via SSE.");
                                      isFinalStatus = true;
                                  } else if (data.status === 'failed') {
                                      finvizUploadStatus.textContent = `Error: ${data.message || 'Job failed.'}`;
                                      finvizUploadStatus.className = 'ms-2 text-danger';
                                      console.error("Upload job failed via SSE:", data.message);
                                      isFinalStatus = true;
                                  } else if (data.status === 'partial_failure') { // <<< ADD HANDLING FOR PARTIAL FAILURE
                                     finvizUploadStatus.textContent = data.message || 'Job completed with some failures.';
                                     finvizUploadStatus.className = 'ms-2 text-warning'; // Use warning color
                                     console.warn("Upload job completed with partial failure via SSE:", data.message);
                                     isFinalStatus = true;
                                  } else {
                                      finvizUploadStatus.textContent = data.message || 'Job in progress...'; 
                                      finvizUploadStatus.className = 'ms-2 text-info';
                                  }
                                  // Close connection and re-enable button on final status
                                  if (isFinalStatus) { // <<< Check the flag
                                      eventSourceUpload.close();
                                      console.log("SSE connection closed (upload).");
                                      // Re-enable both buttons (conditionally for upload button)
                                      hideSpinner(runFinvizUploadBtn, finvizButton);
                                  }
                              } catch (e) {
                                  console.error("Error parsing SSE message (upload):", e);
                                  finvizUploadStatus.textContent = 'Error processing status update.';
                                  finvizUploadStatus.className = 'ms-2 text-warning';
                                  eventSourceUpload.close(); // Close on error
                                  hideSpinner(runFinvizUploadBtn, finvizButton); // Re-enable both
                              }
                          };

                          eventSourceUpload.onerror = function(error) {
                              console.error("SSE connection error (upload):", error);
                              // Update status only if it hasn't already shown completion/failure
                              if (!finvizUploadStatus.classList.contains('text-success') && !finvizUploadStatus.classList.contains('text-danger')) {
                                  finvizUploadStatus.textContent = 'Error receiving status updates. Check console.';
                                  finvizUploadStatus.className = 'ms-2 text-warning';
                              }
                              eventSourceUpload.close(); // Ensure closed
                              hideSpinner(runFinvizUploadBtn, finvizButton); // Re-enable both
                          };

                      } else {
                          // If no job_id received, handle as before
                          console.warn("No job_id received in upload response. Cannot track completion status.");
                          finvizUploadStatus.textContent = result.message || 'Fetch job triggered successfully.';
                          finvizUploadStatus.className = 'ms-2 text-success';
                          hideSpinner(runFinvizUploadBtn, finvizButton); // Re-enable both
                      }
                      // --- End SSE Integration for Upload ---

                 } catch (error) {
                     console.error('Error triggering Finviz fetch for uploaded tickers:', error);
                     finvizUploadStatus.textContent = `Error: ${error.message || 'An unknown error occurred.'}`;
                     finvizUploadStatus.className = 'ms-2 text-danger';
                      if (eventSourceUpload) eventSourceUpload.close(); // Close SSE if open
                      hideSpinner(runFinvizUploadBtn, finvizButton); // Re-enable both
                 } // No finally block needed
             });
        }
    }

    // --- Storage & State Functions (Preparation Tab) ---
    function loadFiltersFromStorage() {
        console.log("Loading filters from localStorage...");
        const savedFilters = localStorage.getItem(FILTER_STORAGE_KEY);
        let loaded = [];
        if (savedFilters) {
            try {
                loaded = JSON.parse(savedFilters);
                 // <<< Log parsed data >>>
                 console.log("[loadFiltersFromStorage] Parsed from localStorage:", JSON.parse(JSON.stringify(loaded)));
                if (!Array.isArray(loaded)) loaded = [];
            } catch (e) {
                console.error("Error parsing saved filters:", e);
                loaded = [];
                localStorage.removeItem(FILTER_STORAGE_KEY);
            }
        } else {
            console.log("No saved filters found.");
        }

        // Ensure structure (id, field, operator, value, comment)
        currentFilters = loaded.map(f => ({
            id: f.id || Date.now() + Math.random(), // Add random for better uniqueness if needed
            field: f.field || '',
            operator: f.operator || '=',
            value: f.value !== undefined ? f.value : '',
            comment: f.comment || '' // Add comment field with default
        }));

        console.log("Processed loaded/default filters:", currentFilters);
        // <<< Log final currentFilters state >>>
        console.log("[loadFiltersFromStorage] State of currentFilters AFTER loading/mapping:", JSON.parse(JSON.stringify(currentFilters)));

        // Add default blank filter if none loaded
        if (currentFilters.length === 0) {
            currentFilters.push({ id: Date.now() + Math.random(), field: '', operator: '=', value: '', comment: '' }); // Add comment
            console.log("Added default blank filter.");
        }
    }

    function saveFiltersToStorage() {
         if (!Array.isArray(currentFilters)) {
             console.error("Attempted to save non-array filters:", currentFilters);
             return;
         }
        // Save id, field, operator, value, comment
        const filtersToSave = currentFilters.map(f => ({
             id: f.id,
             field: f.field,
             operator: f.operator,
             value: f.value,
             comment: f.comment // Add comment field
         }));
        console.log("Saving filters to localStorage:", filtersToSave);
        try {
             localStorage.setItem(FILTER_STORAGE_KEY, JSON.stringify(filtersToSave));
        } catch (e) {
            console.error("Error saving filters to localStorage:", e);
        }
    }

    // NEW function for enabled status
    function loadEnabledStatusFromStorage() {
        console.log("Loading enabled status from localStorage...");
        const savedStatus = localStorage.getItem(FIELD_ENABLED_STORAGE_KEY);
        if (savedStatus) {
            try {
                fieldEnabledStatus = JSON.parse(savedStatus);
                if (typeof fieldEnabledStatus !== 'object' || fieldEnabledStatus === null || Array.isArray(fieldEnabledStatus)) {
                    fieldEnabledStatus = {}; // Reset if not valid object
                }
                // Ensure all values are boolean
                for (const field in fieldEnabledStatus) {
                    if (fieldEnabledStatus.hasOwnProperty(field)) {
                        fieldEnabledStatus[field] = Boolean(fieldEnabledStatus[field]);
                    }
                }
                console.log("Loaded field enabled status:", fieldEnabledStatus);
            } catch (e) {
                console.error("Error parsing saved enabled status:", e);
                fieldEnabledStatus = {};
                localStorage.removeItem(FIELD_ENABLED_STORAGE_KEY);
            }
        } else {
            fieldEnabledStatus = {}; // Initialize empty if nothing saved
            console.log("No saved enabled status found.");
        }
    }

    // NEW function for enabled status
    function saveEnabledStatusToStorage() {
         console.log("Saving enabled status to localStorage:", fieldEnabledStatus);
         try {
            localStorage.setItem(FIELD_ENABLED_STORAGE_KEY, JSON.stringify(fieldEnabledStatus));
         } catch (e) {
            console.error("Error saving enabled status to localStorage:", e);
         }
    }

    // --- NEW: Storage for Numeric Formats ---
    function loadNumericFormatsFromStorage() {
        console.log("Loading numeric formats from localStorage...");
        const savedFormats = localStorage.getItem(FIELD_NUMERIC_FORMAT_STORAGE_KEY);
        if (savedFormats) {
            try {
                fieldNumericFormats = JSON.parse(savedFormats);
                if (typeof fieldNumericFormats !== 'object' || fieldNumericFormats === null || Array.isArray(fieldNumericFormats)) {
                    fieldNumericFormats = {}; // Reset if not valid object
                }
                console.log("Loaded numeric formats:", fieldNumericFormats);
            } catch (e) {
                console.error("Error parsing saved numeric formats:", e);
                fieldNumericFormats = {};
                localStorage.removeItem(FIELD_NUMERIC_FORMAT_STORAGE_KEY);
            }
        } else {
            fieldNumericFormats = {}; // Initialize empty if nothing saved
            console.log("No saved numeric formats found.");
        }
    }

    function saveNumericFormatsToStorage() {
        console.log("Saving numeric formats to localStorage:", fieldNumericFormats);
        try {
            localStorage.setItem(FIELD_NUMERIC_FORMAT_STORAGE_KEY, JSON.stringify(fieldNumericFormats));
        } catch (e) {
            console.error("Error saving numeric formats to localStorage:", e);
        }
    }
    // --- END NEW: Storage for Numeric Formats ---

    // --- NEW: Helper Function to Get Field Descriptor String ---
    function getFieldDescriptor(fieldName) {
        const metadata = fieldMetadata[fieldName];

        if (!metadata) {
            return "(Metadata not available)"; // Fallback
        }

        // const count = metadata.existingValueCount; // Count is now displayed separately

        switch (metadata.type) {
            case 'numeric':
                const min = metadata.min; // Already handles null from step 2
                const max = metadata.max; // Already handles null from step 2
                const average = metadata.average; // <<< ADDED
                const median = metadata.median;   // <<< ADDED
                const format = fieldNumericFormats[fieldName] || 'default'; // Use specific field format

                // Inline helper for consistent formatting
                const formatNumberForDescriptor = (num) => {
                    if (num === null || num === undefined) return 'N/A';
                    // Use the field's specific format for min/max/avg/median display
                    return formatNumericValue(num, format);
                };

                const minFormatted = formatNumberForDescriptor(min);
                const maxFormatted = formatNumberForDescriptor(max);
                const avgFormatted = formatNumberForDescriptor(average);     // <<< ADDED
                const medianFormatted = formatNumberForDescriptor(median); // <<< ADDED

                // <<< Modified return string >>>
                return `Numeric | Min: ${minFormatted} | Max: ${maxFormatted} | Avg: ${avgFormatted} | Median: ${medianFormatted}`;
            case 'text':
                const uniqueCount = metadata.totalUniqueCount !== undefined ? metadata.totalUniqueCount : 'N/A';
                // const textCountDisplay = typeof count === 'number' ? `(${count} records)` : '(count N/A)'; // REMOVED count display
                let examples = '';
                let introPhrase = '';
                if (metadata.uniqueValues && metadata.uniqueValues.length > 0) {
                    const numExamples = Math.min(metadata.uniqueValues.length, 3);
                    const exampleValues = metadata.uniqueValues.slice(0, numExamples).map(v => `"${v}"`); // Add quotes
                    examples = exampleValues.join(', ');
                    
                    if (numExamples === 1) {
                        introPhrase = 'First value found: ';
                    } else if (numExamples === 2) {
                        introPhrase = 'First two found: ';
                    } else { // 3 or more (but we only show 3)
                        introPhrase = 'First three found: ';
                    }
                } else {
                    introPhrase = '(No values found)'
                }
                // REMOVED count display from return string
                return `Text (${uniqueCount} unique values). ${introPhrase}${examples}`;
            case 'empty':
                return 'Empty'; 
            default:
                // Fallback, still try to show count if available // REMOVED count display
                // const fallbackCountDisplay = typeof count === 'number' ? `(${count} records)` : ''; 
                return `(Unknown type: ${metadata.type})`.trim(); // Keep trim just in case
        }
    }
    // --- END NEW Helper Function ---

    // --- NEW: Helper Function to Format Numeric Values ---
    function formatNumericValue(rawValue, format = 'default') {
        if (rawValue === null || rawValue === undefined || String(rawValue).trim() === '' || String(rawValue).trim() === '-') {
            return 'N/A'; // Or return rawValue if preferred for N/A cases
        }

        const num = Number(rawValue);
        if (isNaN(num)) {
            return String(rawValue); // Return original if not a number
        }

        let formattedValue;

        switch (format) {
            case 'percent':
                // Multiply by 100 for percentage display
                formattedValue = (num * 100).toFixed(2) + '%'; 
                break;
            case 'million':
                formattedValue = (num / 1_000_000).toFixed(2) + 'M';
                break;
            case 'billion':
                formattedValue = (num / 1_000_000_000).toFixed(2) + 'B';
                break;
            case 'integer': // NEW: Integer format
                formattedValue = Math.round(num).toString(); // Round to nearest integer
                break;
            case 'raw': // NEW: Raw format
                formattedValue = String(num); // No rounding or suffix
                break;
            case 'default':
            default:
                formattedValue = num.toFixed(2);
                break;
        }

        return formattedValue;
    }
    // --- END NEW Formatting Helper ---

    // --- NEW: Helper Function to Parse Formatted Input to Raw Value ---
    function parseFormattedValue(inputValue, format = 'default') {
        const valueStr = String(inputValue).trim();
        if (valueStr === '') {
            return null; // Treat empty input as null or undefined?
        }

        const num = Number(valueStr);
        if (isNaN(num)) {
            // Consider handling suffixes like M, B, % directly? For now, assume user enters plain number.
            console.warn(`[parseFormattedValue] Input '${valueStr}' is not a valid number.`);
            return NaN; // Indicate parsing failure 
        }

        let rawValue;
        switch (format) {
            case 'percent':
                // Convert percentage input back to decimal
                rawValue = num / 100;
                break;
            case 'million':
                // Convert million input back to raw number
                rawValue = num * 1_000_000;
                break;
            case 'billion':
                // Convert billion input back to raw number
                rawValue = num * 1_000_000_000;
                break;
            case 'integer':
            case 'default':
            case 'raw':
            default:
                // No conversion needed for these formats
                rawValue = num;
                break;
        }

        // console.log(`[parseFormattedValue] Input: ${inputValue}, Format: ${format}, Parsed Raw: ${rawValue}`);
        return rawValue;
    }
    // --- END NEW Parsing Helper ---

    // --- NEW: Helper function to calculate Median ---
    function calculateMedian(numericValues) {
        if (!numericValues || numericValues.length === 0) {
            return null;
        }
        // Create a copy and sort numbers numerically
        const sortedValues = [...numericValues].sort((a, b) => a - b);
        const mid = Math.floor(sortedValues.length / 2);

        if (sortedValues.length % 2 === 0) {
            // Even number of values: average the two middle ones
            return (sortedValues[mid - 1] + sortedValues[mid]) / 2;
        } else {
            // Odd number of values: return the middle one
            return sortedValues[mid];
        }
    }
    // --- END NEW Helper ---

    // --- Helper to update value input based on field metadata (Preparation Tab) ---
    function updateValueInputUI(index, fieldName, inputWrapper, hintSpan) {
        const metadata = fieldMetadata[fieldName];
        // <<< ADD LOG: Check if metadata exists >>>
        console.log(`[updateValueInputUI] Checking metadata for field '${fieldName}':`, metadata ? 'Found' : 'NOT FOUND');
        
        inputWrapper.innerHTML = ''; // Clear previous input/select
        // <<< ADD LOG: Log the filter value being used >>>
        const filterValueForUI = currentFilters[index]?.value;
        console.log(`[updateValueInputUI] Rendering value for filter index ${index} (Field: ${fieldName}). Loaded Value:`, filterValueForUI);
        
        // --- Update hint text using the new descriptor function (which now includes formatting) --- 
        let hintText = fieldName ? getFieldDescriptor(fieldName) : '';
        // Append the input scale guidance if applicable
        if (fieldNumericFormats[fieldName] || 'default') {
            const format = fieldNumericFormats[fieldName] || 'default';
            switch (format) {
                case 'percent': hintText += ' (Enter % value, e.g., 5 for 5%)'; break;
                case 'million': hintText += ' (Enter value in Millions, e.g., 1.5 for 1.5M)'; break;
                case 'billion': hintText += ' (Enter value in Billions, e.g., 2.1 for 2.1B)'; break;
                // No specific suffix needed for default, integer, raw
            }
        }
        hintSpan.textContent = hintText;
        // --- End update hint text ---
        
        let currentInput = null;

        // Define a common handler for updating the filter state
        const updateFilterValue = (newValue) => {
            if (currentFilters[index]) { // Ensure filter still exists
                currentFilters[index].value = newValue;
            }
        };

        // --- Existing logic to create input/select based on metadata --- 
        if (metadata && metadata.type === 'text' && metadata.uniqueValues && metadata.uniqueValues.length > 0
            && metadata.uniqueValues.length <= TEXT_FILTER_DROPDOWN_THRESHOLD) { // <<< ADD THRESHOLD CHECK
            // --- Create Multi-Select (Only if below threshold) ---
            // hintSpan.textContent = `(${metadata.uniqueValues.length} unique values)`; // REMOVED - Hint set above
            const select = document.createElement('select');
            select.multiple = true;
            select.className = 'form-select form-select-sm w-100';
            select.size = Math.min(metadata.uniqueValues.length, 4);

            metadata.uniqueValues.forEach(val => {
                const option = document.createElement('option');
                option.value = val;
                option.textContent = val;
                // Check if current filter value (which might be an array) includes this option
                if (currentFilters[index] && Array.isArray(currentFilters[index].value) && currentFilters[index].value.includes(val)) {
                    option.selected = true;
                }
                select.appendChild(option);
            });

            select.addEventListener('change', (e) => {
                const selectedValues = Array.from(e.target.selectedOptions).map(opt => opt.value);
                updateFilterValue(selectedValues); // Update state with array
            });

            inputWrapper.appendChild(select);
            currentInput = select;

            // <<< Log details before setting dropdown selection >>>
            console.log(`[updateValueInputUI] Dropdown - Filter index ${index}, Field: ${fieldName}, Saved value type: ${typeof currentFilters[index]?.value}, Saved value:`, currentFilters[index]?.value);

        } else {
             // --- Create Text/Number Input (Fallback for text with many values, or non-text types) ---
            const input = document.createElement('input');
            input.className = 'form-control form-control-sm';
            input.placeholder = 'Value';
            // Ensure value is treated as a string for text input
            let initialValue = '';
            if (currentFilters[index]) {
                const filterVal = currentFilters[index].value;
                initialValue = Array.isArray(filterVal)
                                ? '' // Clear if switching from multi-select
                                : (filterVal !== null && filterVal !== undefined ? String(filterVal) : '');
            }
            input.value = initialValue;
            // <<< Log details before setting input value >>>
            console.log(`[updateValueInputUI] Input - Filter index ${index}, Field: ${fieldName}, Saved value:`, currentFilters[index]?.value, `InitialValue set: '${initialValue}'`);

            // Set input type based on metadata (hint is set above)
            // No change needed here, hint updated above covers numeric range display
            if (metadata && metadata.type === 'numeric') {
                input.type = 'number';
                input.step = 'any';
            } else {
                input.type = 'text';
                
                // --- ADD DATALIST for text inputs exceeding threshold ---
                if (metadata && metadata.type === 'text' && metadata.uniqueValues && metadata.uniqueValues.length > 0) {
                    const datalistId = `datalist-${index}-${fieldName.replace(/\W/g, '_')}`; // Unique ID
                    input.setAttribute('list', datalistId);

                    const datalist = document.createElement('datalist');
                    datalist.id = datalistId;

                    metadata.uniqueValues.forEach(val => {
                        const option = document.createElement('option');
                        option.value = val; 
                        // Optionally: option.textContent = val; (usually not needed for datalist)
                        datalist.appendChild(option);
                    });
                    
                    // --- Add log to show datalist content ---
                    // console.log(`[Datalist] Populating datalist #${datalistId} for field '${fieldName}' with options:`, metadata.uniqueValues); // Remove detail log
                    // --- End log ---

                    // Append datalist to the same wrapper as the input
                    inputWrapper.appendChild(datalist); 
                    // console.log(`Created datalist ${datalistId} for field ${fieldName} with ${metadata.uniqueValues.length} options.`); // Remove detail log
                }
                // --- END DATALIST ---
            }
            // REMOVED empty check hint, covered by descriptor
            // if (metadata && metadata.type === 'empty') {
            //     hintSpan.textContent = '(No values found in data)';
            // }

            // Use 'input' event for text/number fields
            input.addEventListener('input', (e) => {
                 updateFilterValue(e.target.value); // Update state with string value
            });

            inputWrapper.appendChild(input);
            currentInput = input;
        }
        // --- End existing logic --- 

        // Hide/Show based on operator (applies to the wrapper AND hint now)
        if (currentFilters[index]) {
            const operator = currentFilters[index].operator;
            const shouldHide = operator === 'exists' || operator === 'notExists';
            inputWrapper.style.display = shouldHide ? 'none' : '';
            hintSpan.style.display = shouldHide ? 'none' : ''; // Control hint visibility too
        }
    }

    // --- Render UI Functions (Preparation Tab) ---
    function renderFilterUI() {
        console.log("Rendering filter UI...");
        // <<< ADD LOG: Show currentFilters state before rendering >>>
        console.log("[renderFilterUI] State of currentFilters at START of function:", JSON.parse(JSON.stringify(currentFilters)));
        // console.log("[renderFilterUI] State of currentFilters before rendering loop:", JSON.parse(JSON.stringify(currentFilters))); // Keep this one too?

        if (!filterControlsContainer) return; // Check if container exists
        filterControlsContainer.innerHTML = ''; // Clear existing rows

        // Filter available fields based on enabled status
        const enabledFields = availableFields.filter(field => fieldEnabledStatus[field] === true);
        console.log("Rendering filters using enabled fields:", enabledFields);

        if (!currentFilters || currentFilters.length === 0) {
            // filterControlsContainer.innerHTML = '<p class="text-muted small mb-0">No filters defined. Click \'+'+\' Add Filter.\'</p>'; // Old version
            filterControlsContainer.innerHTML = `<p class="text-muted small mb-0">No filters defined. Click '+ Add Filter'.</p>`; // Use template literal
            return;
        }

        const operators = [
            { value: '=', text: '=' },
            { value: '>', text: '>' },
            { value: '<', text: '<' },
            { value: '>=', text: '>=' },
            { value: '<=', text: '<=' },
            { value: '!=', text: '!=' },
            { value: 'contains', text: 'contains' },
            { value: 'startsWith', text: 'starts with' },
            { value: 'endsWith', text: 'ends with' },
            { value: 'exists', text: 'exists (non-empty)'}, // Check if field exists and is not null/empty/None
            { value: 'notExists', text: 'does not exist / empty'} // Check if field is missing or null/empty/None
        ];

        currentFilters.forEach((filter, index) => {
            // <<< ADD LOG: Show filter being rendered >>>
            console.log(`[renderFilterUI] Rendering row for filter index ${index}:`, JSON.parse(JSON.stringify(filter)));

            const filterId = filter.id;
            const row = document.createElement('div');
            // Use d-flex for the main row container
            row.className = 'filter-row-container mb-2'; // Container for row + hint
            row.dataset.filterId = filterId;

            const filterRowDiv = document.createElement('div');
            filterRowDiv.className = 'd-flex align-items-center filter-row'; // Flexbox for controls + comment + remove

            // Field Select (fixed width)
            const fieldSelect = document.createElement('select');
            fieldSelect.className = 'form-select form-select-sm me-2 w-auto';
            fieldSelect.title = 'Select Field';
            fieldSelect.innerHTML = '<option value="">-- Field --</option>';
            // Use ONLY enabled fields for the dropdown
            enabledFields.forEach(fieldName => {
                const option = document.createElement('option');
                option.value = fieldName;
                option.textContent = fieldName;
                if (filter.field === fieldName) option.selected = true;
                fieldSelect.appendChild(option);
            });
            filterRowDiv.appendChild(fieldSelect);

            // Operator Select (fixed width)
            const operatorSelect = document.createElement('select');
            operatorSelect.className = 'form-select form-select-sm me-2 w-auto';
            operatorSelect.title = 'Select Operator';
            operators.forEach(op => {
                const option = document.createElement('option');
                option.value = op.value;
                option.textContent = op.text;
                if (filter.operator === op.value) option.selected = true;
                operatorSelect.appendChild(option);
            });
             filterRowDiv.appendChild(operatorSelect);

            // Value Input Wrapper
            const valueWrapper = document.createElement('div');
            // Limit growth and set max-width
            valueWrapper.className = 'value-input-wrapper me-2';
            valueWrapper.style.flexGrow = '0'; // Don't allow growth
            valueWrapper.style.flexShrink = '0'; // Don't allow shrinking either?
            valueWrapper.style.maxWidth = '250px'; // Set max width to 250px
            valueWrapper.style.width = '250px'; // Also set width for consistency?
            filterRowDiv.appendChild(valueWrapper);

            // Comment Input
            const commentInput = document.createElement('input');
            // Allow comment to grow now
            commentInput.className = 'form-control form-control-sm ms-2 me-2 flex-grow-1';
            commentInput.style.flexGrow = '1'; // Allow comment to take remaining space
            commentInput.style.flexShrink = '1'; // Allow shrinking
            commentInput.style.maxWidth = ''; // Remove previous max width
            commentInput.placeholder = 'Comment';
            commentInput.value = filter.comment || '';
            commentInput.title = 'Filter Comment';
            filterRowDiv.appendChild(commentInput);

            // Hint Span
            const hintSpan = document.createElement('span');
            hintSpan.className = 'value-hint small text-muted d-block ms-1';

            // Remove Button
            const removeBtn = document.createElement('button');
            removeBtn.textContent = '×';
            // Keep ms-auto to push it right
            removeBtn.className = 'btn btn-sm btn-outline-danger ms-auto';
            removeBtn.title = 'Remove this filter';
            removeBtn.style.flexGrow = '0'; // Don't allow button to grow
            removeBtn.style.flexShrink = '0'; // Don't allow button to shrink
            removeBtn.addEventListener('click', () => {
                 const indexToRemove = currentFilters.findIndex(f => f.id === filterId);
                 if (indexToRemove > -1) {
                     currentFilters.splice(indexToRemove, 1);
                     saveFiltersToStorage();
                     renderFilterUI();
                 }
             });
            filterRowDiv.appendChild(removeBtn);

            // --- Event Listeners ---
            fieldSelect.addEventListener('change', (e) => {
                const indexToUpdate = currentFilters.findIndex(f => f.id === filterId);
                if (indexToUpdate > -1) {
                    currentFilters[indexToUpdate].field = e.target.value;
                    updateValueInputUI(indexToUpdate, e.target.value, valueWrapper, hintSpan);
                }
            });
             operatorSelect.addEventListener('change', (e) => {
                 const indexToUpdate = currentFilters.findIndex(f => f.id === filterId);
                 if (indexToUpdate > -1) {
                     currentFilters[indexToUpdate].operator = e.target.value;
                     // Update visibility of the wrapper and hint
                     const op = e.target.value;
                     valueWrapper.style.display = (op === 'exists' || op === 'notExists') ? 'none' : '';
                     hintSpan.style.display = (op === 'exists' || op === 'notExists') ? 'none' : '';
                 }
            });
             // Note: valueInput listener is now added *inside* updateValueInputUI
             commentInput.addEventListener('input', (e) => {
                 const indexToUpdate = currentFilters.findIndex(f => f.id === filterId);
                 if (indexToUpdate > -1) {
                     currentFilters[indexToUpdate].comment = e.target.value;
                 }
            });

            // Initial UI update for value input based on loaded field
            // Use findIndex again to ensure we use the correct index after potential resets
            const currentIndex = currentFilters.findIndex(f => f.id === filterId);
            if (currentIndex > -1) {
                updateValueInputUI(currentIndex, filter.field, valueWrapper, hintSpan);
            }

            // Append main row and hint to the container
            row.appendChild(filterRowDiv);
            row.appendChild(hintSpan);
            filterControlsContainer.appendChild(row);
        });
    }

    function renderFieldConfigUI() {
        console.log("Rendering Field Config UI...");
        if (!fieldConfigContainer) return;

        fieldConfigContainer.innerHTML = ''; // Clear previous content

        // --- Create and Prepend Search Input --- //
        const searchGroup = document.createElement('div');
        searchGroup.className = 'input-group input-group-sm mb-2 px-2'; // Added padding
        const searchIcon = document.createElement('span');
        searchIcon.className = 'input-group-text';
        searchIcon.innerHTML = '<i class="bi bi-search"></i>';
        const searchInput = document.createElement('input');
        searchInput.type = 'text';
        searchInput.id = 'pre-transform-field-search'; // Unique ID
        searchInput.className = 'form-control form-control-sm';
        searchInput.placeholder = 'Search fields...';
        searchGroup.appendChild(searchIcon);
        searchGroup.appendChild(searchInput);
        fieldConfigContainer.appendChild(searchGroup);
        // --- End Search Input ---

        // --- Create Header Row --- //
        const headerRow = document.createElement('div');
        headerRow.className = 'd-flex align-items-center mb-2 p-2 border-bottom fw-bold text-muted small';
        // ... (rest of header row creation remains the same) ...
        headerRow.style.position = 'sticky';
        headerRow.style.top = '0';
        headerRow.style.backgroundColor = 'var(--bs-body-bg, white)'; // Use CSS variable for background, fallback to white
        headerRow.style.zIndex = '10'; // Ensure it stays above scrolling content

        const headers = [
            { key: 'name', text: 'Field name', width: '120px', align: 'start', extraClasses: 'me-3' }, // <<< DECREASED WIDTH
            { key: 'count', text: 'Occured', width: '80px', align: 'end', extraClasses: 'me-3 text-end' }, // Updated text and width
            { key: 'descriptor', text: 'Data descriptor', width: 'auto', align: 'start', extraClasses: 'flex-grow-1 me-3' },
            { key: 'format', text: 'Format', width: '140px', align: 'start', extraClasses: 'me-3' }, // <<< Increased width
            { key: 'info', text: 'Info', width: '300px', align: 'start', extraClasses: 'me-3' }, // <<< INCREASED WIDTH
            { key: 'enabled', text: 'Included', width: '70px', align: 'center', extraClasses: 'text-center' }
        ];

        headers.forEach(headerInfo => {
            const headerEl = document.createElement('div');
            headerEl.textContent = headerInfo.text;
            headerEl.style.minWidth = headerInfo.width;
            if (headerInfo.width === 'auto') {
                headerEl.classList.add('flex-grow-1');
            } else {
                headerEl.style.flexBasis = headerInfo.width; // Use flex-basis for better control
                headerEl.style.flexShrink = '0';
            }
             headerEl.className += ` ${headerInfo.extraClasses || ''}`;
            headerEl.dataset.sortKey = headerInfo.key;
            headerEl.style.cursor = 'pointer';
            headerEl.title = `Sort by ${headerInfo.text}`; // Update title text

            // Add sort indicator space
            const sortIndicator = document.createElement('span');
            sortIndicator.className = 'sort-indicator ms-1'; // Add ms-1 for spacing
            headerEl.appendChild(sortIndicator);

            headerEl.addEventListener('click', () => {
                const sortKey = headerEl.dataset.sortKey;
                if (currentSortKey === sortKey) {
                    currentSortDirection = currentSortDirection === 'asc' ? 'desc' : 'asc';
                } else {
                    currentSortKey = sortKey;
                    currentSortDirection = 'asc';
                }
                sortAndReRenderFields();
            });
            headerRow.appendChild(headerEl);
        });

        fieldConfigContainer.appendChild(headerRow);
        updateSortIndicators(); // Initial indicator update
        // --- End Header Row ---

        if (!availableFields || availableFields.length === 0) {
            // Append message *after* header if no data
            const noDataMsg = document.createElement('p');
            noDataMsg.className = 'text-muted small mt-2 ms-2'; // Added ms-2 for indent
            noDataMsg.textContent = 'Load data first to configure fields.';
            fieldConfigContainer.appendChild(noDataMsg);
             // Attach listener even if no data, so it works if data loads later?
             applyPreTransformSearchListener();
            return;
        }

        // --- Render Data Rows (using a separate function now for clarity) ---
        renderFieldDataRows(availableFields);

        // --- Attach Search Listener --- //
        applyPreTransformSearchListener();
    }

    // --- NEW: Separate function to render just the data rows ---
    function renderFieldDataRows(fieldsToRender) {
         // Clear only existing data rows (keep header)
         const existingRows = fieldConfigContainer.querySelectorAll('.field-data-row');
         existingRows.forEach(row => row.remove());

         fieldsToRender.forEach(field => {
            const metadata = fieldMetadata[field] || {}; // Get metadata or empty obj
            const isEnabled = fieldEnabledStatus.hasOwnProperty(field) ? fieldEnabledStatus[field] : true;
            const existingCount = metadata.existingValueCount !== undefined ? metadata.existingValueCount : null; // Use null for sorting
            const descriptorText = getFieldDescriptor(field);

            const row = document.createElement('div');
            // Add specific class for data rows
            row.className = 'd-flex align-items-center mb-2 p-2 border-bottom field-data-row small'; // <<< ADDED .small CLASS
            row.dataset.fieldName = field; 
            if (!isEnabled) {
                row.style.opacity = '0.6'; 
            }

            // Create elements with matching widths/alignment from header config
            // 1. Name
            const nameSpan = document.createElement('span');
            nameSpan.textContent = field;
            nameSpan.className = 'fw-bold me-3'; // Match header spacing
            nameSpan.style.minWidth = '120px'; // <<< DECREASED WIDTH
            nameSpan.style.flexBasis = '120px'; // <<< DECREASED WIDTH
            nameSpan.style.flexShrink = '0';
            nameSpan.title = field; 
            row.appendChild(nameSpan);

            // 2. Count
            const countSpan = document.createElement('span');
            // Handle null count appropriately for display
            countSpan.textContent = (metadata.type === 'empty' || existingCount === null) ? '-' : existingCount;
            countSpan.className = 'small text-muted text-end me-3'; // Match header spacing & align
            countSpan.style.minWidth = '80px'; // <<< MATCH UPDATED HEADER WIDTH 
            countSpan.style.flexBasis = '80px'; // <<< MATCH UPDATED HEADER WIDTH
            countSpan.style.flexShrink = '0';
            row.appendChild(countSpan);

            // 3. Descriptor
            const descriptorSpan = document.createElement('span');
            descriptorSpan.textContent = descriptorText;
            descriptorSpan.className = 'small text-muted flex-grow-1 me-3'; // Match header spacing 
            row.appendChild(descriptorSpan);
            
            // 4. Format Dropdown (for numeric fields)
            const formatWrapper = document.createElement('div');
            formatWrapper.className = 'me-3'; // Match header spacing
            formatWrapper.style.minWidth = '140px'; // Match increased header width
            formatWrapper.style.flexBasis = '140px';
            formatWrapper.style.flexShrink = '0';
            
            if (metadata.type === 'numeric') {
                const formatSelect = document.createElement('select');
                formatSelect.className = 'form-select form-select-sm format-select'; // <<< ADDED format-select class
                formatSelect.title = 'Select numeric format';
                
                const formats = [ // Updated text labels
                    { value: 'raw',     text: 'raw data' },
                    { value: 'integer', text: 'integer' },
                    { value: 'default', text: 'decimal' }, // Changed from 'Default (1.23)'
                    { value: 'percent', text: 'in %' },      // Changed from 'Percent (1.23%)'
                    { value: 'million', text: 'in Millions' },// Changed from 'Millions (1.23M)'
                    { value: 'billion', text: 'in Billions' } // Changed from 'Billions (1.23B)'
                ];

                formats.forEach(fmt => {
                    const option = document.createElement('option');
                    option.value = fmt.value;
                    option.textContent = fmt.text;
                    if (fieldNumericFormats[field] === fmt.value) {
                        option.selected = true;
                    }
                    formatSelect.appendChild(option);
                });

                formatSelect.addEventListener('change', (e) => {
                    const newFormat = e.target.value;
                    fieldNumericFormats[field] = newFormat;
                    saveNumericFormatsToStorage();
                    
                    // Re-render the descriptor span for this row only
                    const updatedDescriptorText = getFieldDescriptor(field);
                    descriptorSpan.textContent = updatedDescriptorText;
                    // Also re-render chart if needed? Or wait for filter/apply? Let's wait.
                    console.log(`Format for field '${field}' changed to '${newFormat}'. Descriptor updated.`);
                    // <<< Explicitly re-attach modification listeners >>>
                    if (window.AnalyticsConfigManager?.initializeModificationDetection) {
                        window.AnalyticsConfigManager.initializeModificationDetection();
                    }
                });

                formatWrapper.appendChild(formatSelect);
            } else {
                 // Optional: Add a placeholder or leave empty for non-numeric fields
                 formatWrapper.textContent = '-'; // Placeholder
                 formatWrapper.classList.add('text-muted', 'text-center');
            }
            row.appendChild(formatWrapper); // Add format selector/placeholder

            // 4.5. <<< NEW: Info Text Input >>>
            const infoWrapper = document.createElement('div');
            infoWrapper.className = 'me-3'; // Match header spacing
            infoWrapper.style.minWidth = '300px'; // <<< INCREASED WIDTH
            infoWrapper.style.flexBasis = '300px'; // <<< INCREASED WIDTH
            infoWrapper.style.flexShrink = '0';

            const infoInput = document.createElement('input');
            infoInput.type = 'text';
            infoInput.className = 'form-control form-control-sm field-info-input'; // Added specific class
            infoInput.placeholder = 'Tooltip text...';
            infoInput.value = fieldInfoTips[field] || ''; // Get value from global map
            infoInput.title = 'Enter tooltip text for this field';

            infoInput.addEventListener('input', (e) => {
                const newValue = e.target.value.trim(); // Get trimmed value
                console.log(`[Analytics Input] Tip changed for '${field}': Setting fieldInfoTips['${field}'] = '${newValue}'`);
                fieldInfoTips[field] = newValue; // Update global map
                saveInfoTipsToStorage(); // Persist change
                // <<< Explicitly re-attach modification listeners >>>
                if (window.AnalyticsConfigManager?.initializeModificationDetection) {
                    window.AnalyticsConfigManager.initializeModificationDetection();
                }
            });

            infoWrapper.appendChild(infoInput);
            row.appendChild(infoWrapper); // Add info input wrapper to row
            // <<< END NEW >>>

            // 5. Enabled Checkbox (was 4)
            const enabledWrapper = document.createElement('div');
            // Center checkbox within its allocated space
            enabledWrapper.className = 'form-check form-switch d-flex justify-content-center'; 
            enabledWrapper.style.minWidth = '70px'; 
            enabledWrapper.style.flexBasis = '70px';
            enabledWrapper.style.flexShrink = '0';
            const enabledCheckbox = document.createElement('input');
            enabledCheckbox.type = 'checkbox';
            enabledCheckbox.className = 'form-check-input';
            enabledCheckbox.checked = isEnabled;
            enabledCheckbox.id = `enable-${field.replace(/\W/g, '_')}`;
            enabledCheckbox.title = isEnabled ? 'Disable this field' : 'Enable this field';
            enabledCheckbox.addEventListener('change', (e) => {
                const newStatus = e.target.checked;
                fieldEnabledStatus[field] = newStatus;
                saveEnabledStatusToStorage();
                row.style.opacity = newStatus ? '1' : '0.6';
                // Re-rendering the whole table is complex due to sorting, 
                // so we just update opacity here. Filters will use updated status.
                // Consider a full re-sort/re-render if needed after toggle?
                renderFilterUI();
                applyFilters();
                // <<< Explicitly re-attach modification listeners >>>
                if (window.AnalyticsConfigManager?.initializeModificationDetection) {
                    window.AnalyticsConfigManager.initializeModificationDetection();
                }
            });
            enabledWrapper.appendChild(enabledCheckbox);
            row.appendChild(enabledWrapper);

            // <<< NEW: Add Hover Effect Listeners >>>
            row.addEventListener('mouseover', () => {
                row.style.backgroundColor = 'var(--bs-tertiary-bg)'; // Use Bootstrap variable for hover color
            });
            row.addEventListener('mouseout', () => {
                row.style.backgroundColor = ''; // Reset background color
            });
            // <<< END NEW >>>

            fieldConfigContainer.appendChild(row);
        });
    }
    // --- END NEW Data Row Render Function ---

    // --- NEW: Sort and Re-render Function ---
    function sortAndReRenderFields() {
        if (!availableFields || availableFields.length === 0) return; // No fields to sort

        const fieldsToSort = [...availableFields]; // Create a mutable copy

        fieldsToSort.sort((a, b) => {
            const metaA = fieldMetadata[a] || {}; 
            const metaB = fieldMetadata[b] || {};
            let valA, valB;

            switch (currentSortKey) {
                case 'name':
                    valA = a.toLowerCase();
                    valB = b.toLowerCase();
                    break;
                case 'count':
                    // Treat empty/N/A as -1 for sorting purposes to put them first/last
                    valA = metaA.existingValueCount !== undefined && metaA.type !== 'empty' ? metaA.existingValueCount : -1;
                    valB = metaB.existingValueCount !== undefined && metaB.type !== 'empty' ? metaB.existingValueCount : -1;
                    break;
                case 'descriptor':
                    // Sort by the generated text descriptor
                    valA = getFieldDescriptor(a).toLowerCase();
                    valB = getFieldDescriptor(b).toLowerCase();
                    break;
                case 'enabled':
                    // Sort boolean true first (desc) or false first (asc)
                    valA = fieldEnabledStatus.hasOwnProperty(a) ? fieldEnabledStatus[a] : true;
                    valB = fieldEnabledStatus.hasOwnProperty(b) ? fieldEnabledStatus[b] : true;
                    break;
                default:
                    return 0; // No sort
            }

            let comparison = 0;
            if (typeof valA === 'string' && typeof valB === 'string') {
                comparison = valA.localeCompare(valB);
            } else if (typeof valA === 'number' && typeof valB === 'number') {
                comparison = valA - valB;
            } else if (typeof valA === 'boolean' && typeof valB === 'boolean') {
                comparison = (valA === valB) ? 0 : (valA ? -1 : 1); // True first
            }

            return currentSortDirection === 'asc' ? comparison : comparison * -1;
        });

        // Re-render the rows in the sorted order
        renderFieldDataRows(fieldsToSort);
        updateSortIndicators(); // Update indicators after sort
        // <<< Ensure search listener is active after re-rendering rows >>>
        applyPreTransformSearchListener();
    }
    // --- END NEW Sort Function ---

    // --- NEW: Update Sort Indicators ---
    function updateSortIndicators() {
        const headers = fieldConfigContainer.querySelectorAll('[data-sort-key]');
        headers.forEach(header => {
            const indicator = header.querySelector('.sort-indicator');
            if (!indicator) return;

            if (header.dataset.sortKey === currentSortKey) {
                indicator.innerHTML = currentSortDirection === 'asc' ? ' <i class="bi bi-sort-up"></i>' : ' <i class="bi bi-sort-down"></i>';
            } else {
                indicator.innerHTML = ''; // Clear other indicators
            }
        });
    }
    // --- END NEW Sort Indicators --- 

    // --- Apply Filters Function (Preparation Tab) ---
    function applyFilters() {
        console.log("Applying filters...");
        console.log("Data to filter:", fullProcessedData.length);
        console.log("Enabled status:", fieldEnabledStatus);

        // Changed from outputArea to outputTableContainer // TODO: Verify if outputTableContainer is the correct ID
        const outputTable = document.getElementById('output-table'); // <<< CHANGED: Get table element
        if (!outputTable || !filterResultsCount) { // Check table and count element
            console.error("Output table or filter count element not found.");
            return;
        }

        if (!fullProcessedData || fullProcessedData.length === 0) {
            // outputArea.textContent = 'No data loaded to filter.'; // OLD PRE TAG
            // Clear DataTable if it exists and show message
            if ($.fn.dataTable.isDataTable('#output-table')) {
                outputDataTable.clear().draw();
                // Optionally update the table body with a message
                $('#output-table tbody').html('<tr><td colspan="100%" class="text-center text-muted small">No data loaded to filter.</td></tr>');
            }
            filterResultsCount.textContent = '(0 records)';
            filteredDataForChart = []; // Clear chart data too
            renderChart();
            return;
        }

        const activeFilters = currentFilters.filter(f => f.field && f.field !== '' && f.operator); // Also ensure operator exists

        let filteredData = fullProcessedData;

        if (activeFilters.length > 0) {
             filteredData = fullProcessedData.filter(item => {
                 if (!item) return false; // Removed || !item.processed_data check here, handle inside loop

                 for (const filter of activeFilters) {
                     // --- Get item value based on field --- 
                     let itemValue = null;
                     if (filter.field === 'source') {
                         itemValue = item.source;
                     } else if (item.processed_data) { // Check if processed_data exists before accessing
                        itemValue = item.processed_data[filter.field];
                     }
                     // --- End Get item value ---

                     // --- Get filter value and operator (Declared only once) --- 
                     const filterValue = filter.value;
                     const operator = filter.operator;
                     // --- End Get value/operator --- 

                     // --- Multi-Select Handling --- // (Keep existing filter logic)
                     if (Array.isArray(filterValue)) {
                        const itemValueStr = String(itemValue);
                        if (operator === '=') {
                            if (!filterValue.includes(itemValueStr)) return false;
                        } else if (operator === '!=') {
                            if (filterValue.includes(itemValueStr)) return false;
                        } else {
                            console.warn(`Operator '${operator}' not directly supported for multi-select field '${filter.field}'. Filter skipped.`);
                            return false;
                        }
                        continue; 
                     }

                     // --- Single Value Handling --- // (Keep existing filter logic)
                     const filterValueStr = String(filterValue || ''); 
                     const valueExists = !(itemValue === null || itemValue === undefined || String(itemValue).trim() === '' || String(itemValue).trim() === '-');

                     if (operator === 'exists') {
                         if (!valueExists) return false;
                         continue; 
                     }
                     if (operator === 'notExists') {
                         if (valueExists) return false; 
                         continue; 
                     }

                     if (!valueExists) {
                        if ((operator === '=' || operator === '!=') && (filterValueStr === '' || filterValueStr === 'null' || filterValueStr === 'undefined')) {
                            const isItemEmpty = !valueExists;
                            const isFilterConsideredEmpty = (filterValueStr === '' || filterValueStr === 'null' || filterValueStr === 'undefined');
                            if (operator === '=' && isItemEmpty !== isFilterConsideredEmpty) return false;
                            if (operator === '!=' && isItemEmpty === isFilterConsideredEmpty) return false;
                            continue; 
                        } else {
                             return false;
                        }
                     }

                     const itemValueStr = String(itemValue).toLowerCase();
                     const filterValueLower = filterValueStr.toLowerCase();
                     const itemNum = parseFloat(itemValue); 
                     const filterNum = parseFloat(filterValueStr); 
                     let numericComparisonDone = false;

                     if (!isNaN(itemNum) && !isNaN(filterNum)) {
                        numericComparisonDone = true;
                        let parsedFilterNum = filterNum; 
                        const format = fieldNumericFormats[filter.field] || 'default';
                        if (['percent', 'million', 'billion'].includes(format)) {
                            const parsedVal = parseFormattedValue(filterValueStr, format);
                            if (!isNaN(parsedVal)) {
                                parsedFilterNum = parsedVal; 
                            } else {
                                console.warn(`Filter skipped: Could not parse filter value '${filterValueStr}' for field '${filter.field}' with format '${format}'.`);
                                return false; 
                            }
                        } 
                         switch (operator) {
                             case '=': if (!(itemNum === parsedFilterNum)) return false; break;
                             case '>': if (!(itemNum > parsedFilterNum)) return false; break;
                             case '<': if (!(itemNum < parsedFilterNum)) return false; break;
                             case '>=': if (!(itemNum >= parsedFilterNum)) return false; break;
                             case '<=': if (!(itemNum <= parsedFilterNum)) return false; break;
                             case '!=': if (!(itemNum !== parsedFilterNum)) return false; break;
                             default: numericComparisonDone = false; 
                         }
                      } else {
                         if (['>', '<', '>=', '<='].includes(operator)) {
                             console.warn(`Numeric comparison operator '${operator}' used, but values are not both numeric: Field='${filter.field}', Item='${itemValue}', Filter='${filterValueStr}'. Filter fails.`);
                             return false; 
                         }
                         numericComparisonDone = false;
                     }

                     if (!numericComparisonDone) {
                        switch(operator) {
                            case '=': if (!(itemValueStr === filterValueLower)) return false; break;
                            case '!=': if (!(itemValueStr !== filterValueLower)) return false; break;
                            case 'contains': if (!itemValueStr.includes(filterValueLower)) return false; break;
                            case 'startsWith': if (!itemValueStr.startsWith(filterValueLower)) return false; break;
                            case 'endsWith': if (!itemValueStr.endsWith(filterValueLower)) return false; break;
                        }
                     }
                 }
                 return true;
             });
        }

         // --- NEW: DataTable Update Logic --- 

         // 1. Determine Columns/Headers based on enabled fields
         const enabledFields = availableFields.filter(field => fieldEnabledStatus[field] === true);
         // Always include Ticker first, consider including 'source' and 'error' if needed?
         // For now, just ticker + enabled processed_data fields.
         const currentHeaders = ['Ticker', ...enabledFields]; 
         console.log("Table Headers:", currentHeaders);

         // 2. Format data for the table (array of arrays)
         const tableData = filteredData.map(item => {
             const row = [item.ticker || 'N/A']; // Start with ticker
             enabledFields.forEach(field => {
                 let value = ''; // Default to empty string
                 let rawValue = null; // <<< Initialize rawValue
                 if (field === 'source') { // Handle 'source' if it becomes enabled
                     rawValue = item.source;
                     value = rawValue || ''; // Use raw value for source directly
                 } else if (item.processed_data && item.processed_data.hasOwnProperty(field)) {
                     rawValue = item.processed_data[field];
                     // Apply formatting for display in the table
                     const format = fieldNumericFormats[field] || 'default';
                     if (fieldMetadata[field]?.type === 'numeric') { 
                        // value = formatNumericValue(rawValue, format); // OLD: Formatted value
                        value = rawValue; // <<< CORRECTED: Push RAW value for numeric types
                     } else {
                         value = (rawValue === null || rawValue === undefined) ? '' : String(rawValue);
                     }
                 }
                 row.push(value);
             });
             // Optionally add error column data
             // if (headers.includes('Error')) {
             //    row.push(item.error || '');
             // }
             return row;
         });

         // 3. Check if DataTable needs destruction/re-initialization (headers changed)
         const headersChanged = JSON.stringify(currentHeaders) !== JSON.stringify(lastAppliedHeaders);
         if (headersChanged && $.fn.dataTable.isDataTable('#output-table')) {
             console.log("Headers changed, destroying existing DataTable.");
             outputDataTable.destroy();
             outputDataTable = null;
             // Clear the tbody and thead manually after destroying
             $('#output-table tbody').empty(); 
             $('#output-table thead tr').empty();
         }
         lastAppliedHeaders = [...currentHeaders]; // Store current headers

         // 4. Update Table Header (if not initialized or headers changed)
         if (!outputDataTable || headersChanged) {
             const theadRow = outputTable.querySelector('thead tr');
             theadRow.innerHTML = ''; // Clear placeholder or old headers
             currentHeaders.forEach(headerText => {
                 const th = document.createElement('th');
                 th.textContent = headerText;

                 // <<< NEW: Add Tooltip Attributes (Step 6) >>>
                 if (headerText !== 'Ticker') { // Don't add tooltips to the 'Ticker' column itself
                     const infoText = fieldInfoTips[headerText] || ''; // Get info text using headerText as field name
                     if (infoText) {
                         th.title = infoText;
                         th.dataset.bsToggle = 'tooltip';
                         th.dataset.bsPlacement = 'top';
                     } else {
                         // Important: Remove attributes if infoText is empty
                         th.removeAttribute('title');
                         th.removeAttribute('data-bs-toggle');
                         th.removeAttribute('data-bs-placement');
                     }
                 }
                 // <<< END NEW >>>

                 theadRow.appendChild(th);
             });
         }

         // 5. Initialize or Update DataTable
         if (outputDataTable) {
             // Table exists, just update data
             console.log("Updating existing DataTable data.");
             outputDataTable.clear();
             outputDataTable.rows.add(tableData);
             outputDataTable.draw();
+            // <<< Force column width recalculation after updating data >>>
+            outputDataTable.columns.adjust().draw(false); // draw(false) prevents resetting paging
         } else {
             // Initialize DataTable for the first time or after destruction
             console.log("Initializing DataTable.");
             try {
                 outputDataTable = $('#output-table').DataTable({
                     data: tableData,
                     // columns: currentHeaders.map(header => ({ title: header })), // Let header update handle titles
                     paging: true,       // Enable pagination
                     searching: true,    // Enable search box
                     lengthChange: true, // Allow user to change number of rows shown
                     pageLength: 50,     // Default number of rows per page
                     scrollX: true,      // Enable horizontal scrolling if needed
                     scrollY: '400px',   // Set vertical scroll height
                     scrollCollapse: true,// Collapse table height when few records
                     destroy: true,      // Allows re-initialization
                     stateSave: false,   // Don't save search/sort state in localStorage (can conflict with our filters)
                     order: [[0, 'asc']], // Default sort by first column (Ticker)
                     language: { // Customize text if needed
                         emptyTable: "No matching records found based on current filters.",
                         zeroRecords: "No matching records found"
                     },
                     // <<< Add dom and buttons configuration >>>
                     dom: "<'row'<'col-sm-12 col-md-6'l><'col-sm-12 col-md-6 d-flex justify-content-end align-items-center'fB>>" +
                          "<'row'<'col-sm-12'tr>>" +
                          "<'row'<'col-sm-12 col-md-5'i><'col-sm-12 col-md-7'p>>", // Add B for Buttons near filter f
                     buttons: [
                        // 'copyHtml5', // OLD
                        // 'excelHtml5', // OLD
                        // 'csvHtml5' // OLD
                        // PDF export excluded as requested
                         {
                            extend: 'copyHtml5',
                            filename: function() { return generateTimestampedFilename('FilteredData_Copy'); },
                            title: function() { return `Filtered Data (${new Date().toLocaleString()})`; } // Optional: Title for copy
                        },
                        {
                            extend: 'excelHtml5',
                            filename: function() { return generateTimestampedFilename('FilteredData'); },
                            title: function() { return `Filtered Data (${new Date().toLocaleString()})`; } // Optional: Title for Excel sheet
                        },
                        {
                            extend: 'csvHtml5',
                            filename: function() { return generateTimestampedFilename('FilteredData'); },
                            title: function() { return `Filtered Data (${new Date().toLocaleString()})`; } // Optional: Title for CSV
                        }
                    ],
                    // <<< NEW: columnDefs for negative number styling >>>
                    columnDefs: currentHeaders.map((headerText, index) => {
                        // Skip 'Ticker' column (index 0)
                        if (index === 0) {
                            return null; // Return null for columns we don't want to define specifically
                        }
                        
                        const fieldName = headerText; // For non-ticker columns, headerText is fieldName
                        const meta = fieldMetadata[fieldName] || {};
                        
                        if (meta.type === 'numeric') {
                            const format = fieldNumericFormats[fieldName] || 'default';
                            return {
                                targets: index, // Target this specific column index
                                render: function (data, type, row) { // 'data' is the cell's original data
                                    // Apply styling only for display rendering
                                    if (type === 'display') {
                                        const rawValue = data; // Use the 'data' parameter directly (should be raw number)
                                        const num = Number(rawValue);
                                        const formattedValue = formatNumericValue(rawValue, format); // <<< CORRECTED: Always format here for display
                                        
                                        // Check if the raw number is negative
                                        if (!isNaN(num) && num < 0) {
                                            return `<span class="text-negative">${formattedValue}</span>`; // Wrap the formatted value
                                        } else {
                                            return formattedValue; // Return formatted value without span
                                        }
                                    }
                                    // For sorting, filtering, type detection etc., return the raw data
                                    return data; // Return original data (raw number or string)
                                }
                            };
                        } else {
                            return null; // Return null for non-numeric columns
                        }
                    }).filter(def => def !== null) // Remove null entries from the array
                    // <<< END columnDefs >>>
                 });
+                // <<< Force column width recalculation after initialization >>>
+                outputDataTable.columns.adjust().draw();
             } catch (error) {
                 console.error("Error initializing DataTable:", error);
                 // Handle initialization error (e.g., display message to user)
                 filterResultsCount.textContent = 'Error displaying results table.';
             }
         }
         // --- END DataTable Update Logic ---

         // --- OLD JSON Output Logic (Removed) ---
         // const dataForDisplay = filteredData.map(item => { ... });
         // outputArea.textContent = JSON.stringify(dataForDisplay, null, 2);
         // --- End OLD JSON Output Logic ---

         filterResultsCount.textContent = `Showing ${filteredData.length} matching records (out of ${fullProcessedData.length}).`; // Count based on filtered records

         // --- Update data used for chart and render --- 
         filteredDataForChart = filteredData;

         // By default, analysis uses filtered data until transformations are applied
         // finalDataForAnalysis = [...filteredDataForChart]; // <<< DO NOT RESET HERE

         // Instead, just update the chart based on the filtered (pre-transform) data
         // The Analyze tab UI will be updated *after* transformations run.
         console.log("ApplyFilters finished. Triggering chart render with pre-transform data.");
         // renderChart(); // <<< REMOVED: Chart should only update after transformations (using finalDataForAnalysis)
         // --- End Chart Update ---

        // <<< NEW: Initialize/Update Tooltips After Table Draw (Step 7 Call) >>>
        initializeTooltips('#output-table thead');
        // <<< END NEW >>>

        console.log("Filter application complete.");
    }
    // --- END applyFilters definition ---

    // --- NEW: Populate Report Field Selector --- 
    function populateReportFieldSelector() {
        console.log("Populating report field selectors (X, Y, Size)...");

        // <<< ADDED: Fetch Post-Transform Settings >>>
        let postTransformEnabledStatus = {};
        // Fetch other settings if needed later (formats, tips)
        const postTransformModule = window.AnalyticsPostTransformModule;
        if (postTransformModule) {
            if (typeof postTransformModule.getPostTransformEnabledStatus === 'function') {
                postTransformEnabledStatus = postTransformModule.getPostTransformEnabledStatus() || {};
            } else { console.warn("[populateReportFieldSelector] getPostTransformEnabledStatus not found on module."); }
            // Add getters for formats/tips here if needed by this function in the future
        } else {
            console.warn("[populateReportFieldSelector] AnalyticsPostTransformModule not found.");
        }
        // <<< END ADDED >>>

        // Determine which field list to use: final post-transform if available, otherwise pre-transform
        // <<< ADDED typeof check >>>
        const useFinalFields = typeof finalAvailableFields !== 'undefined' && Array.isArray(finalAvailableFields) && finalAvailableFields.length > 0;
        const fieldsToUse = useFinalFields ? finalAvailableFields : availableFields;
        // Also get the correct metadata source
        // <<< ADDED typeof check >>>
        const metadataToUse = (typeof finalFieldMetadata !== 'undefined' && Object.keys(finalFieldMetadata).length > 0) ? finalFieldMetadata : fieldMetadata;
        const sourceMsg = useFinalFields ? "post-transform" : "pre-transform";
        console.log(`Using ${sourceMsg} fields:`, fieldsToUse);

        const xSelector = reportXAxisSelector;
        const ySelector = reportFieldSelector;

        if (!xSelector || !ySelector || !reportSizeSelector) {
            console.error("Report axis selectors not found!");
            return;
        }

        // <<< Assign to existing variables BEFORE clearing selectors >>>
        previousXValue = xSelector.value;
        previousYValue = ySelector.value; 
        previousSizeValue = reportSizeSelector.value; 

        // Clear existing options
        xSelector.innerHTML = '<option value="index" selected>-- Record Index (Default) --</option>';
        ySelector.innerHTML = '<option value="" selected>-- Select Field --</option>';
        reportSizeSelector.innerHTML = '<option value="" selected>-- Select Field --</option>';

        // <<< FIX: Check fieldsToUse instead of fields >>>
        if (!fieldsToUse || fieldsToUse.length === 0) { 
            console.log(`No available fields (${sourceMsg}) to populate report selectors.`); // Log source
            return;
        }

        // Filter available fields based on enabled status (still use the single shared status)
        // <<< FIX: Filter fieldsToUse instead of fields >>>
        // const enabledFields = fieldsToUse.filter(field => fieldEnabledStatus[field] !== false); // Use !== false to include undefined/true // <<< OLD: Uses pre-transform status
        const enabledFields = fieldsToUse.filter(field => postTransformEnabledStatus[field] !== false); // <<< NEW: Use post-transform status
        console.log(`Populating report selectors with enabled (${sourceMsg}) fields:`, enabledFields);

        // --- Separate check for numeric fields (optional, but helpful) ---
        const numericFields = enabledFields.filter(field => metadataToUse[field]?.type === 'numeric' || field === 'index'); // Treat 'index' as numeric conceptually
        console.log("Numeric fields (post-transform) identified for selectors:", numericFields);
        
        // Populate X, Y, and Size selectors
        enabledFields.forEach(field => {
            // Y-axis option
            const optionY = document.createElement('option');
            optionY.value = field; // Use actual field name (lowercase ticker)
            optionY.textContent = (field === 'ticker') ? 'Ticker' : field; // Display uppercase Ticker
            ySelector.appendChild(optionY);
            
            // X-axis option
            const optionX = document.createElement('option');
            optionX.value = field; // Use actual field name
            optionX.textContent = (field === 'ticker') ? 'Ticker' : field; // Display uppercase Ticker
            xSelector.appendChild(optionX);

            // Size-axis option (Only add numeric fields)
            // Check if the field type is numeric according to post-transform metadata
            if (metadataToUse[field]?.type === 'numeric') {
                const optionSize = document.createElement('option');
                optionSize.value = field;
                optionSize.textContent = field;
                reportSizeSelector.appendChild(optionSize);
            }
        });

        // Try to restore previous selections (if they weren't the default index)
        if (enabledFields.includes(previousYValue)) {
            ySelector.value = previousYValue;
        }
        // Restore X only if it's an enabled field and not the default 'index'
        if (previousXValue && previousXValue !== 'index' && enabledFields.includes(previousXValue)) {
            xSelector.value = previousXValue;
        } else {
            // Otherwise, ensure the default 'index' option is selected
            xSelector.value = 'index';
        }

        // Restore Size selector if possible
        if (previousSizeValue && metadataToUse[previousSizeValue]?.type === 'numeric' && enabledFields.includes(previousSizeValue)) {
            reportSizeSelector.value = previousSizeValue;
        } else {
            reportSizeSelector.value = ""; // Reset if previous not valid/numeric/enabled
        }

        // If previous Y selection is no longer valid, reset chart
        if (previousYValue && !enabledFields.includes(previousYValue)) {
            if (reportChartInstance) {
                reportChartInstance.destroy();
                reportChartInstance = null;
            }
            if (chartStatus) chartStatus.textContent = 'Select fields to generate the chart.';
        }
    }
    // --- END Populate Report Field Selector ---

    // --- NEW: Populate Report Color Selector --- 
    function populateReportColorSelector() {
        console.log("Populating report color selector...");

        // <<< ADDED: Fetch Post-Transform Settings >>>
        let postTransformEnabledStatus = {};
        // Fetch other settings if needed later (formats, tips)
        const postTransformModule = window.AnalyticsPostTransformModule;
        if (postTransformModule) {
            if (typeof postTransformModule.getPostTransformEnabledStatus === 'function') {
                postTransformEnabledStatus = postTransformModule.getPostTransformEnabledStatus() || {};
            } else { console.warn("[populateReportColorSelector] getPostTransformEnabledStatus not found on module."); }
            // Add getters for formats/tips here if needed by this function in the future
        } else {
            console.warn("[populateReportColorSelector] AnalyticsPostTransformModule not found.");
        }
        // <<< END ADDED >>>

        // Determine which field list to use
        // <<< ADDED typeof check >>>
        const useFinalFields = typeof finalAvailableFields !== 'undefined' && Array.isArray(finalAvailableFields) && finalAvailableFields.length > 0;
        const fieldsToUse = useFinalFields ? finalAvailableFields : availableFields;
        // <<< ADDED typeof check >>>
        const metadataToUse = (typeof finalFieldMetadata !== 'undefined' && Object.keys(finalFieldMetadata).length > 0) ? finalFieldMetadata : fieldMetadata; // Keep metadata check consistent
        const sourceMsg = useFinalFields ? "post-transform" : "pre-transform";
        console.log(`Using ${sourceMsg} fields/metadata for color selector:`, fieldsToUse);

        if (!reportColorSelector) return;

        const previousValue = reportColorSelector.value;
        reportColorSelector.innerHTML = '<option value="">-- No Color Variation --</option>'; // Clear and add default
 
        if (!fieldsToUse || fieldsToUse.length === 0) {
            console.log("No available fields (post-transform) to populate color selector.");
            return;
        }
 
        // Filter for enabled fields (using shared status)
        // const enabledFields = fieldsToUse.filter(field => fieldEnabledStatus[field] !== false); // Check !== false // <<< OLD: Uses pre-transform status
        const enabledFields = fieldsToUse.filter(field => postTransformEnabledStatus[field] !== false); // <<< NEW: Use post-transform status
        console.log("Populating color selector with enabled (post-transform) fields:", enabledFields);
 
        // Sort fields alphabetically
        enabledFields.sort();
 
        // Populate selector (allow non-numeric fields for color)
        enabledFields.forEach(field => {
            const option = document.createElement('option');
            option.value = field; // Use actual field name (lowercase ticker)
            option.textContent = (field === 'ticker') ? 'Ticker' : field; // Display uppercase Ticker
            reportColorSelector.appendChild(option);
        });

        // Set default to 'name' if available, otherwise restore previous or set default
        const defaultColorFieldLower = 'name'; // Define the desired default in lowercase

        // Find the actual field name (case-insensitive) if it exists and is enabled
        // <<< Update to handle lowercase 'ticker' correctly >>>
        const nameFieldActualCase = enabledFields.find(f => f.toLowerCase() === defaultColorFieldLower && f !== 'ticker'); // Prefer non-ticker name
        const tickerFieldExists = enabledFields.includes('ticker');
        const isNameFieldAvailableAndEnabled = enabledFields.some(f => f.toLowerCase() === defaultColorFieldLower);
        
        console.log(`[populateReportColorSelector] Found 'name' field with actual case: ${nameFieldActualCase}`);
        console.log(`[populateReportColorSelector] Ticker field exists: ${tickerFieldExists}`);

        // Prioritize restoring previous valid selection
        if (previousValue && enabledFields.includes(previousValue)) {
            reportColorSelector.value = previousValue;
            console.log(`[populateReportColorSelector] Restoring previous value: ${previousValue}`);
        }
        // Else if 'name' field exists (and wasn't the restored previous value), set to name
        else if (nameFieldActualCase && previousValue !== nameFieldActualCase) {
            reportColorSelector.value = nameFieldActualCase;
            console.log(`[populateReportColorSelector] Setting default value to actual case of 'name': ${nameFieldActualCase}`);
        }
        // Else if 'ticker' exists (and wasn't the restored previous value)
        else if (tickerFieldExists && previousValue !== 'ticker') { 
            reportColorSelector.value = 'ticker'; // Use lowercase value
            console.log(`[populateReportColorSelector] 'name' not found/restored, setting default to 'ticker'.`);
        }
        // Else (no valid previous, no name, no ticker), set to empty default
        else {
            reportColorSelector.value = "";
            console.log(`[populateReportColorSelector] No default ('name' or 'ticker') or previous value found, setting to empty.`);
        }
        // <<< END Update to handle lowercase 'ticker' correctly >>>

        // --- OLD DEFAULT/RESTORE LOGIC ---
        /*
        if (isNameFieldAvailableAndEnabled) { // <<< NEW CHECK: Use the case-insensitive result
            // If previous value was valid and *not* the default 'name' (case-insensitive check), restore it
            if (previousValue && previousValue.toLowerCase() !== defaultColorFieldLower && enabledFields.includes(previousValue)) { // CHECK 2 (includes() check is fine here as previousValue holds the exact case)
                reportColorSelector.value = previousValue;
                console.log(`[populateReportColorSelector] Restoring previous value: ${previousValue}`);
            } else {
                // Otherwise, set to the desired default 'name' using the actual case found
                // Make sure nameFieldActualCase is not null/undefined before assigning
                if (nameFieldActualCase) {
                    reportColorSelector.value = nameFieldActualCase;
                    console.log(`[populateReportColorSelector] Setting default value to actual case: ${nameFieldActualCase}`);
                } else {
                     // Fallback if somehow nameFieldActualCase is null despite isNameFieldAvailableAndEnabled being true (shouldn't happen)
                     reportColorSelector.value = ""; 
                     console.log(`[populateReportColorSelector] 'name' field was reported available but not found with specific case. Setting to empty.`);
                }
            }
        } else if (previousValue && enabledFields.includes(previousValue)) { // CHECK 3: 'name' not enabled, but previous valid value exists?
            // If 'name' isn't available, but previous value is, restore it
            reportColorSelector.value = previousValue;
            console.log(`[populateReportColorSelector] 'name' not found/enabled, restoring previous value: ${previousValue}`);
        } else {
            // Otherwise (no 'name', no valid previous), set to empty default
            reportColorSelector.value = "";
            console.log(`[populateReportColorSelector] No default or previous value found, setting to empty.`);
        }
        */
        // --- END OLD LOGIC ---
        console.log(`[populateReportColorSelector] Final value set: ${reportColorSelector.value}`);
    }
    // --- END Populate Report Color Selector ---

    // --- RENAMED: Render Chart (previously Render Scatter Plot) --- 
    function renderChart() {
        const canvas = document.getElementById('report-chart-canvas');
        const ctx = canvas?.getContext('2d');
        const statusElement = document.getElementById('chart-status');

        // <<< ADDED: Fetch Post-Transform Settings >>>
        let localPlotDataForSearch = [];
        let postTransformEnabledStatus = {};
        let postTransformNumericFormats = {};
        let postTransformFieldInfoTips = {};
        const postTransformModule = window.AnalyticsPostTransformModule;

        if (postTransformModule) {
            if (typeof postTransformModule.getPostTransformEnabledStatus === 'function') {
                postTransformEnabledStatus = postTransformModule.getPostTransformEnabledStatus() || {};
            } else { console.warn("[renderChart] getPostTransformEnabledStatus not found on module."); }

            if (typeof postTransformModule.getPostTransformNumericFormats === 'function') {
                postTransformNumericFormats = postTransformModule.getPostTransformNumericFormats() || {};
            } else { console.warn("[renderChart] getPostTransformNumericFormats not found on module."); }

            if (typeof postTransformModule.getPostTransformInfoTips === 'function') {
                postTransformFieldInfoTips = postTransformModule.getPostTransformInfoTips() || {};
            } else { console.warn("[renderChart] getPostTransformInfoTips not found on module."); }
        } else {
            console.warn("[renderChart] AnalyticsPostTransformModule not found.");
        }

        // Also fetch pre-transform formats/tips for fallback if needed within renderChart
        const mainModule = window.AnalyticsMainModule;
        const preTransformNumericFormats = mainModule?.getNumericFieldFormats ? mainModule.getNumericFieldFormats() : {};
        const preTransformFieldInfoTips = mainModule?.getFieldInfoTips ? mainModule.getFieldInfoTips() : {};
        // <<< END ADDED >>>

        if (!canvas || !ctx || !statusElement) {
            console.error('Chart canvas, context, or status element not found!');
            return;
        }

        // --- NEW: Check if post-transformation data is ready --- 
        if (!finalDataForAnalysis || !Array.isArray(finalDataForAnalysis) || finalDataForAnalysis.length === 0 || !finalFieldMetadata || Object.keys(finalFieldMetadata).length === 0) {
            console.log('renderChart: Post-transformation data (finalDataForAnalysis / finalFieldMetadata) not ready. Clearing chart.');
            statusElement.textContent = 'Load data and run transformations to generate the chart.';
            // Destroy existing chart instance if it exists
            if (reportChartInstance) { // <<< CORRECTED VARIABLE NAME
                reportChartInstance.destroy(); // <<< CORRECTED VARIABLE NAME
                reportChartInstance = null; // <<< CORRECTED VARIABLE NAME
                console.log('Previous chart instance destroyed.');
            }
            // Optionally clear the canvas explicitly, though destroy usually handles it
            // ctx.clearRect(0, 0, canvas.width, canvas.height);
            return; // Exit function early
        }
        // --- END NEW CHECK --- 

        const xAxisField = reportXAxisSelector.value || 'index';
        const yAxisField = reportFieldSelector.value;

        console.log("Rendering chart..."); 
        // <<< LOG THE DATA SOURCE AT THE START >>>
        if (finalDataForAnalysis && finalDataForAnalysis.length > 0) {
           // console.log("RenderChart using finalDataForAnalysis. First item:", JSON.parse(JSON.stringify(finalDataForAnalysis[0]))); 
           // if (!finalDataForAnalysis[0]?.processed_data?.['test 2']) { 
           //     console.warn("WARNING: 'test 2' field is MISSING from first item in finalDataForAnalysis at start of renderChart!"); 
           // }
           // --- REMOVE THE WARNING LINE DIRECTLY ---
           // console.warn("WARNING: 'test 2' field is MISSING from first item in finalDataForAnalysis at start of renderChart!"); 

           if (finalDataForAnalysis.length > 0 && finalDataForAnalysis[0]) {
                // Simple log showing keys for the first item
                console.log("RenderChart using finalDataForAnalysis. First item keys:", Object.keys(finalDataForAnalysis[0]), "Processed_data keys:", Object.keys(finalDataForAnalysis[0].processed_data || {}));
                
            } else {
                console.log("RenderChart called with empty or missing finalDataForAnalysis.");
            }
        } else {
            console.log("RenderChart called with empty or missing finalDataForAnalysis.");
        }

        if (!reportFieldSelector || !reportXAxisSelector || !reportChartCanvas || !chartStatus || !reportColorSelector || !reportSizeSelector || !reportChartTypeSelector) { // Added Size selector check
            console.error("Report tab elements not found.");
            return;
        }

        // --- Declare variables at the top --- 
        let plotData = []; 
        const labels = []; 
        const numericData = []; 
        const pointBackgroundColors = [];
        const pointBorderColors = [];
        let nonNumericXCount = 0; // Track non-numeric X (only if X field is selected)
        let nonNumericYCount = 0; // Track non-numeric Y
        let nonNumericSizeCount = 0; // Track non-numeric/non-positive Size
        let missingItemCount = 0; // Track missing items
        // --- End variable declarations --- 

        // <<< NEW: Reset chart data used for searching >>>
        // localPlotDataForSearch = []; // <<< REMOVED: Redundant reassignment

        // Helper function to get value from item, prioritizing processed_data
        const getValue = (item, field) => {
            if (!item) return null;

            // <<< ADD Ticker Debugging >>>
            let valueFound = null;
            let foundLocation = null;

            // 1. Check inside processed_data (where original AND synthetic fields live)
            if (item.processed_data && item.processed_data.hasOwnProperty(field)) {
                // return item.processed_data[field]; // OLD RETURN
                valueFound = item.processed_data[field];
                foundLocation = 'processed_data';
            }
            // 2. If not found in processed_data, check top-level fields
            // <<< Use simple property check instead of hasOwnProperty >>>
            else if (item[field] !== undefined && field !== 'processed_data') {
                 // console.debug(`[getValue] Field '${field}' found at top level.`); // Keep debug for non-ticker?
                 // return item[field]; // OLD RETURN
                 valueFound = item[field];
                 foundLocation = 'top-level';
            }

            // <<< ADD Ticker Debugging LOG >>>
            if (field === 'Ticker') {
                // <<< Update log to reflect new check >>>
                console.log(`[getValue DEBUG for Ticker] Checking field '${field}' on item:`, item); // Log the item object
                try {
                    console.log(`[getValue DEBUG for Ticker] Object.keys(item):`, Object.keys(item)); // Log keys
                } catch (e) {
                    console.log(`[getValue DEBUG for Ticker] Error getting Object.keys(item):`, e);
                }
                console.log(`[getValue DEBUG for Ticker] Checked processed_data: ${item.processed_data?.hasOwnProperty('Ticker') ? 'Found' : 'Not Found'}. Checked top-level (item["Ticker"] !== undefined): ${item['Ticker'] !== undefined ? 'Found' : 'Not Found'}. Found Location: ${foundLocation || 'None'}. Value Returning:`, valueFound);
            }

            // 3. Return the found value or null if not found anywhere
            return valueFound;
            // <<< END Ticker Debugging >>>
        };

        const selectedYField = reportFieldSelector.value;
        const selectedXField = reportXAxisSelector.value; // NEW: Get X-axis field
        const colorField = reportColorSelector.value; 
        const sizeField = reportSizeSelector.value; // NEW: Get size field
        const selectedChartType = reportChartTypeSelector.value;
        const useIndexAsX = (!selectedXField || selectedXField === 'index'); // Check if default X is used
        // <<< USE POST-TRANSFORM METADATA if available, otherwise pre-transform >>>
        const currentMetadata = (finalFieldMetadata && Object.keys(finalFieldMetadata).length > 0) ? finalFieldMetadata : fieldMetadata;
        console.log(`Selected X:${useIndexAsX ? 'Index' : selectedXField}, Y:${selectedYField}, Color:${colorField || '-'}, Size:${sizeField || '-'}, Type:${selectedChartType}`);

        chartStatus.textContent = ''; // Clear previous status

        // --- Disable X-axis selector for bar charts ---
        if (selectedChartType === 'bar') {
            reportXAxisSelector.disabled = true;
            // Optionally reset X-axis selection for bar? Or just ignore it? Let's ignore it for now.
            // reportXAxisSelector.value = ''; // Reset
        } else {
            reportXAxisSelector.disabled = false;
        }
        // --- End Disable X-axis ---

        // --- Enable/Disable Size selector based on chart type ---
        reportSizeSelector.disabled = (selectedChartType !== 'bubble');
        // --- End Enable/Disable Size ---

        // Need Y field selected for all chart types
        if (!selectedYField) {
            chartStatus.textContent = 'Please select a field for the Y axis.';
            if (reportChartInstance) {
                reportChartInstance.destroy();
                reportChartInstance = null;
            }
            return;
        }

        // Need Size field selected *only* for bubble charts
        if (selectedChartType === 'bubble' && !sizeField) {
            chartStatus.textContent = 'Please select a field for Bubble Size.';
            if (reportChartInstance) {
                reportChartInstance.destroy();
                reportChartInstance = null;
            }
            return;
        }

        if (!finalDataForAnalysis || finalDataForAnalysis.length === 0) { 
            chartStatus.textContent = 'No data available to plot (apply filters or load data).';
            if (reportChartInstance) {
                reportChartInstance.destroy();
                reportChartInstance = null;
            }
            return;
        }

        // --- Pre-calculate min/max for size scaling (Bubble only) ---
        let minSize = Infinity;
        let maxSize = -Infinity;
        if (selectedChartType === 'bubble' && sizeField) {
            // <<< USE finalDataForAnalysis >>>
            finalDataForAnalysis.forEach(item => { 
                if (item) {
                    const sizeValue = getValue(item, sizeField);
                    const numericSize = Number(sizeValue);
                    // Consider only positive numeric values for size scaling
                    if (!isNaN(numericSize) && numericSize > 0) {
                        if (numericSize < minSize) minSize = numericSize;
                        if (numericSize > maxSize) maxSize = numericSize;
                    }
                }
            });
            // Handle case where no valid positive sizes found
            if (minSize === Infinity || maxSize === -Infinity) {
                console.warn(`No valid positive numeric data found for size field '${sizeField}'. Bubbles will have default size.`);
                // Set defaults to prevent division by zero later
                minSize = 1;
                maxSize = 1;
            } else if (minSize === maxSize) {
                 // If all valid sizes are the same, adjust slightly for scaling formula
                 minSize = maxSize / 2;
            }
            console.log(`[Bubble Scaling] Min Size: ${minSize}, Max Size: ${maxSize} for field '${sizeField}'`);
        }
        // --- End Pre-calculation ---

        // Color mapping logic (remains the same)
        const colorMap = {};
        const predefinedColors = [
            'rgba(255, 99, 132, 0.7)', 'rgba(54, 162, 235, 0.7)', 'rgba(255, 206, 86, 0.7)',
            'rgba(75, 192, 192, 0.7)', 'rgba(153, 102, 255, 0.7)', 'rgba(255, 159, 64, 0.7)',
            'rgba(199, 199, 199, 0.7)', 'rgba(83, 102, 255, 0.7)', 'rgba(40, 159, 64, 0.7)',
            'rgba(210, 99, 132, 0.7)'
        ];
        let colorIndex = 0;
        const defaultColor = 'rgba(128, 128, 128, 0.6)';
        const getColorForValue = (value) => {
            if (value === null || value === undefined || value === '') return defaultColor;
            const valueStr = String(value);
            if (!colorMap[valueStr]) {
                colorMap[valueStr] = predefinedColors[colorIndex % predefinedColors.length];
                colorIndex++;
            }
            return colorMap[valueStr];
        };
        
        // Process data points
        // Reset counts
        nonNumericXCount = 0;
        nonNumericYCount = 0;
        nonNumericSizeCount = 0; // Reset size count too
        missingItemCount = 0;
        
        // <<< USE finalDataForAnalysis >>>
        finalDataForAnalysis.forEach((item, index) => { 
            // <<< START DEBUG LOGGING >>>
            if (index < 5) { // Log first 5 items
                console.log(`--- Chart Processing Item ${index} (Ticker: ${item?.ticker}) ---`);
                console.log("Processed Data Object:", item?.processed_data);
                const rawX = getValue(item, selectedXField);
                const rawY = getValue(item, selectedYField);
                const numX = Number(rawX);
                const numY = Number(rawY);
                const isXNumeric = !isNaN(numX) && rawX !== null && String(rawX).trim() !== '';
                const isYNumeric = !isNaN(numY) && rawY !== null && String(rawY).trim() !== '';
                console.log(`X Field ('${selectedXField}'): Raw='${rawX}', Num=${numX}, isNumeric=${isXNumeric}`);
                console.log(`Y Field ('${selectedYField}'): Raw='${rawY}', Num=${numY}, isNumeric=${isYNumeric}`);
            }
            // <<< END DEBUG LOGGING >>>
 
             let pointColor = defaultColor;
             let colorValue = null;
 
             if (item) {
                 // Get Y-axis value
                 const rawYValue = getValue(item, selectedYField);
                 const numericYValue = Number(rawYValue);
                 const isYNumeric = !isNaN(numericYValue) && rawYValue !== null && String(rawYValue).trim() !== '';
                 
                 // Get X-axis value (only if needed for scatter/line)
                 let rawXValue = null;
                 let numericXValue = null;
                 let isXNumeric = false;
                 // Determine X value based on selection or default index
                 if (selectedChartType === 'scatter' || selectedChartType === 'line' || selectedChartType === 'bubble') { // Include bubble
                     if (useIndexAsX) {
                         // Use the loop index as the X value
                         numericXValue = index;
                         rawXValue = index; // Store index as raw value too for consistency
                         isXNumeric = true; // Index is always numeric
                     } else {
                         // Use the selected field for X value
                         rawXValue = getValue(item, selectedXField);
                         numericXValue = Number(rawXValue);
                         isXNumeric = !isNaN(numericXValue) && rawXValue !== null && String(rawXValue).trim() !== '';
                     }
                 }

                 // Check if data is valid for the *specific chart type*
                 let isValidPoint = false;
                 if (selectedChartType === 'scatter' || selectedChartType === 'line') {
                     isValidPoint = isXNumeric && isYNumeric;
                     if (!isXNumeric) nonNumericXCount++;
                     if (!isYNumeric) nonNumericYCount++;
                 } else if (selectedChartType === 'bar') {
                     isValidPoint = isYNumeric; // Only Y needs to be numeric for bar
                     if (!isYNumeric) nonNumericYCount++;
                 } else if (selectedChartType === 'bubble') {
                     // Bubble needs valid X, Y, and Size
                     const rawSizeValue = getValue(item, sizeField);
                     const numericSizeValue = Number(rawSizeValue);
                     const isSizeNumericPositive = !isNaN(numericSizeValue) && numericSizeValue > 0;

                     isValidPoint = isXNumeric && isYNumeric && isSizeNumericPositive;

                     // Increment specific counts for bubble exclusions
                     if (!isXNumeric) nonNumericXCount++;
                     if (!isYNumeric) nonNumericYCount++;
                     if (!isSizeNumericPositive) nonNumericSizeCount++;
                 }

                 if (isValidPoint) {
                     // Get color field value
                     if (colorField) {
                         colorValue = getValue(item, colorField);
                         pointColor = getColorForValue(colorValue);
                     } else {
                         pointColor = predefinedColors[0]; // Default if no color field
                     }

                     // Add data based on chart type
                     if (selectedChartType === 'scatter' || selectedChartType === 'line') {
                         const dataPoint = {
                             x: numericXValue, // Use the actual numeric X value
                             y: numericYValue,
                             ticker: item.ticker || 'N/A',
                             colorValue: colorValue,
                             originalX: rawXValue, // Store original values for tooltip if needed
                             originalY: rawYValue
                         };
                         plotData.push(dataPoint); 
                     } else if (selectedChartType === 'bar') {
                         labels.push(item.ticker || `Index ${index}`); // X is the ticker/label
                         numericData.push(numericYValue); // Y is the bar height
                         // Bar colors are pushed later, need to store color value
                         // We'll handle this by iterating over labels/numericData later
                     } else if (selectedChartType === 'bubble') {
                         const rawSizeValue = getValue(item, sizeField);
                         const numericSizeValue = Number(rawSizeValue);
                         // Scale radius (ensure min/max are valid)
                         const minRadius = 5;
                         const maxRadius = 30;
                         let radius = minRadius; // Default size if scaling fails
                         if (maxSize > minSize) { // Avoid division by zero
                             radius = minRadius + ((numericSizeValue - minSize) / (maxSize - minSize)) * (maxRadius - minRadius);
                         }
                         radius = Math.max(minRadius, radius); // Ensure minimum radius

                         const dataPoint = {
                             x: numericXValue,
                             y: numericYValue,
                             r: radius, // Calculated radius
                             ticker: item.ticker || 'N/A',
                             colorValue: colorValue,
                             originalX: rawXValue,
                             originalY: rawYValue,
                             originalSize: rawSizeValue // Store original size value for tooltip
                         };
                         plotData.push(dataPoint);
                     }

                     // Push colors (only needed for scatter/line here, bar handled later)
                      if (selectedChartType !== 'bar') {
                          pointBackgroundColors.push(pointColor);
                          pointBorderColors.push(pointColor.replace(/0\.\d+\)/, '1)')); 
                      }

                 } 
                 // No else block needed here for invalid points, counts incremented above

             } else {
                  missingItemCount++; // Missing item entirely
             }
         });
         
         // --- Handle Bar Chart Colors ---
          if (selectedChartType === 'bar') {
              // We need to iterate through the generated labels/numericData
              // and find the corresponding original item to get the color value
              labels.forEach((label, index) => {
                  // Find the original item - this assumes labels are unique tickers or unique indices
                  // <<< USE finalDataForAnalysis >>>
                  const originalItem = finalDataForAnalysis.find(item => 
                      (item && item.ticker === label) || (item && !item.ticker && label === `Index ${index}`)
                  );
                  let pointColor = defaultColor;
                  if (originalItem && colorField) {
                      const colorValue = getValue(originalItem, colorField);
                      pointColor = getColorForValue(colorValue);
                  } else if (originalItem) {
                      pointColor = predefinedColors[0]; // Default if no color field
                  }
                  pointBackgroundColors.push(pointColor);
                  pointBorderColors.push(pointColor.replace(/0\.\d+\)/, '1)'));
              });
          }
         // --- End Bar Chart Colors ---

         // Check if any valid data points were found for the selected type
         console.log('Type of plotData before hasData:', typeof plotData, 'Value:', plotData); // DEBUG LOG
         const hasData = (selectedChartType === 'bar' ? numericData.length > 0 : (plotData && plotData.length > 0)); // Simplified check
         if (!hasData) {
             chartStatus.textContent = `No valid numeric data found for the selected axes (${selectedXField ? 'X:'+selectedXField+', ' : ''}Y:${selectedYField}) in the current filtered data.`;
              if (reportChartInstance) {
                 reportChartInstance.destroy();
                 reportChartInstance = null;
             }
             return;
         }
         
         // Construct status message about excluded points
         let excludedMessages = [];
         if (missingItemCount > 0) excludedMessages.push(`${missingItemCount} missing records`);
         // Only report non-numeric X for scatter/line
         // Only report non-numeric X if a specific field (not index) was selected
         if (!useIndexAsX && (selectedChartType === 'scatter' || selectedChartType === 'line') && nonNumericXCount > 0) {
             excludedMessages.push(`${nonNumericXCount} non-numeric X values ('${selectedXField}')`);
         }
          if (nonNumericYCount > 0) { // Relevant for all types
              excludedMessages.push(`${nonNumericYCount} non-numeric Y values ('${selectedYField}')`);
          }
          if (selectedChartType === 'bubble' && nonNumericSizeCount > 0) {
              excludedMessages.push(`${nonNumericSizeCount} invalid Size values ('${sizeField}')`);
          }

         const plottedCount = hasData ? (selectedChartType === 'bar' ? numericData.length : plotData.length) : 0;
         if (excludedMessages.length > 0) {
             chartStatus.textContent = `Plotting ${plottedCount} points. ${excludedMessages.join(', ')} were excluded.`;
         } else {
             chartStatus.textContent = `Plotting ${plottedCount} points.`;
         }

         // const ctx = reportChartCanvas.getContext('2d'); // <<< REMOVED: ctx is already defined at the top

         // Destroy previous chart instance if it exists
         if (reportChartInstance) {
             reportChartInstance.destroy();
         }

         // Determine data structure based on chart type
         let chartDataConfig;
         let xAxisConfig; // Define X-axis config separately

         if (selectedChartType === 'scatter' || selectedChartType === 'line') {
             chartDataConfig = {
                 datasets: [{
                     label: `${selectedYField} vs ${selectedXField}`, // Combined label
                     data: plotData, // Use {x, y, ...} data
                     backgroundColor: pointBackgroundColors,
                     borderColor: pointBorderColors,
                     pointRadius: selectedChartType === 'scatter' ? 5 : 3, 
                     pointHoverRadius: selectedChartType === 'scatter' ? 7 : 5,
                     borderWidth: selectedChartType === 'line' ? 2 : 1, 
                     fill: selectedChartType === 'line' ? false : undefined, 
                     tension: selectedChartType === 'line' ? 0.1 : undefined 
                 }]
             };
             xAxisConfig = { // Define X-axis for scatter/line
                  title: {
                      display: true,
                      text: useIndexAsX ? 'Record Index' : selectedXField // Use selected X field name or default
                  },
                  type: 'linear', // Numeric axis
                  position: 'bottom'
             };
         } else if (selectedChartType === 'bar') {
             chartDataConfig = {
                 labels: labels, // Use ticker labels for categories
                 datasets: [{
                     label: selectedYField, // Y field is the value
                     data: numericData, // Use numeric array for bar heights
                     backgroundColor: pointBackgroundColors, // Use generated colors
                     borderColor: pointBorderColors,
                     borderWidth: 1
                 }]
             };
             xAxisConfig = { // Define X-axis for bar
                  title: {
                      display: true,
                      text: 'Ticker / Record' // Generic label for categories
                  },
                  type: 'category', // Categorical axis
                  position: 'bottom'
             };
         } else if (selectedChartType === 'bubble') {
             chartDataConfig = {
                 datasets: [{
                     label: `${selectedYField} vs ${selectedXField}`,
                     data: plotData,
                     backgroundColor: pointBackgroundColors,
                     borderColor: pointBorderColors,
                     borderWidth: 1
                 }]
             };
             xAxisConfig = {
                 title: {
                     display: true,
                     text: useIndexAsX ? 'Record Index' : selectedXField
                 },
                 type: 'linear',
                 position: 'bottom'
             };
         } else {
              console.error(`Unsupported chart type: ${selectedChartType}`);
              chartStatus.textContent = `Unsupported chart type selected: ${selectedChartType}`;
              return;
         }

         // Create the chart
         reportChartInstance = new Chart(ctx, {
             type: selectedChartType, // Use selected type
             data: chartDataConfig, // Use the prepared config
             options: {
                 responsive: true,
                 maintainAspectRatio: false,
                 scales: {
                     x: xAxisConfig, // Use the defined X-axis config
                     y: {
                         // <<< Apply formatting to Y-axis ticks >>>
                         ticks: {
                             callback: function(value, index, ticks) {
                                 // Get format for the Y field
                                 const yField = reportFieldSelector.value;
                                 // const format = (yField && fieldNumericFormats[yField]) ? fieldNumericFormats[yField] : 'default'; // OLD: Uses only pre-transform
                                 // <<< NEW: Prioritize post-transform format >>>
                                 const format = (yField && postTransformNumericFormats.hasOwnProperty(yField))
                                                  ? postTransformNumericFormats[yField]
                                                  : ((yField && preTransformNumericFormats[yField]) ? preTransformNumericFormats[yField] : 'default');
                                 return formatNumericValue(value, format);
                             }
                         },
                         title: {
                             display: true,
                             text: selectedYField // Y-axis label is the selected Y field
                         }
                     }
                 },
                 plugins: {
                     legend: {
                         display: true, 
                         position: 'top',
                     },
                     tooltip: {
                         callbacks: {
                             label: function(context) {
                                 // console.log("Tooltip callback executed."); // Reduced log frequency
                                 let labelLines = [];
                                 const chartType = context.chart.config.type;
                                 const pointData = context.raw;
                                 const yField = reportFieldSelector.value; // Get Y field name
                                 const xField = reportXAxisSelector.value; // Get X field name
                                 const useIndexAsX = (!xField || xField === 'index'); // Check if default X is used
                                 const colorField = reportColorSelector.value; // Get color field name
                                 const sizeField = reportSizeSelector.value; // Get size field name

                                 if (chartType === 'bar') {
                                     labelLines.push(`Ticker: ${context.label || 'N/A'}`);
                                     // Prioritize post-transform format for Y
                                     const yFormat = (yField && postTransformNumericFormats.hasOwnProperty(yField))
                                                       ? postTransformNumericFormats[yField]
                                                       : ((yField && preTransformNumericFormats[yField]) ? preTransformNumericFormats[yField] : 'default');
                                     const formattedY = formatNumericValue(context.parsed.y, yFormat);
                                     labelLines.push(`${yField || 'Value'}: ${formattedY} (Raw: ${context.parsed.y})`);

                                     // Add color field info for bar charts
                                     if (colorField) {
                                         const dataIndex = context.dataIndex;
                                         const originalItem = finalDataForAnalysis.find(item =>
                                             (item && item.ticker === context.label) || (item && !item.ticker && context.label === `Index ${dataIndex}`)
                                         );
                                         if (originalItem) {
                                             const colorValue = getValue(originalItem, colorField);
                                             if (colorValue !== null && colorValue !== undefined) {
                                                  labelLines.push(`${colorField}: ${colorValue}`);
                                             }
                                         }
                                     }
                                 } else if (pointData) {
                                     // Scatter/Line/Bubble chart
                                     if (pointData.ticker) {
                                         labelLines.push(`Ticker: ${pointData.ticker}`);
                                     }

                                     const xLabel = useIndexAsX ? 'Index' : xField;
                                     const yLabel = yField;

                                     // Format X value (Prioritize post-transform)
                                     const xFormat = (xField && postTransformNumericFormats.hasOwnProperty(xField))
                                                       ? postTransformNumericFormats[xField]
                                                       : ((xField && preTransformNumericFormats[xField]) ? preTransformNumericFormats[xField] : 'default');
                                     const formattedX = useIndexAsX ? pointData.x : formatNumericValue(pointData.x, xFormat);

                                     // Format Y value (Prioritize post-transform)
                                     const yFormatTooltip = (yLabel && postTransformNumericFormats.hasOwnProperty(yLabel))
                                                       ? postTransformNumericFormats[yLabel]
                                                       : ((yLabel && preTransformNumericFormats[yLabel]) ? preTransformNumericFormats[yLabel] : 'default');
                                     const formattedYTooltip = formatNumericValue(pointData.y, yFormatTooltip);

                                     // Push X and Y lines ONCE
                                     labelLines.push(`${xLabel}: ${formattedX} ${!useIndexAsX && pointData.originalX != pointData.x ? '(Raw: '+pointData.originalX+')' : ''}`);
                                     labelLines.push(`${yLabel}: ${formattedYTooltip} ${pointData.originalY != pointData.y ? '(Raw: '+pointData.originalY+')' : ''}`);

                                     // Add color field info
                                     if (colorField && pointData.colorValue !== null && pointData.colorValue !== undefined) {
                                         labelLines.push(`${colorField}: ${pointData.colorValue}`);
                                     }

                                     // Add size field info for bubble charts
                                     if (chartType === 'bubble' && sizeField && pointData.originalSize !== null && pointData.originalSize !== undefined) {
                                         // Format Size value (Prioritize post-transform)
                                         const sizeFormat = (sizeField && postTransformNumericFormats.hasOwnProperty(sizeField))
                                                           ? postTransformNumericFormats[sizeField]
                                                           : ((sizeField && preTransformNumericFormats[sizeField]) ? preTransformNumericFormats[sizeField] : 'default');
                                         const formattedSize = formatNumericValue(pointData.originalSize, sizeFormat);
                                         labelLines.push(`${sizeField} (Size): ${formattedSize}`); // Removed (Raw: ...) part
                                     }

                                 } else {
                                      // Fallback
                                      labelLines.push(`${context.dataset.label || 'Data'}: (${context.parsed.x}, ${context.parsed.y})`);
                                 }

                                 return labelLines;
                             }
                         }
                     }, // End tooltip callbacks
                     zoom: {
                         pan: {
                             enabled: true,
                             mode: 'xy', // Allow panning on both axes
                             threshold: 5, // Minimum drag distance to trigger pan
                             overscroll: true, // Explicitly set overscroll mode
                         },
                         zoom: {
                             wheel: {
                                 enabled: true, // Enable zooming via mouse wheel
                             },
                             pinch: {
                                 enabled: true // Enable zooming via pinch gesture (requires Hammer.js for touch)
                             },
                             drag: {
                                  enabled: true, // Enable zooming via drag selection 
                                  modifierKey: 'shift', // Optional: Require Shift key for drag zoom
                             },
                             mode: 'xy', // Allow zooming on both axes
                         }
                     }
                 }
             }
         });

         // Force an update after initial render - might help plugin init
         if (reportChartInstance) {
             reportChartInstance.update(); 
             // Trigger resize event after update
             window.dispatchEvent(new Event('resize')); 
             console.log("Forced chart update and dispatched resize event.");
         }

         console.log(`Chart rendered successfully as ${selectedChartType}.`); // Updated log

         // <<< USE finalDataForAnalysis >>>
         console.log(`Generating ${selectedChartType} plot for field: ${selectedYField} with ${finalDataForAnalysis.length} records.`); // Corrected variable name and length source

         // Prepare data for chart
         // For Scatter/Line: [{ x: index, y: value, ticker: ticker, colorValue: colorValue }]
         // For Bar: labels: [ticker1, ticker2,...], data: [value1, value2,...]
         // Use let instead of const
         // plotData = []; 
         // labels = []; 
         // numericData = []; 
         // pointBackgroundColors = [];
         // pointBorderColors = [];
         // nonNumericCount = 0; 

         // <<< NEW: Assign final plot data for search functionality >>>
         currentChartPlotData = plotData; // Use the generated plotData
         console.log(`[renderChart] Updated currentChartPlotData with ${currentChartPlotData.length} points.`);
    }
    // --- END Render Chart --- 

    // --- NEW: Chart Ticker Highlight Functions ---
    function resetChartHighlight() {
        if (!reportChartInstance || highlightedPointIndex < 0 || !originalPointStyle) {
            return; // Nothing to reset or chart not ready
        }
        try {
            // Assuming highlight affects the first dataset
            const dataset = reportChartInstance.data.datasets[0];
            if (!dataset) { // Simplified check
                console.warn("[resetChartHighlight] Dataset not found. Cannot reset style.");
                return;
            }

            // Restore original style (handle array/single value for each property)
            const propsToRestore = ['backgroundColor', 'borderColor', 'borderWidth', 'radius', 'pointStyle'];
            propsToRestore.forEach(prop => {
                if (originalPointStyle.hasOwnProperty(prop)) {
                    if (Array.isArray(dataset[prop])) {
                        if (dataset[prop].length > highlightedPointIndex) {
                             dataset[prop][highlightedPointIndex] = originalPointStyle[prop];
                        }
                    } else {
                        dataset[prop] = originalPointStyle[prop];
                    }
                }
            });

            reportChartInstance.update('none'); // Update without animation
            console.log(`[ChartHighlight] Reset highlight for index ${highlightedPointIndex}`);
        } catch (error) {
            console.error("[resetChartHighlight] Error resetting highlight:", error);
        } finally {
            // Clear state regardless of success/failure
            highlightedPointIndex = -1;
            originalPointStyle = null;
            const searchStatus = document.getElementById('chart-search-status');
            if (searchStatus) searchStatus.textContent = ''; // Clear status message
        }
    }

    function highlightTickerOnChart(tickerToFind) {
        console.log("[ChartHighlight] highlightTickerOnChart function entered."); 
        resetChartHighlight(); // Reset previous highlight first
        const searchStatus = document.getElementById('chart-search-status');

        // Wrap reset call in try-catch
        try {
            // resetChartHighlight(); // Called above, remove duplicate
            console.log("[ChartHighlight] resetChartHighlight completed successfully.");
        } catch (resetError) {
            console.error("[ChartHighlight] Error occurred within resetChartHighlight call:", resetError);
        }

        console.log(`[ChartHighlight] BEFORE Check 1: reportChartInstance valid? ${!!reportChartInstance}, currentChartPlotData valid? ${!!currentChartPlotData}, data length: ${currentChartPlotData?.length}`);
        if (!reportChartInstance || !currentChartPlotData || currentChartPlotData.length === 0) {
            console.log("[ChartHighlight] EXITING at Check 1 (Chart/data not ready).");
            if (searchStatus) searchStatus.textContent = 'Chart/data not ready.';
            return;
        }
        console.log(`[ChartHighlight] Checking chart type. Instance type: '${reportChartInstance?.config?.type}'`);
        const chartType = reportChartInstance.config.type;
        // Allow highlight on bar charts too now, though pointStyle won't apply
        if (chartType !== 'scatter' && chartType !== 'bubble' && chartType !== 'line' && chartType !== 'bar') { 
            if (searchStatus) searchStatus.textContent = 'Highlighting not supported for this chart type.';
            return;
        }

        const upperTicker = tickerToFind.toUpperCase().trim();
        if (!upperTicker) {
            if (searchStatus) searchStatus.textContent = '';
            return;
        }

        // --- Find Index --- Find index based on chart type
        let foundIndex = -1;
        if (chartType === 'bar') {
            // For bar charts, find the index in the labels array
            const labels = reportChartInstance.data.labels || [];
            foundIndex = labels.findIndex(label => label && label.toUpperCase() === upperTicker);
        } else {
            // For other chart types, use currentChartPlotData (scatter, line, bubble)
            foundIndex = currentChartPlotData.findIndex(p => p && p.ticker && p.ticker.toUpperCase() === upperTicker);
        }

        if (foundIndex > -1) {
            try {
                const dataset = reportChartInstance.data.datasets[0];
                if (!dataset) throw new Error("Dataset not found");
                const dataLength = (chartType === 'bar') ? (dataset.data?.length ?? 0) : currentChartPlotData.length;
                if (dataLength === 0) throw new Error("Dataset has no data");

                // --- Store Original Styles --- 
                originalPointStyle = {};
                const propsToStoreAndModify = ['backgroundColor', 'borderColor', 'borderWidth', 'radius', 'pointStyle'];

                // Helper to get original value (array or single)
                const getOriginalValue = (prop) => {
                    if (!dataset.hasOwnProperty(prop)) return undefined; // Property doesn't exist
                    const propValue = dataset[prop];
                    return Array.isArray(propValue) ? (propValue[foundIndex] ?? undefined) : propValue;
                };

                propsToStoreAndModify.forEach(prop => {
                     originalPointStyle[prop] = getOriginalValue(prop);
                });
                
                // Define fallbacks for potentially missing originals
                const fallbacks = {
                    backgroundColor: 'rgba(128,128,128,0.1)',
                    borderColor: 'rgba(128,128,128,1)',
                    borderWidth: 1,
                    radius: (chartType === 'bar') ? undefined : 3, // No default radius for bars
                    pointStyle: (chartType === 'bar') ? undefined : 'circle' // No default pointStyle for bars
                };

                // Apply fallbacks if original was undefined
                for (const prop in fallbacks) {
                    if (originalPointStyle[prop] === undefined) {
                        originalPointStyle[prop] = fallbacks[prop];
                    }
                }
                // Ensure we don't store undefined if a property was truly missing
                Object.keys(originalPointStyle).forEach(key => {
                    if (originalPointStyle[key] === undefined) delete originalPointStyle[key];
                });
                
                console.log("[ChartHighlight] Stored original styles:", JSON.parse(JSON.stringify(originalPointStyle)));

                // --- Apply Highlight Styles --- 
                const HIGHLIGHT_BORDER_WIDTH = 4; // Increased border
                const HIGHLIGHT_RADIUS_INCREASE = 6; // Increased radius bump
                const HIGHLIGHT_POINT_STYLE = 'star';

                // Calculate new radius safely (only if original radius exists)
                let newRadius = originalPointStyle.radius; // Start with original
                if (typeof originalPointStyle.radius === 'number') { // Check if it was a number
                     newRadius = originalPointStyle.radius + HIGHLIGHT_RADIUS_INCREASE;
                }
                
                // Styles to apply
                const highlightStyles = {
                    // backgroundColor: HIGHLIGHT_COLOR, // Optional: Change background too?
                    borderColor: HIGHLIGHT_COLOR,
                    borderWidth: HIGHLIGHT_BORDER_WIDTH,
                    radius: newRadius, // Apply calculated new radius
                    pointStyle: (chartType === 'bubble' || chartType === 'bar') ? originalPointStyle.pointStyle : HIGHLIGHT_POINT_STYLE // Don't apply star to bubble/bar
                };

                // Helper to ensure property is an array and set the value
                const ensureArrayAndSet = (prop, valueToSet) => {
                    if (!dataset.hasOwnProperty(prop) && !originalPointStyle.hasOwnProperty(prop)) {
                         console.warn(`[Highlight] Skipping property '${prop}' as it doesn't exist in dataset or originals.`);
                         return; // Don't try to set properties that don't exist
                    }
                    
                    let currentArray = dataset[prop];
                    const original = originalPointStyle[prop]; // Use stored original

                    if (!Array.isArray(currentArray)) {
                        // If not an array, create one, filling with original/fallback
                        console.log(`[Highlight] Converting '${prop}' to array.`);
                        currentArray = new Array(dataLength).fill(original);
                        dataset[prop] = currentArray; // Assign the new array back to the dataset
                    }
                    
                    // Ensure array is long enough (might happen with dynamic data)
                    if (currentArray.length < dataLength) {
                        console.warn(`[Highlight] Array for '${prop}' is shorter than data length. Extending...`);
                        const originalFill = currentArray[0] !== undefined ? currentArray[0] : original; // Use first element or original as fill
                        while(currentArray.length < dataLength) {
                            currentArray.push(originalFill);
                        }
                    }

                    // Set the value at the specific index
                    if (foundIndex < currentArray.length) {
                        currentArray[foundIndex] = valueToSet;
                        console.log(`[Highlight] Set ${prop}[${foundIndex}] = ${valueToSet}`);
                    } else {
                         console.error(`[Highlight] foundIndex ${foundIndex} is out of bounds for '${prop}' array (length ${currentArray.length})`);
                    }
                };

                // Apply the highlight styles using the helper
                for (const prop in highlightStyles) {
                    // Only apply radius/pointStyle if they existed originally or make sense for chart type
                    if (prop === 'radius' && originalPointStyle.radius === undefined) continue;
                    if (prop === 'pointStyle' && originalPointStyle.pointStyle === undefined) continue;
                    
                    ensureArrayAndSet(prop, highlightStyles[prop]);
                }

                highlightedPointIndex = foundIndex;
                reportChartInstance.update('none'); // Update chart without animation
                if (searchStatus) searchStatus.textContent = `Ticker ${tickerToFind.toUpperCase()} highlighted.`;
                console.log(`[ChartHighlight] Highlight applied to index ${foundIndex} for ticker ${tickerToFind}`);

            } catch (error) {
                console.error(`[ChartHighlight] Error applying highlight for ${tickerToFind}:`, error);
                if (searchStatus) searchStatus.textContent = `Error highlighting ${tickerToFind}.`;
                highlightedPointIndex = -1;
                originalPointStyle = null;
            }
        } else {
            if (searchStatus) searchStatus.textContent = `Ticker ${tickerToFind.toUpperCase()} not found in chart data.`;
            console.log(`[ChartHighlight] Ticker ${tickerToFind} not found.`);
        }
    }
    // --- END Highlight Functions ---

    // --- Data Loading & State Update (Preparation Tab) ---
     function processLoadedDataAndUpdateState() {
         if (!fullProcessedData || fullProcessedData.length === 0) {
             availableFields = [];
             fieldMetadata = {}; // Clear metadata if no data
             // Keep loaded weights/enabled status? Or clear them?
             // Let's keep them for now, in case user reloads data.
             // fieldEnabledStatus = {};
             console.log("No processed data loaded or data is empty.");
             // Re-render empty UIs
             renderFieldConfigUI();
             renderFilterUI();
             applyFilters(); // Clear output area if needed
             return;
         }

         // --- Discover Fields --- 
         const discoveredFields = new Set();
         fullProcessedData.forEach(item => {
             if (item && item.processed_data) {
                 Object.keys(item.processed_data).forEach(key => discoveredFields.add(key));
             }
         });
         discoveredFields.add('source'); // --- ADD 'source' FIELD EXPLICITLY --- 
         availableFields = [...discoveredFields].sort();
         console.log("Discovered fields (including source):", availableFields);

         // --- Calculate Metadata --- 
         const newFieldMetadata = {};
         const MAX_UNIQUE_TEXT_VALUES_FOR_DROPDOWN = 100; // Limit for text dropdowns

         availableFields.forEach(field => {
            let numericCount = 0;
            let existingValueCount = 0; // <<< Initialize counter for existing values
            let min = Infinity;
            let max = -Infinity;
            let sum = 0; // <<< ADDED for average
            let numericValues = []; // <<< ADDED for median
            const allUniqueTextValues = new Set(); // <<< Use Set to get ALL unique text values

            fullProcessedData.forEach(item => {
                // --- Get value based on field (source vs processed_data) --- 
                let value = null;
                if (field === 'source') {
                    if (item && item.source) {
                        value = item.source;
                    }
                } else if (item && item.processed_data && item.processed_data.hasOwnProperty(field)) {
                     value = item.processed_data[field];
                }
                // --- End Get value ---
                
                // Check if the value exists and is not null/undefined/empty string/placeholder '-'
                const valueExists = value !== null && value !== undefined && String(value).trim() !== '' && String(value).trim() !== '-';
                
                if (valueExists) {
                     existingValueCount++; // <<< Increment count if value exists
                     const num = Number(value);
                     if (!isNaN(num)) {
                         numericCount++;
                         if (num < min) min = num;
                         if (num > max) max = num;
                         sum += num; // <<< ADDED
                         numericValues.push(num); // <<< ADDED
                     } else {
                         // Collect ALL unique non-numeric strings
                         allUniqueTextValues.add(String(value)); // Store as string
                     }
                }
            });

            // Determine field type and store metadata
            if (existingValueCount === 0) {
                newFieldMetadata[field] = { 
                    type: 'empty', 
                    existingValueCount: 0 
                }; // Field exists but no valid values
            } else if (numericCount / existingValueCount >= 0.8) { // Heuristic: >= 80% numeric?
                // <<< Calculate average and median BEFORE creating metadata object >>>
                const average = (numericCount > 0) ? sum / numericCount : null;
                const median = calculateMedian(numericValues);

                newFieldMetadata[field] = {
                    type: 'numeric',
                    min: min === Infinity ? null : min, // Store null
                    max: max === -Infinity ? null : max, // Store null
                    average: average, // <<< ADDED
                    median: median,   // <<< ADDED
                    existingValueCount: existingValueCount
                };
            } else {
                const totalUniqueCount = allUniqueTextValues.size; // <<< Get total unique count
                // Create the potentially truncated array for dropdowns
                const uniqueValuesForDropdown = [...allUniqueTextValues].sort().slice(0, MAX_UNIQUE_TEXT_VALUES_FOR_DROPDOWN);
                
                newFieldMetadata[field] = { 
                    type: 'text', 
                    uniqueValues: uniqueValuesForDropdown, // For dropdowns
                    totalUniqueCount: totalUniqueCount,   // Exact total unique count
                    existingValueCount: existingValueCount 
                };
            }
         });
         fieldMetadata = newFieldMetadata; // Update global metadata
         console.log("Calculated field metadata:", fieldMetadata);

         // --- Initialize Enabled Status for New Fields --- 
         let statusChanged = false;
         availableFields.forEach(field => {
             // Default enabled status true if not present
             if (!(field in fieldEnabledStatus)) {
                 fieldEnabledStatus[field] = true;
                 statusChanged = true;
                 console.log(`Initialized enabled status for new field '${field}' to true.`);
             }
         });

         // Optional: Clean up status for fields no longer present in the data
         const currentAvailableSet = new Set(availableFields);
         Object.keys(fieldEnabledStatus).forEach(field => {
             if (!currentAvailableSet.has(field)) {
                 console.log(`Removing stale enabled status for field '${field}'.`);
                 delete fieldEnabledStatus[field];
                 statusChanged = true;
             }
         });

         // Save if defaults were added or stale entries removed
         if (statusChanged) saveEnabledStatusToStorage();

         // --- Initialize Numeric Format Status for New Numeric Fields --- 
         let formatStatusChanged = false;
         availableFields.forEach(field => {
             const meta = fieldMetadata[field] || {};
             // Only apply to numeric fields
             if (meta.type === 'numeric') {
                 // Default format is 'default' if not present
                 if (!(field in fieldNumericFormats)) {
                     fieldNumericFormats[field] = 'default'; 
                     formatStatusChanged = true;
                     console.log(`Initialized numeric format for new field '${field}' to 'default'.`);
                 }
             }
         });

         // Optional: Clean up formats for fields no longer numeric or present
         const currentNumericFields = new Set(availableFields.filter(f => (fieldMetadata[f] || {}).type === 'numeric'));
         Object.keys(fieldNumericFormats).forEach(field => {
             if (!currentNumericFields.has(field)) {
                 console.log(`Removing stale numeric format for field '${field}'.`);
                 delete fieldNumericFormats[field];
                 formatStatusChanged = true;
             }
         });

         // Save if defaults were added or stale entries removed
         if (formatStatusChanged) saveNumericFormatsToStorage();
         // --- End Initialize Numeric Format Status ---

         // --- REMOVE Re-render UIs --- 
         // renderFieldConfigUI(); // Render config first (populates fieldEnabledStatus)
         // renderFilterUI(); // Then render filters (uses fieldEnabledStatus)
         // populateReportFieldSelector(); // Populates BOTH X and Y now
         // populateReportColorSelector(); 
     }

    // --- Button Listeners (Preparation Tab) ---
    // Add Filter Button
     if (addFilterBtn) {
         addFilterBtn.addEventListener('click', () => {
             console.log("Add Filter clicked");
             // Add a new blank filter object including comment
             currentFilters.push({ id: Date.now() + Math.random(), field: '', operator: '=', value: '', comment: '' });
             // Don't save here, let Apply Filters or Remove handle saving
             renderFilterUI();
         });
     }

    // Apply Filters Button
    if (applyFiltersBtn) {
        applyFiltersBtn.addEventListener('click', () => {
            console.log("Apply Filters clicked - Current Filter State:", JSON.parse(JSON.stringify(currentFilters)));
            // NOTE: The `currentFilters` array should already be up-to-date
            // due to the 'change'/'input' listeners on the select/input fields.
            // We just need to save this state before applying.
            saveFiltersToStorage();
            applyFilters(); // Apply the filters using the current state
        });
    }

    // Reset Filters Button
    if (resetFiltersBtn) {
        resetFiltersBtn.addEventListener('click', () => {
             console.log("Reset Filters clicked");
             // Reset filters to a single blank one including comment
             currentFilters = [{ id: Date.now() + Math.random(), field: '', operator: '=', value: '', comment: '' }];
             saveFiltersToStorage(); // Save the reset state
             renderFilterUI();
             applyFilters(); // Re-apply filters (which should now show all data)
        });
    }

    // Process Data Button (Load from DB)
    console.log("Locating processButton element..."); // Updated log
    // Use processButton declared earlier
    console.log("processButton element:", processButton);

    // Check if the button element exists before proceeding
    if (processButton) {
        // <<< MOVE processStatus declaration HERE >>>
        const processStatus = document.getElementById('process-analytics-status');
        console.log("Locating processStatus element just before adding listener:", processStatus); // <<< ADD LOG
        
        // Check if status element was found before adding listener
        if (processStatus) {
            console.log("Adding event listener to processButton."); 
            processButton.addEventListener('click', async function() {
                console.log("processButton clicked!"); 
                // <<< Show spinner and disable button >>>
                if (typeof window.showSpinner === 'function') {
                    window.showSpinner(processButton);
                } else {
                    console.error("Global showSpinner function not found!");
                    processButton.disabled = true; // Fallback
                }
                // Use the processStatus variable captured just above
                processStatus.textContent = 'Processing data...';
                processStatus.className = 'ms-2 text-info';
                // outputArea.textContent = ''; // No longer needed
                if(filterResultsCount) filterResultsCount.textContent = ''; // Clear count

                try {
                    // Step 1: Call the endpoint to process and store data (using the backend python function)
                    console.log("Calling /api/analytics/process-raw-data endpoint...");
                    const processResponse = await fetch('/api/analytics/process-raw-data', {
                        method: 'POST',
                        headers: { 'Accept': 'application/json' }
                    });

                    const processResult = await processResponse.json();

                    if (!processResponse.ok) {
                        const errorDetail = processResult.detail || `Processing failed with status ${processResponse.status} - ${processResponse.statusText}`;
                        console.error("Error response from process-raw-data:", processResult);
                        throw new Error(errorDetail);
                    }

                    console.log("Process request successful:", processResult);
                    processStatus.textContent = processResult.message || 'Processing request successful.';
                    processStatus.className = 'ms-2 text-success';

                    // Step 2: Call the endpoint to get the processed data
                    console.log("Calling /api/analytics/get-processed-data endpoint...");
                    const getResponse = await fetch('/api/analytics/get-processed-data', {
                        method: 'GET',
                        headers: { 'Accept': 'application/json' }
                    });

                    if (!getResponse.ok) {
                         const getErrorResult = await getResponse.json();
                         const getErrorDetail = getErrorResult.detail || `Failed to get processed data with status ${getResponse.status} - ${getResponse.statusText}`;
                         console.error("Error response from get-processed-data:", getErrorResult);
                         throw new Error(getErrorDetail);
                    }

                    const processedData = await getResponse.json();
                    console.log(`Successfully fetched ${processedData.length} processed records.`);
                    fullProcessedData = processedData;

                    // Step 3: Process loaded data (extract fields, init weights/status)
                    console.log("Processing loaded data and updating PRE-transform state...");
                    processLoadedDataAndUpdateState(fullProcessedData); // Calculate PRE-transform state
 
                    // Step 3.5: Initialize FINAL state based on loaded data
                    console.log("Initializing final state based on loaded data...");
                    finalDataForAnalysis = [...fullProcessedData]; // Initialize final data
                    updateFinalFieldsAndMetadata(finalDataForAnalysis); // Calculate POST-transform state
 
                    // Step 4: Apply initial filters (updates Prep table and filteredDataForChart)
                    console.log("Applying initial filters...");
                    applyFilters(); // This updates filteredDataForChart and calls renderChart
 
                    // Step 5: Update ALL UI components now that both states are populated
                    console.log("Rendering initial UI...");
                    updateAnalyticsUI({ updatePrepUI: true, updateAnalyzeUI: true });

                    // <<< NEW: Initialize Pivot Table Placeholder >>>
                    console.log("Placeholder: Pivot table initialization would happen here after initial load.");
                    // TODO: Add logic here to initialize the pivot table
                    //       in the '#pivot-table-output' div using the initial 'finalDataForAnalysis' data.
                    //       Example using a hypothetical 'initializePivotTable' function:
                    // initializePivotTable(finalDataForAnalysis);
                    const initialPivotStatus = document.getElementById('pivot-table-status');
                    if (initialPivotStatus) {
                        initialPivotStatus.textContent = `Initial data processed. Pivot table library integration needed to display ${finalDataForAnalysis.length} records.`;
                    }
                    // <<< END NEW >>>

                } catch (error) {
                    console.error('Error during Finviz data processing/fetching:', error);
                    // outputArea.textContent = `An error occurred. Check console for details. \nError: ${error.message}`; // No longer using outputArea
                    // Display error in the status span instead?
                    processStatus.textContent = `Error: ${error.message || 'An unknown error occurred.'}`;
                    processStatus.className = 'ms-2 text-danger';
                    fullProcessedData = []; // Clear data on error
                    // <<< Clear BOTH pre- and post-transform states on error >>>
                    availableFields = [];
                    fieldMetadata = {};
                    finalAvailableFields = []; 
                    finalFieldMetadata = {};
                    finalDataForAnalysis = []; // Clear final data on error
                    
                    // processLoadedDataAndUpdateState(); // Re-render UIs (will show empty state) // <<< Don't call this anymore
                    applyFilters(); // Clear table on error
                    updateAnalyticsUI({ updatePrepUI: true, updateAnalyzeUI: true }); // Update UI to show empty state
                } finally {
                     processButton.disabled = false;
                     console.log("Processing/fetching finished.");
                     // <<< Hide spinner and re-enable button >>>
                     if (typeof window.hideSpinner === 'function') {
                         window.hideSpinner(processButton);
                     } else {
                         console.error("Global hideSpinner function not found!");
                         processButton.disabled = false; // Fallback
                     }
                 }
             });
        } else {
             console.error("Could not find processStatus element (#process-analytics-status). Listener not attached."); // Specific error message
        }
    } else {
        console.error("Could not find processButton element (#process-analytics-data-btn). Listener not attached."); 
    }

    // --- Helper functions to toggle button spinner/text --- 
    function showSpinner(button, otherButton) {
        const spinner = button.querySelector('.spinner-border');
        const text = button.querySelector('.button-text');
        if (spinner) spinner.style.display = 'inline-block';
        if (text) text.style.display = 'none';
        button.disabled = true;
        if (otherButton) otherButton.disabled = true; // Disable other button too
        // <<< ADD LOG: Check if elements were found >>>
        console.log("[showSpinner] Found spinner element:", spinner ? 'Yes' : 'NO');
        console.log("[showSpinner] Found text element:", text ? 'Yes' : 'NO');
    }

    function hideSpinner(button, otherButton) {
        const spinner = button.querySelector('.spinner-border');
        const text = button.querySelector('.button-text');
        if (spinner) spinner.style.display = 'none';
        if (text) text.style.display = 'inline-block'; 
        
        // Re-enable the current button
        button.disabled = false;

        // Re-enable the other button conditionally
        if (otherButton) {
            // If the other button is the upload button, only enable it if tickers are loaded
            if (otherButton.id === 'run-finviz-upload-btn') {
                if (uploadedTickers.length > 0) {
                    otherButton.disabled = false;
                }
            } else {
                // Otherwise (if other button is the screened fetch), always re-enable it
                otherButton.disabled = false;
            }
        }
    }
    // --- End Helper Functions --- 

    // --- Initial Page Load Logic ---
    console.log("Initial page load: Loading state from localStorage...");
    loadFiltersFromStorage();
    // loadWeightsFromStorage(); // REMOVED
    loadEnabledStatusFromStorage();
    loadNumericFormatsFromStorage(); // <<< ADD THIS CALL HERE
    loadInfoTipsFromStorage(); // <<< ADD THIS CALL HERE FOR STEP 4
    // <<< REMOVE Immediate filter render - wait for data/metadata >>>
    // renderFilterUI();

    // --- Register Chart.js Plugins --- 
    if (window.ChartZoom) { // Check if plugin loaded
        Chart.register(window.ChartZoom);
        console.log("Chartjs zoom plugin registered.");
    } else {
        console.warn("Chartjs zoom plugin (ChartZoom) not found. Zoom/pan disabled.");
    }
    // --- End Plugin Registration --- 

    // Initial render based on loaded state (data is empty initially)
    renderFilterUI(); // Render Prep tab UI elements
    renderFieldConfigUI(); // <<< ADD THIS CALL HERE
    // Optionally trigger the "Load Data from DB" automatically on page load?
    // processButton.click(); // Uncomment to auto-load data
    
    // --- Event Listener for Report Field Selector ---
    if (reportFieldSelector) { // Y-axis
        reportFieldSelector.addEventListener('change', renderChart); 
    }
    // --- NEW: Event Listener for Report X-Axis Selector ---
    if (reportXAxisSelector) {
        reportXAxisSelector.addEventListener('change', renderChart);
    }
    
    // --- Event Listener for Report Color Selector --- Moved inside DOMContentLoaded
    if (reportColorSelector) {
        reportColorSelector.addEventListener('change', renderChart);
    }
    // --- END Event Listener ---

    console.log("Initial page load complete.");

    // --- NEW: Add Event Listener for Report Chart Type Selector --- Moved inside DOMContentLoaded
    if (reportChartTypeSelector) {
        reportChartTypeSelector.addEventListener('change', renderChart);
    }
    // --- END Event Listener ---

    // --- NEW: Add Event Listener for Reset Chart Button ---
    if (resetChartBtn) {
        resetChartBtn.addEventListener('click', () => {
            console.log("Reset View button clicked");
            if (reportChartInstance) {
                reportChartInstance.resetZoom();
                console.log("Chart zoom/pan reset.");
            } else {
                console.log("No chart instance found to reset zoom.");
            }
        });
    }
    // --- END Reset Button Listener ---

    // --- NEW: Add Event Listener for Report Size Selector ---
    if (reportSizeSelector) {
        reportSizeSelector.addEventListener('change', renderChart);
    }
    // --- END Size Selector Listener ---

    // --- NEW: Add Event Listener for Swap Axes Button ---
    if (swapAxesBtn && reportXAxisSelector && reportFieldSelector) { // Ensure all elements exist
        swapAxesBtn.addEventListener('click', () => {
            console.log("Swap Axes button clicked");
            const currentX = reportXAxisSelector.value;
            const currentY = reportFieldSelector.value;

            // --- Validation ---
            // 1. Don't swap if Y is not selected
            if (!currentY) {
                console.log("[Swap Axes] Cannot swap: Y-axis not selected.");
                return;
            }
            // 2. Check if Y value exists in X options
            const yOptionInX = Array.from(reportXAxisSelector.options).some(opt => opt.value === currentY);
            if (!yOptionInX) {
                console.log(`[Swap Axes] Cannot swap: Y-axis value '${currentY}' not found in X-axis options.`);
                return;
            }

            let newX = currentY;
            let newY = currentX;

            // --- Handle special 'index' case for X ---
            if (currentX === 'index') {
                // Allow swapping Y -> X, but keep Y as is (don't put 'index' in Y)
                newY = currentY; // Y remains unchanged
                console.log("[Swap Axes] X is index. Swapping Y->X only.");
            } else {
                // 3. Normal case: Check if X value exists in Y options
                const xOptionInY = Array.from(reportFieldSelector.options).some(opt => opt.value === currentX);
                if (!xOptionInY) {
                    console.log(`[Swap Axes] Cannot swap: X-axis value '${currentX}' not found in Y-axis options.`);
                    return;
                }
                // If all checks pass for normal swap, newX and newY are already set correctly
                console.log("[Swap Axes] Performing full swap.");
            }

            // Apply the new values
            reportXAxisSelector.value = newX;
            reportFieldSelector.value = newY;

            // Re-render the chart
            renderChart();
        });
    }
    // --- END Swap Axes Button Listener ---

    // --- Integration with Transformation Module ---
    function runTransformations() {
        console.log("Main: runTransformations called.");
        const transformModule = window.AnalyticsTransformModule; // Access directly

        if (!transformModule) {
            console.error("Transformation module not available.");
            // Optionally update a status element
            return;
        }

        const rules = transformModule.getTransformationRules();
        console.log("Main: Retrieved transformation rules:", rules);

        // Use the data that resulted from filtering as input
        const inputData = filteredDataForChart;
        if (!inputData || inputData.length === 0) {
            console.warn("Main: No data available from filtering stage to transform.");
            // Optionally update status
            finalDataForAnalysis = []; // Ensure final data is clear
            updateFinalFieldsAndMetadata(finalDataForAnalysis); // Update fields based on empty data
           // updateAnalyticsUI(); // Re-render components
            transformModule.renderTransformedDataPreview(finalDataForAnalysis);
            return;
        }

        console.log(`Main: Applying ${rules.length} rules to ${inputData.length} records...`);
        console.log("[runTransformations] fieldInfoTips BEFORE transformModule.applyTransformations:", JSON.parse(JSON.stringify(fieldInfoTips))); // Log state BEFORE transform logic
        // Apply transformations using the exposed function
        finalDataForAnalysis = transformModule.applyTransformations(inputData, rules);
        console.log(`Main: Transformation complete. Result has ${finalDataForAnalysis.length} records.`);
        console.log("[runTransformations] fieldInfoTips AFTER transformModule.applyTransformations, BEFORE rendering post-transform UI:", JSON.parse(JSON.stringify(fieldInfoTips))); // Log state AFTER transform logic

        // Update the preview panel in the transform tab (this is okay)
        transformModule.renderTransformedDataPreview(finalDataForAnalysis);

        // <<< LOG FINAL DATA BEFORE UI UPDATE >>>
        if (finalDataForAnalysis && finalDataForAnalysis.length > 0) {
            // console.log("End of runTransformations. finalDataForAnalysis[0]:", JSON.parse(JSON.stringify(finalDataForAnalysis[0]))); 
            // if (!finalDataForAnalysis[0]?.processed_data?.['test 2']) { // <<< ENSURE THIS BLOCK IS REMOVED
            //      console.error("CRITICAL WARNING: 'test 2' MISSING from finalDataForAnalysis immediately after transformation!");
            // } // <<< ENSURE THIS BLOCK IS REMOVED
            if (finalDataForAnalysis.length > 0 && finalDataForAnalysis[0]) {
                // Simple log showing keys for the first item
                 console.log("End of runTransformations: First item keys:", Object.keys(finalDataForAnalysis[0]), "Processed_data keys:", Object.keys(finalDataForAnalysis[0].processed_data || {}));
            }
        } else {
             console.log("End of runTransformations. finalDataForAnalysis is empty.");
        }

        // <<< UPDATE POST-TRANSFORM STATE and Analyze UI >>>
        // <<< ADD LOGGING BEFORE UPDATE >>>
        if (finalDataForAnalysis && finalDataForAnalysis.length > 0) {
            console.log("[Analytics] Data structure BEFORE calling updateFinalFieldsAndMetadata:", JSON.parse(JSON.stringify(finalDataForAnalysis[0])));
        } else {
            console.log("[Analytics] finalDataForAnalysis is empty BEFORE calling updateFinalFieldsAndMetadata.");
        }
        updateFinalFieldsAndMetadata(finalDataForAnalysis); // Update the *final* fields/metadata
        // updateAnalyticsUI({ updatePrepUI: false, updateAnalyzeUI: true }); // Trigger ONLY Analyze UI update // <<< Temporarily comment out UI update

        // <<< NEW: Update Pivot Table >>>
        // TODO: Add logic here to initialize or update the pivot table
        //       in the '#pivot-table-output' div using the 'finalDataForAnalysis' data.
        //       Example using a hypothetical 'updatePivotTable' function:
        // updatePivotTable(finalDataForAnalysis);
        console.log("Placeholder: Pivot table update would happen here.");
        const pivotStatus = document.getElementById('pivot-table-status');
        if (pivotStatus) {
            pivotStatus.textContent = `Data transformed. Pivot table library integration needed to display ${finalDataForAnalysis.length} records.`;
        }
        // <<< END NEW >>>

        // <<< ADDED: Render the post-transform config UI >>>
        if (window.AnalyticsPostTransformModule && typeof window.AnalyticsPostTransformModule.renderPostTransformFieldConfigUI === 'function') {
            console.log("[Analytics] Calling renderPostTransformFieldConfigUI...");
            window.AnalyticsPostTransformModule.renderPostTransformFieldConfigUI();

            // <<< EXPLICITLY ATTACH/RE-ATTACH MODIFICATION LISTENERS >>>
            if (window.AnalyticsConfigManager && typeof window.AnalyticsConfigManager.initializeModificationDetection === 'function') {
                console.log("[Analytics] Attaching/Re-attaching modification listeners after Post-Transform UI update.");
                window.AnalyticsConfigManager.initializeModificationDetection();
            } else {
                 console.warn("[Analytics] AnalyticsConfigManager or initializeModificationDetection not found when trying to attach listeners post Post-Transform UI update.");
            }
        } else {
            console.error("[Analytics] AnalyticsPostTransformModule or render function not found when trying to render post-transform config!");
        }
        // <<< END ADDED >>>

        // <<< ADDED: Explicitly update Analyze UI after transformations >>>
        updateAnalyticsUI({ updatePrepUI: false, updateAnalyzeUI: true });
        console.log("[Analytics] Analyze UI update triggered after transformations.");
        // <<< END ADDED >>>
    }

    function updateFinalFieldsAndMetadata(data) {
        console.log("Updating FINAL available fields and metadata based on provided data...");
        if (!data || data.length === 0) {
            finalAvailableFields = []; // <<< Update final state
            finalFieldMetadata = {};   // <<< Update final state
            console.log("No data provided for final state, fields and metadata cleared.");
            // Don't clear enabled status or formats here, preserve user settings
            return;
        }

        // --- Discover Fields (including synthetic ones) ---
        const discoveredFields = new Set();
        let tickerFound = false; // Track if ticker was found in the data at all
        data.forEach(item => {
             if (item?.ticker !== undefined) { // Check specifically for ticker presence
                tickerFound = true;
             }
             if (item?.processed_data) { // Safer check
                 Object.keys(item.processed_data).forEach(key => discoveredFields.add(key));
             }
        });
        const allDiscoveredNonTickerFields = [...discoveredFields].sort(); // Get ONLY non-ticker fields discovered
        // console.log("All discovered non-ticker fields post-transform:", allDiscoveredNonTickerFields);

        // --- Filter discovered non-ticker fields based on PRE-TRANSFORM enabled status ---
        const preTransformEnabledStatus = fieldEnabledStatus; // Use the global pre-transform status map
        let tempFinalFields = allDiscoveredNonTickerFields.filter(field => {
            // Keep field if it's enabled pre-transform (or status missing -> default true)
            const isEnabledPreTransform = preTransformEnabledStatus[field] !== false; // Default true if undefined/true
            if (!isEnabledPreTransform) {
                console.log(`[updateFinalFieldsAndMetadata] Excluding field '${field}' because it was disabled pre-transform.`);
            }
            return isEnabledPreTransform;
        });
        // console.log("Filtered non-ticker fields:", tempFinalFields);

        // --- Prepend 'ticker' (lowercase) to the list if it was found in the original data --- 
        if (tickerFound) {
             finalAvailableFields = ['ticker', ...tempFinalFields]; // Use lowercase 'ticker'
        } else {
             finalAvailableFields = tempFinalFields; // Ticker was not present, don't add it
        }
        // console.log("Final available fields (post-transform, filtered by pre-transform status):", finalAvailableFields);
        console.log("Final available fields (post-transform, filtered by pre-transform status, Ticker added if present):", finalAvailableFields);

        // --- Calculate Metadata based ONLY on the filtered finalAvailableFields ---
        // <<< Also calculate metadata for 'ticker' if it was found >>>
        const newFinalFieldMetadata = {}; // Use separate temp variable
        const MAX_UNIQUE_TEXT_VALUES_FOR_DROPDOWN = 100; // Limit for text dropdowns

        // Calculate metadata for 'ticker' separately if it exists
        if (tickerFound) {
             const allUniqueTextValues = new Set();
             let existingValueCount = 0;
             data.forEach(item => {
                 const value = item?.ticker;
                 const valueExists = value !== null && value !== undefined && String(value).trim() !== '';
                 if (valueExists) {
                     existingValueCount++;
                     allUniqueTextValues.add(String(value));
                 }
            });
             const totalUniqueCount = allUniqueTextValues.size;
             const uniqueValuesForDropdown = [...allUniqueTextValues].sort().slice(0, MAX_UNIQUE_TEXT_VALUES_FOR_DROPDOWN);
             newFinalFieldMetadata['ticker'] = { 
                 type: 'text', 
                 uniqueValues: uniqueValuesForDropdown, 
                 totalUniqueCount: totalUniqueCount,   
                 existingValueCount: existingValueCount 
             };
        }

        // --- Iterate over the FILTERED NON-TICKER finalAvailableFields for other metadata ---
        // Use tempFinalFields here as it doesn't include 'ticker' yet
        tempFinalFields.forEach(field => {
            // ... (rest of metadata calculation logic remains the same) ...
            // Ensure this logic only uses `tempFinalFields` (which are guaranteed to be in processed_data)

            let numericCount = 0;
            let existingValueCount = 0;
            let min = Infinity;
            let max = -Infinity;
            let sum = 0;
            let numericValues = [];
            const allUniqueTextValues = new Set();

            data.forEach(item => {
                let value = null;
                 if (item?.processed_data && item.processed_data.hasOwnProperty(field)) { // Check only processed_data
                     value = item.processed_data[field];
                 }
                const valueExists = value !== null && value !== undefined && String(value).trim() !== '' && String(value).trim() !== '-';

                if (valueExists) {
                    existingValueCount++;
                    const num = Number(value);
                    if (!isNaN(num)) {
                        numericCount++;
                        if (num < min) min = num;
                        if (num > max) max = num;
                        sum += num;
                        numericValues.push(num);
                    } else {
                        allUniqueTextValues.add(String(value));
                    }
                }
            });

            // Determine field type and store metadata
            if (existingValueCount === 0) {
                newFinalFieldMetadata[field] = { type: 'empty', existingValueCount: 0 };
            } else if (numericCount / existingValueCount >= 0.8) {
                const average = (numericCount > 0) ? sum / numericCount : null;
                const median = calculateMedian(numericValues);
                newFinalFieldMetadata[field] = { type: 'numeric', min: min === Infinity ? null : min, max: max === -Infinity ? null : max, average: average, median: median, existingValueCount: existingValueCount };
            } else {
                // const MAX_UNIQUE_TEXT_VALUES_FOR_DROPDOWN = 100; // Defined elsewhere or remove if not needed here
                const totalUniqueCount = allUniqueTextValues.size;
                const uniqueValuesForDropdown = [...allUniqueTextValues].sort().slice(0, MAX_UNIQUE_TEXT_VALUES_FOR_DROPDOWN);
                newFinalFieldMetadata[field] = { type: 'text', uniqueValues: uniqueValuesForDropdown, totalUniqueCount: totalUniqueCount, existingValueCount: existingValueCount };
            }
        });
        finalFieldMetadata = newFinalFieldMetadata; // Update the final metadata object
        console.log("Calculated final field metadata (post-transform, based on filtered fields):", finalFieldMetadata);


        // --- Initialize Enabled Status & Formats for New Fields ---
        // (Similar logic to processLoadedDataAndUpdateState)
        let statusChanged = false;
        let formatStatusChanged = false;
        finalAvailableFields.forEach(field => {
            // Initialize enabled status only if field truly doesn't exist pre-transform
            // (It will already exist if it was enabled pre-transform)
            if (!(field in fieldEnabledStatus)) {
                 fieldEnabledStatus[field] = true; // Default *new* fields to true
                 statusChanged = true;
                 console.log(`[updateFinalFieldsAndMetadata] Initialized enabled status for NEW field '${field}' to true.`);
            }
            // Initialize format for numeric fields if not already set
            const meta = finalFieldMetadata[field] || {};
            if (meta.type === 'numeric' && !(field in fieldNumericFormats)) {
                fieldNumericFormats[field] = 'default';
                formatStatusChanged = true;
                 console.log(`[updateFinalFieldsAndMetadata] Initialized numeric format for field '${field}' to 'default'.`);
            }
        });
        // Don't clean up here - preserve settings even if field temporarily disappears
        if (statusChanged) saveEnabledStatusToStorage();
        if (formatStatusChanged) saveNumericFormatsToStorage();
    }

    // <<< REFACTOR UI UPDATE FUNCTION >>>
    function updateAnalyticsUI({ updatePrepUI = false, updateAnalyzeUI = false } = {}) {
         console.log(`Updating Analytics UI components... Prep: ${updatePrepUI}, Analyze: ${updateAnalyzeUI}`);

         if (updatePrepUI) {
             console.log("Updating Preparation Tab UI...");
             // Re-render PREP components that depend on PRE-transform availableFields or fieldMetadata
             renderFieldConfigUI(); // Uses availableFields, fieldMetadata
             renderFilterUI(); // Uses availableFields, fieldMetadata
             // Apply filters handles the output table using availableFields/filteredDataForChart
             // No need to call applyFilters here unless specifically intended

             // <<< EXPLICITLY ATTACH MODIFICATION LISTENERS >>>
             if (window.AnalyticsConfigManager && typeof window.AnalyticsConfigManager.initializeModificationDetection === 'function') {
                 console.log("[AnalyticsUI] Attaching modification listeners after Prep UI update.");
                 window.AnalyticsConfigManager.initializeModificationDetection();
             } else {
                  console.warn("[AnalyticsUI] AnalyticsConfigManager or initializeModificationDetection not found when trying to attach listeners post Prep UI update.");
             }
         }

         if (updateAnalyzeUI) {
              console.log("Updating Analyze Tab UI (Chart/Selectors/DataTable)...");
              // Re-render ANALYZE components that depend on FINAL fields/metadata
              populateReportFieldSelector(); // Uses finalAvailableFields, finalFieldMetadata
              populateReportColorSelector(); // Uses finalAvailableFields
              renderChart(); // Uses finalDataForAnalysis, finalFieldMetadata

              // <<< ADDED: Trigger final data table update >>>
              if (window.AnalyticsDataTableModule && typeof window.AnalyticsDataTableModule.updateTable === 'function') {
                  console.log("[AnalyticsUI] Triggering AnalyticsDataTableModule update...");
                  window.AnalyticsDataTableModule.updateTable();
              } else {
                  console.warn("[AnalyticsUI] AnalyticsDataTableModule or updateTable function not found.");
              }
         }

         // DataTable in applyFilters needs to be updated separately if we want it to show transformed data
         // For now, leave DataTable showing pre-transform data.
         console.log("Analytics UI update finished.");
    }

    // Initial render based on loaded state (data is empty initially)
    // <<< Initial rendering should only update Prep UI >>>
    updateAnalyticsUI({ updatePrepUI: true, updateAnalyzeUI: false });
    // renderFilterUI(); // Render Prep tab UI elements // Handled by updateAnalyticsUI
    // renderFieldConfigUI(); // <<< ADD THIS CALL HERE // Handled by updateAnalyticsUI
    // Optionally trigger the "Load Data from DB" automatically on page load?
    // processButton.click(); // Uncomment to auto-load data

    // --- Expose Main Module Functionality --- 
    // IMPORTANT: This should be one of the LAST things done in this listener
    window.AnalyticsMainModule = {
        runTransformations: runTransformations,
        getCurrentFilteredData: () => filteredDataForChart, // Expose getter for input data
        getAvailableFields: () => availableFields, // <<< PRE-TRANSFORM FIELDS
        getFieldMetadata: () => fieldMetadata,     // <<< PRE-TRANSFORM METADATA
        // --- Add getters for FINAL state ---
        getFinalAvailableFields: () => finalAvailableFields, // <<< POST-TRANSFORM FIELDS
        getFinalFieldMetadata: () => finalFieldMetadata,   // <<< POST-TRANSFORM METADATA
        // --- End added getters ---
        getFieldEnabledStatus: () => fieldEnabledStatus, // <<< ADDED (Shared status)
        // --- Add Setters for direct state update ---
        loadFilters: loadFilters,
        loadFieldSettings: loadFieldSettings
        // Add other functions here if needed by other modules
    };
    console.log("AnalyticsMainModule initialized and exposed."); // <<< ADD CONFIRMATION LOG

    // --- Add event listener for the Filters & Output tab to adjust DataTable columns --- ADDED
    // <<< Remove listener from old tab >>>
    /*
    const filterTabTrigger = document.getElementById('filter-subtab');
    if (filterTabTrigger) {
        filterTabTrigger.addEventListener('shown.bs.tab', function (event) {
            console.log("Filters & Output tab shown. Adjusting DataTable columns.");
            if (outputDataTable) {
                try {
                     // Use setTimeout to ensure rendering is complete before adjusting
                     setTimeout(() => {
                          outputDataTable.columns.adjust().draw(false); // Use draw(false) to avoid resetting paging
                          console.log("DataTable columns adjusted.");
                     }, 0); 
                } catch (e) {
                    console.error("Error adjusting DataTable columns on tab show:", e);
                }
            }
        });
    } else {
        console.warn("Could not find filter sub-tab trigger (#filter-subtab) to attach column adjust listener.");
    }
    */
    // --- End DataTable Tab Listener --- ADDED

    // <<< NEW: Attach listener to Filtered Data Tab >>>
    const filteredDataTabTrigger = document.getElementById('filtered-data-subtab'); // <<< UPDATED ID
    if (filteredDataTabTrigger) {
        filteredDataTabTrigger.addEventListener('shown.bs.tab', function (event) {
            console.log("Filtered Data sub-tab shown. Adjusting DataTable columns."); // <<< UPDATED Log Message
            if (outputDataTable) {
                try {
                     // Use setTimeout to ensure rendering is complete before adjusting
                     setTimeout(() => {
                          outputDataTable.columns.adjust().draw(false); // Use draw(false) to avoid resetting paging
                          console.log("DataTable columns adjusted.");
                     }, 0); 
                } catch (e) {
                    console.error("Error adjusting DataTable columns on tab show:", e);
                }
            }
        });
    } else {
        console.warn("Could not find filtered data sub-tab trigger (#filtered-data-subtab) to attach column adjust listener."); // <<< UPDATED ID & Message
    }
    // <<< END NEW >>>

    // --- End Helper Functions --- 
    window.showSpinner = showSpinner; // Expose globally
    window.hideSpinner = hideSpinner; // Expose globally

    // --- NEW: Load Info Tips Function ---
    function loadInfoTipsFromStorage() {
        console.log("Loading field info tips from localStorage..."); // Log start
        const savedTips = localStorage.getItem(FIELD_INFO_TIPS_STORAGE_KEY);
        if (savedTips) {
            try {
                const parsedTips = JSON.parse(savedTips);
                // Ensure we load an object, even if stored data is invalid/null
                if (parsedTips && typeof parsedTips === 'object') {
                    fieldInfoTips = parsedTips;
                     console.log("Successfully parsed info tips:", fieldInfoTips);
                } else {
                    console.warn("Stored info tips data was not a valid object. Resetting to empty.");
                    fieldInfoTips = {};
                }
            } catch (e) {
                console.error("Error parsing field info tips from localStorage:", e);
                fieldInfoTips = {}; // Reset to default on error
                 localStorage.removeItem(FIELD_INFO_TIPS_STORAGE_KEY); // Clear invalid data
            }
        } else {
            console.log("No saved info tips found in localStorage.");
            fieldInfoTips = {}; // Initialize empty if nothing saved
        }
    }
    // --- END NEW ---

    // --- NEW: Save Info Tips Function ---
    function saveInfoTipsToStorage() {
        console.log("Saving field info tips to localStorage..."); // Log start
        try {
            const tipsString = JSON.stringify(fieldInfoTips);
            localStorage.setItem(FIELD_INFO_TIPS_STORAGE_KEY, tipsString);
            console.log("Successfully saved info tips:", fieldInfoTips);
        } catch (e) {
            console.error("Error saving field info tips to localStorage:", e);
            // Optionally, add a user-facing warning here if storage is full
        }
    }
    // --- END NEW ---

    // --- NEW: Tooltip Initializer Helper (Step 7) ---
    function initializeTooltips(containerSelector) {
        console.log(`Initializing tooltips for selector: ${containerSelector}`);
        const tooltipTriggerList = document.querySelectorAll(`${containerSelector} [data-bs-toggle="tooltip"]`);
        // Dispose existing tooltips first
        tooltipTriggerList.forEach(tooltipTriggerEl => {
            const existingTooltip = bootstrap.Tooltip.getInstance(tooltipTriggerEl);
            if (existingTooltip) {
                // console.log("Disposing existing tooltip for:", tooltipTriggerEl); // Debug log if needed
                existingTooltip.dispose();
            }
        });
        // Initialize new tooltips
        const newTooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl));
        console.log(`Initialized ${newTooltipList.length} tooltips.`);
    }
    // --- END NEW ---

    // --- NEW: Timestamped Filename Helper ---
    function generateTimestampedFilename(baseName = 'Export') {
        const now = new Date();
        const pad = (num) => num.toString().padStart(2, '0');
        // Format: ddmmyyhhmm
        const timestamp = `${pad(now.getDate())}${pad(now.getMonth() + 1)}${now.getFullYear().toString().slice(-2)}${pad(now.getHours())}${pad(now.getMinutes())}`;
        return `${baseName}_${timestamp}`;
    }
    // --- END NEW ---

    // --- Add Hover Listeners for Cross-Highlighting --- 
    const outputTableBody = document.querySelector('#output-table tbody');
    if (outputTableBody) {
        outputTableBody.addEventListener('mouseenter', (event) => {
            if (event.target.tagName === 'TD') {
                const cell = event.target;
                const row = cell.parentElement;
                const cellIndex = cell.cellIndex;

                // Highlight row
                row.classList.add('row-highlight');

                // Highlight column
                const table = row.closest('table');
                const rows = table.querySelectorAll('tbody tr');
                rows.forEach(r => {
                    const cells = r.querySelectorAll('td');
                    if (cells.length > cellIndex) {
                        cells[cellIndex].classList.add('col-highlight');
                    }
                });
            }
        }, true); // Use capture phase to ensure listener fires

        outputTableBody.addEventListener('mouseleave', (event) => {
            if (event.target.tagName === 'TD') {
                const cell = event.target;
                const row = cell.parentElement;
                const cellIndex = cell.cellIndex;

                // Remove row highlight
                row.classList.remove('row-highlight');

                // Remove column highlight
                const table = row.closest('table');
                const highlightedCols = table.querySelectorAll('td.col-highlight');
                highlightedCols.forEach(c => c.classList.remove('col-highlight'));
            }
        }, true); // Use capture phase
    }
    // --- END Hover Listeners --- 

    // --- NEW: Functions to Load State Directly --- 
    function loadFilters(filtersArray) {
        console.log("[AnalyticsMain] Loading filters directly:", filtersArray);
        if (!Array.isArray(filtersArray)) {
            console.error("[AnalyticsMain] Invalid filters data provided for direct load.", filtersArray);
            currentFilters = [{ id: Date.now() + Math.random(), field: '', operator: '=', value: '', comment: '' }]; // Reset to default
        } else {
            // Basic validation/mapping of loaded filters
            currentFilters = filtersArray.map(f => ({
                id: f.id || Date.now() + Math.random(),
                field: f.field || '',
                operator: f.operator || '=',
                value: f.value !== undefined ? f.value : '',
                comment: f.comment || ''
            }));
            if (currentFilters.length === 0) {
                currentFilters.push({ id: Date.now() + Math.random(), field: '', operator: '=', value: '', comment: '' });
            }
        }
        // No need to save to storage here, this is for direct activation
        renderFilterUI(); // Re-render the filter controls
        applyFilters();   // Re-apply filters to the data table & chart input
        console.log("[AnalyticsMain] Filters loaded and applied.");
    }

    function loadFieldSettings(enabledStatus, numericFormats, infoTips) {
        console.log("[AnalyticsMain] Loading field settings directly...");
        // Enabled Status
        if (enabledStatus && typeof enabledStatus === 'object' && !Array.isArray(enabledStatus)) {
            fieldEnabledStatus = enabledStatus;
            // Ensure boolean values
            for (const field in fieldEnabledStatus) {
                if (fieldEnabledStatus.hasOwnProperty(field)) {
                    fieldEnabledStatus[field] = Boolean(fieldEnabledStatus[field]);
                }
            }
        } else {
             console.warn("[AnalyticsMain] Invalid enabledStatus provided, resetting.");
             fieldEnabledStatus = {};
        }
        // Numeric Formats
        if (numericFormats && typeof numericFormats === 'object' && !Array.isArray(numericFormats)) {
            fieldNumericFormats = numericFormats;
        } else {
             console.warn("[AnalyticsMain] Invalid numericFormats provided, resetting.");
             fieldNumericFormats = {};
        }
        // Info Tips - MERGE instead of assign
        console.log("[loadFieldSettings] Merging infoTips. Before merge:", JSON.parse(JSON.stringify(fieldInfoTips)));
        console.log("[loadFieldSettings] Tips received from scenario:", JSON.parse(JSON.stringify(infoTips)));
        const loadedTips = (infoTips && typeof infoTips === 'object' && !Array.isArray(infoTips)) ? infoTips : {};
        // Create a new object to avoid modifying the original in unexpected ways if it's passed by reference elsewhere
        const mergedTips = { ...fieldInfoTips }; // Start with current tips
        for (const field in loadedTips) {
            if (loadedTips.hasOwnProperty(field)) {
                mergedTips[field] = loadedTips[field]; // Overwrite/add tips from loaded scenario
            }
        }
        fieldInfoTips = mergedTips; // Assign the merged result
        console.log("[loadFieldSettings] Merged infoTips. After merge:", JSON.parse(JSON.stringify(fieldInfoTips)));

        // Info Tips - Revert to direct assignment for scenario load
        console.log("[loadFieldSettings] Overwriting infoTips. Received from scenario:", JSON.parse(JSON.stringify(infoTips)));
        if (infoTips && typeof infoTips === 'object' && !Array.isArray(infoTips)) {
            fieldInfoTips = { ...infoTips }; // Create a shallow copy to prevent mutation issues
        } else {
             console.warn("[loadFieldSettings] Invalid infoTips provided, resetting.");
             fieldInfoTips = {};
        }
        console.log("[loadFieldSettings] infoTips after assignment:", JSON.parse(JSON.stringify(fieldInfoTips)));

        // No need to save to storage here
        // Re-render relevant UI components
        renderFieldConfigUI(); // Uses enabledStatus, numericFormats, infoTips
        renderFilterUI();      // Filter dropdowns depend on enabledStatus
        applyFilters();        // Table rendering/formatting depends on settings
        updateAnalyticsUI({ updatePrepUI: false, updateAnalyzeUI: true }); // Update chart axes/tooltips if formats changed
        console.log("[AnalyticsMain] Field settings loaded and UI updated.");
    }
    // --- END NEW Direct Load Functions ---

    // --- Expose Public API --- 
    console.log("Exposing AnalyticsMainModule API...");
    window.AnalyticsMainModule = {
        // Data access
        getFullProcessedData: () => fullProcessedData,
        getFilteredData: () => filteredDataForChart, // Expose pre-transform filtered data
        getFinalDataForAnalysis: () => finalDataForAnalysis, // Expose post-transform data
        getAvailableFields: () => availableFields, // Pre-transform fields
        getFieldMetadata: () => fieldMetadata, // Pre-transform metadata
        getFinalAvailableFields: () => finalAvailableFields, // Post-transform fields
        getFinalFieldMetadata: () => finalFieldMetadata, // Post-transform metadata
        getFieldEnabledStatus: () => fieldEnabledStatus,
        getNumericFieldFormats: () => fieldNumericFormats, // Pre-transform formats
        getFieldInfoTips: () => fieldInfoTips, // Pre-transform tips
        getFilters: () => currentFilters, // <<< ADDED Getter for filters array

        // UI Updaters / Actions
        updateAnalyticsUI: updateAnalyticsUI,
        runTransformations: runTransformations,
        applyFilters: applyFilters, // Allow triggering filters externally if needed
        renderChart: renderChart, // Allow triggering chart render

        // Scenario Management Hooks
        loadFilters: loadFilters, // Load filter data
        loadFieldSettings: loadFieldSettings, // Load pre-transform settings

        // --- Items needed by PostTransform Module (use getters) ---\n\
        // Getter for pre-transform numeric formats (already correct)\n\
        // Getter for pre-transform info tips (already correct)\n\
        // Getter for final available fields (already correct)\n\
        // Getter for final field metadata (already correct)\n\
        // openNumericFormatModal: openNumericFormatModal, // Exposing the function to open the modal - KEEP IF NEEDED\n\
        formatOptions: {                          // Exposing the format options object\n\
            'raw':     'raw data',
            'integer': 'integer',
            'default': 'decimal',
            'percent': 'in %',
            'million': 'in Millions',
            'billion': 'in Billions',
            'configure': 'Custom...' // Ensure this matches modal trigger value if needed\n\
        },
        // --- NEW: Expose functions needed by DataTable Module --- 
        formatNumericValue: formatNumericValue,
        generateTimestampedFilename: generateTimestampedFilename
    };
    console.log("AnalyticsMainModule API exposed.", window.AnalyticsMainModule)

    // --- NEW: Pre-Transform Search Functionality ---
    function applyPreTransformSearchListener() {
        const searchInput = document.getElementById('pre-transform-field-search');
        const container = document.getElementById('field-config-container');

        if (!searchInput || !container) {
            console.warn("[applyPreTransformSearchListener] Search input or container not found. Cannot attach listener.");
            return;
        }
        console.log("[applyPreTransformSearchListener] Attaching listener...");

        let searchTimeout;
        // Use 'input' event for immediate feedback (debounced)
        searchInput.addEventListener('input', () => {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                const searchTerm = searchInput.value.toLowerCase().trim();
                const rows = container.querySelectorAll('.field-data-row'); // Select by specific class
                console.log(`[PreTransformSearch] Filtering for: ${searchTerm}`);
                rows.forEach(row => {
                    const fieldName = row.dataset.fieldName?.toLowerCase() || '';
                    if (fieldName.includes(searchTerm)) {
                        // Show row by removing the hidden class
                        row.classList.remove('search-hidden');
                    } else {
                        // Hide row by adding the hidden class
                        row.classList.add('search-hidden');
                    }
                });
            }, 300); // Debounce search by 300ms
        });
    }
    // --- END NEW Search Functionality ---

    // --- NEW: Listener for Data Visual Tab Shown ---
    if (dataVisualTabTrigger) {
        dataVisualTabTrigger.addEventListener('shown.bs.tab', function (event) {
            console.log("Data Visual tab shown. Refreshing chart and selectors.");
            // Use setTimeout to allow tab transition to finish smoothly
            setTimeout(() => {
                // Call the existing UI update function, targeting only the Analyze tab components
                updateAnalyticsUI({ updatePrepUI: false, updateAnalyzeUI: true });
            }, 0);
        });
    } else {
        console.warn("Could not find Data Visual tab trigger (#data-visual-tab) to attach refresh listener.");
    }
    // --- END NEW ---

    // --- NEW: Listener for Data Table Tab Shown --- 
    const dataTableTabTrigger = document.getElementById('data-table-tab'); 
    if (dataTableTabTrigger) {
        dataTableTabTrigger.addEventListener('shown.bs.tab', function (event) {
            console.log("Data Table tab shown. Refreshing Analyze UI components (including table).");
            // Use setTimeout to allow tab transition to finish smoothly
            setTimeout(() => {
                // Call the existing UI update function, targeting only the Analyze tab components
                // This will call renderChart and AnalyticsDataTableModule.updateTable
                updateAnalyticsUI({ updatePrepUI: false, updateAnalyzeUI: true });
            }, 0);
        });
    } else {
        console.warn("Could not find Data Table tab trigger (#data-table-tab) to attach refresh listener.");
    }
    // --- END NEW --- 

    // --- NEW: Add Listener for Chart Ticker Search ---
    const chartTickerSearchInput = document.getElementById('chart-ticker-search');
    if (chartTickerSearchInput) {
        console.log("[ChartSearch] Attaching listener to search input."); // Verify listener attachment
        chartTickerSearchInput.addEventListener('input', () => {
            clearTimeout(chartSearchTimeout);
            const searchTerm = chartTickerSearchInput.value;
            console.log(`[ChartSearch] Input event: searchTerm = '${searchTerm}'`); // Log search term
            chartSearchTimeout = setTimeout(() => {
                if (searchTerm.trim() === '') {
                    console.log("[ChartSearch] Search term empty, resetting highlight.");
                    resetChartHighlight();
                } else {
                    console.log(`[ChartSearch] Debounced search, calling highlightTickerOnChart('${searchTerm}')`);
                    highlightTickerOnChart(searchTerm);
                }
            }, 500); // Debounce search by 500ms
        });
    }
    // --- END NEW ---

}); 