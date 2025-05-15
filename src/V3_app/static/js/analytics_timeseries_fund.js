(function() {
    "use strict";

    const LOG_PREFIX = "[TimeseriesFundamentalsModule]";
    let timeseriesChartInstance = null; // To hold the Chart.js instance for this module
    let fhTickerSelect = null; // Store globally within this IIFE
    let fhFieldSelect = null; // Store globally
    let fhStartDateInput = null;
    let fhEndDateInput = null;
    let isDataReadyForFund = false; // NEW: Flag to track data readiness

    /**
     * Initializes the controls for the Fundamentals History study.
     * Populates ticker and field select dropdowns.
     */
    function initializeFundamentalsHistoryStudyControls() {
        console.log(LOG_PREFIX, "Initializing Fundamentals History study controls (UI elements only)...");

        fhTickerSelect = document.getElementById('ts-fh-ticker-select');
        fhFieldSelect = document.getElementById('ts-fh-field-select');
        fhStartDateInput = document.getElementById('ts-fh-start-date');
        fhEndDateInput = document.getElementById('ts-fh-end-date');

        if (!fhTickerSelect) console.error(LOG_PREFIX, "Ticker select (ts-fh-ticker-select) not found!");
        if (!fhFieldSelect) console.error(LOG_PREFIX, "Field select (ts-fh-field-select) not found!");
        // Start/End date inputs are optional to find, as they might not always be used or visible.

        // Populate fields (this is static and doesn't depend on final data)
        populateFieldSelect();
        // Tickers will be populated by handleAnalyticsTransformationComplete_Fund
        console.log(LOG_PREFIX, "Fundamentals History controls UI references set. Fields populated. Tickers pending data.");

        // NEW: Check if data is already ready and populate tickers if so
        if (isDataReadyForFund && fhTickerSelect) {
            console.log(LOG_PREFIX, "Data was ready. Populating tickers now during control initialization.");
            populateFundamentalsHistoryTickerSelect();
        } else if (fhTickerSelect) {
            console.log(LOG_PREFIX, "Tickers pending data readiness signal ('AnalyticsTransformComplete').");
            // To handle the case where bootstrap multiselect might have been initialized on an empty select,
            // provide a placeholder if tickers are not yet populated.
            if (typeof $(fhTickerSelect).multiselect === 'function' && !$(fhTickerSelect).data('multiselect')) {
                $(fhTickerSelect).multiselect({
                    buttonWidth: '100%',
                    nonSelectedText: 'Tickers loading...',
                    numberDisplayed: 1
                });
            } else if (typeof $(fhTickerSelect).multiselect === 'function' && $(fhTickerSelect).data('multiselect')) {
                 // Update placeholder if already multiselect
                $(fhTickerSelect).multiselect('setOptions', { nonSelectedText: 'Tickers loading...' });
                $(fhTickerSelect).multiselect('rebuild');
            }
        }
    }

    // NEW function to populate tickers, mirroring analytics_timeseries.js logic
    function populateFundamentalsHistoryTickerSelect() {
        console.log(LOG_PREFIX, "Populating Fundamentals History ticker select...");
        if (!fhTickerSelect) {
            console.error(LOG_PREFIX, "Ticker select (ts-fh-ticker-select) not found when trying to populate.");
            return;
        }

        let analyticsOriginalData = [];
        if (window.AnalyticsMainModule && typeof window.AnalyticsMainModule.getFinalDataForAnalysis === 'function') {
            try {
                const finalDataPackage = window.AnalyticsMainModule.getFinalDataForAnalysis();
                if (Array.isArray(finalDataPackage)) {
                    analyticsOriginalData = finalDataPackage;
                } else if (finalDataPackage && finalDataPackage.originalData && Array.isArray(finalDataPackage.originalData)) {
                    analyticsOriginalData = finalDataPackage.originalData;
                } else {
                    console.warn(LOG_PREFIX, "getFinalDataForAnalysis did not return a direct array or an object with originalData array.");
                }
            } catch (e) {
                console.error(LOG_PREFIX, "Error calling getFinalDataForAnalysis:", e);
            }
        } else {
            console.warn(LOG_PREFIX, "AnalyticsMainModule.getFinalDataForAnalysis() not available for tickers.");
        }

        const uniqueTickers = new Set();
        if (Array.isArray(analyticsOriginalData)) {
            analyticsOriginalData.forEach(item => {
                if (item && item.ticker) {
                    uniqueTickers.add(item.ticker);
                }
            });
        }

        const sortedTickers = Array.from(uniqueTickers).sort();
        
        fhTickerSelect.innerHTML = ''; // Clear existing
        if (sortedTickers.length > 0) {
            // Add a "Select Ticker(s)" or similar placeholder/instruction
            // Since it's a multi-select, a disabled selected "placeholder" is tricky.
            // Bootstrap multiselect usually handles this. If not, users just select.
            // For consistency, let's add a "select all" and "deselect all" if we adopt bootstrap multiselect.
            // For now, just populate.
            sortedTickers.forEach(ticker => {
                const option = document.createElement('option');
                option.value = ticker;
                option.textContent = ticker;
                fhTickerSelect.appendChild(option);
            });
        } else {
            const noTickerOption = document.createElement('option');
            noTickerOption.value = "";
            noTickerOption.textContent = "No tickers in loaded data";
            noTickerOption.disabled = true;
            fhTickerSelect.appendChild(noTickerOption);
        }
        console.log(LOG_PREFIX, `Populated tickers for Fundamentals History: ${sortedTickers.length}`);

        // Initialize Bootstrap Multiselect if available and not already initialized
        if (fhTickerSelect && typeof $(fhTickerSelect).multiselect === 'function') {
            // Check if it's already a multiselect to avoid re-initialization issues
            if (!$(fhTickerSelect).data('multiselect')) {
                 $(fhTickerSelect).multiselect({
                    buttonWidth: '100%',
                    enableFiltering: true,
                    enableCaseInsensitiveFiltering: true,
                    maxHeight: 200,
                    includeSelectAllOption: true,
                    nonSelectedText: 'Select Ticker(s)',
                    numberDisplayed: 1,
                    nSelectedText: ' tickers selected',
                    allSelectedText: 'All tickers selected',
                });
            } else {
                // If already initialized, just rebuild to reflect new options
                $(fhTickerSelect).multiselect('rebuild');
            }
        }
    }

    function populateFieldSelect() {
        if (!fhFieldSelect) {
            console.error(LOG_PREFIX, "Field select element not found for Fundamentals History.");
            return;
        }

        let allFieldsMetadata = {};
        let finalAvailableFields = [];

        if (window.AnalyticsMainModule) {
            if (typeof window.AnalyticsMainModule.getFinalFieldMetadata === 'function') {
                allFieldsMetadata = window.AnalyticsMainModule.getFinalFieldMetadata() || {};
            } else {
                console.warn(LOG_PREFIX, "AnalyticsMainModule.getFinalFieldMetadata() not available.");
            }
            if (typeof window.AnalyticsMainModule.getFinalAvailableFields === 'function') {
                finalAvailableFields = window.AnalyticsMainModule.getFinalAvailableFields() || [];
            } else {
                console.warn(LOG_PREFIX, "AnalyticsMainModule.getFinalAvailableFields() not available.");
            }
        } else {
            console.warn(LOG_PREFIX, "AnalyticsMainModule not available for field population.");
        }

        fhFieldSelect.innerHTML = ''; // Clear existing options
        let populatedCount = 0;

        finalAvailableFields.forEach(fullFieldIdentifier => {
            // const fieldMeta = allFieldsMetadata[fullFieldIdentifier]; // Not used for display anymore
            
            if (fullFieldIdentifier && typeof fullFieldIdentifier === 'string' && fullFieldIdentifier.startsWith('yf_item_')) {
                const option = document.createElement('option');
                option.value = fullFieldIdentifier; 
                
                // MODIFIED: Display name is fullFieldIdentifier with "yf_item_" prefix removed.
                // Example: "yf_item_balance_sheet_annual_Total Assets" becomes "balance_sheet_annual_Total Assets"
                option.textContent = fullFieldIdentifier.substring('yf_item_'.length); 
                
                fhFieldSelect.appendChild(option);
                populatedCount++;
            }
        });

        if (populatedCount === 0) {
            const noFieldOption = document.createElement('option');
            noFieldOption.value = "";
            noFieldOption.textContent = "No fundamental fields available";
            noFieldOption.disabled = true;
            fhFieldSelect.appendChild(noFieldOption);
        }
        console.log(LOG_PREFIX, `Populated fields for Fundamentals History: ${populatedCount}`);
        
        // Initialize Bootstrap Multiselect for fields if not already
        if (fhFieldSelect && typeof $(fhFieldSelect).multiselect === 'function') {
            if (!$(fhFieldSelect).data('multiselect')) {
                $(fhFieldSelect).multiselect({
                    buttonWidth: '100%',
                    enableFiltering: true,
                    enableCaseInsensitiveFiltering: true,
                    filterPlaceholder: 'Search fields...',
                    maxHeight: 250, // Increased height
                    includeSelectAllOption: true,
                    nonSelectedText: 'Select Field(s)',
                    numberDisplayed: 1,
                    nSelectedText: ' fields selected',
                    allSelectedText: 'All fields selected',
                });
            } else {
                $(fhFieldSelect).multiselect('rebuild');
            }
        }
    }

    /**
     * Handles the "Run" button click for the Fundamentals History study.
     * Fetches data from the backend and triggers chart rendering.
     */
    async function handleRunFundamentalsHistory() {
        console.log(LOG_PREFIX, "Run Fundamentals History clicked.");
        
        const loadingIndicator = document.getElementById('timeseries-loading-indicator'); 
        const runButton = document.getElementById('ts-fh-run-study-btn');

        if (!fhTickerSelect || !fhFieldSelect) { // REMOVED: itemType and itemTimeCoverage selects
            alert("Error: Essential configuration elements for Fundamentals History are missing.");
            console.error(LOG_PREFIX, "Missing ticker or field select elements for FH study.");
            return;
        }

        const selectedTickers = $(fhTickerSelect).val() || [];
        // MODIFIED: selectedFieldIdentifiers now contains full identifiers
        const selectedFieldIdentifiers = $(fhFieldSelect).val() || []; 
        
        const startDate = fhStartDateInput ? fhStartDateInput.value : null;
        const endDate = fhEndDateInput ? fhEndDateInput.value : null;
        const chartType = document.getElementById('ts-fh-chart-type').value;

        if (selectedTickers.length === 0) {
            alert("Please select at least one ticker.");
            return;
        }
        // MODIFIED: Check selectedFieldIdentifiers
        if (selectedFieldIdentifiers.length === 0) {
            alert("Please select at least one field.");
            return;
        }
         if (startDate && endDate && new Date(startDate) >= new Date(endDate)) {
            alert("Start Date must be strictly before End Date.");
            return;
        }

        if (loadingIndicator) loadingIndicator.style.display = 'flex';
        if (runButton) runButton.disabled = true;

        // MODIFIED: Request payload now sends field_identifiers
        const requestPayload = {
            tickers: selectedTickers,
            field_identifiers: selectedFieldIdentifiers,
            start_date: startDate || null, 
            end_date: endDate || null,     
        };

        console.log(LOG_PREFIX, "Request Payload:", JSON.stringify(requestPayload, null, 2));

        try {
            const response = await fetch('/api/v3/timeseries/fundamentals_history', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRF-Token': document.getElementById('csrf_token') ? document.getElementById('csrf_token').value : ''
                },
                body: JSON.stringify(requestPayload)
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ detail: "Unknown error during fundamentals fetch." }));
                console.error(LOG_PREFIX, "API Error:", response.status, errorData);
                throw new Error(errorData.detail || `HTTP error ${response.status}`);
            }

            const data = await response.json(); // Expected: {ticker: {field_payload_key: [{date: value}, ...]}}
            console.log(LOG_PREFIX, "API Response Data:", data);

            if (Object.keys(data).length === 0) {
                 showPlaceholderWithMessage("No data returned for the selected fundamentals criteria.");
                 return;
            }
            
            // The renderFundamentalsHistoryChart function will need to understand that selectedFields
            // (passed to it) now effectively means the payload_keys derived from the response structure.
            // The chart title / legend might also need adjustment if it was using itemType/itemCoverage.
            renderFundamentalsHistoryChart(data, chartType, selectedTickers, Object.keys(data[selectedTickers[0]] || {})); // Pass payload_keys
            
        } catch (error) {
            console.error(LOG_PREFIX, "Error fetching or processing fundamentals history:", error);
            showPlaceholderWithMessage(`Error: ${error.message}`);
        } finally {
            if (loadingIndicator) loadingIndicator.style.display = 'none';
            if (runButton) runButton.disabled = false;
        }
    }

    /**
     * Renders the fundamentals history chart.
     * @param {object} apiData - Data from the API {ticker: {field: [{date, value}, ...]}}
     * @param {string} chartType - 'line' or 'bar'
     * @param {string[]} selectedTickers - List of selected tickers
     * @param {string[]} selectedFields - List of selected fields
     */
    function renderFundamentalsHistoryChart(apiData, chartType, selectedTickers, fieldPayloadKeys) {
        console.log(LOG_PREFIX, `Rendering ${chartType} chart for fundamentals. Tickers: ${selectedTickers.join(', ')}, Fields: ${fieldPayloadKeys.join(', ')}`);
        
        const chartCanvas = document.getElementById('ts-chart-canvas');
        if (!chartCanvas) {
            console.error(LOG_PREFIX, "Chart canvas element (ts-chart-canvas) not found.");
            return;
        }
        // const ctx = chartCanvas.getContext('2d'); // Get context inside renderGenericTimeseriesChart

        if (timeseriesChartInstance) {
            timeseriesChartInstance.destroy();
            timeseriesChartInstance = null;
        }
        
        const datasets = [];
        const lineColors = [
            'rgb(75, 192, 192)', 'rgb(255, 99, 132)', 'rgb(54, 162, 235)',
            'rgb(255, 206, 86)', 'rgb(153, 102, 255)', 'rgb(255, 159, 64)',
            '#28a745', '#ffc107', '#dc3545', '#17a2b8', '#6f42c1', '#fd7e14'
        ];
        let colorIndex = 0;

        // Determine all unique dates from the apiData to use as chart labels
        const allDates = new Set();
        selectedTickers.forEach(ticker => {
            if (apiData[ticker]) {
                fieldPayloadKeys.forEach(payloadKey => {
                    if (apiData[ticker][payloadKey]) {
                        apiData[ticker][payloadKey].forEach(point => allDates.add(point.date));
                    }
                });
            }
        });
        const sortedDateStrings = Array.from(allDates).sort((a, b) => new Date(a) - new Date(b));
        const chartLabels = sortedDateStrings.map(dateStr => new Date(dateStr).valueOf()); // Use numeric timestamps for time scale

        selectedTickers.forEach(ticker => {
            if (apiData[ticker]) {
                fieldPayloadKeys.forEach(payloadKey => {
                    if (apiData[ticker][payloadKey] && apiData[ticker][payloadKey].length > 0) {
                        const timeseriesForField = apiData[ticker][payloadKey];
                        
                        // MODIFIED: Map data to {x: timestamp, y: value}
                        // Ensure data points align with unique sorted dates (chartLabels)
                        // chartLabels contains sorted numeric timestamps.
                        const dataPoints = chartLabels.map(labelTimestamp => {
                            const point = timeseriesForField.find(p => new Date(p.date).valueOf() === labelTimestamp);
                            return {
                                x: labelTimestamp, // Use the timestamp for x
                                y: point ? (typeof point.value === 'string' ? parseFloat(point.value) : point.value) : null
                            };
                        });

                        datasets.push({
                            label: `${ticker} - ${payloadKey}`,
                            data: dataPoints,
                            borderColor: lineColors[colorIndex % lineColors.length],
                            backgroundColor: chartType === 'bar' ? lineColors[colorIndex % lineColors.length] : lineColors[colorIndex % lineColors.length].replace('rgb', 'rgba').replace(')', ',0.1)'),
                            borderWidth: chartType === 'bar' ? 0 : 1.5, // No border for bars, or thin border
                            fill: false,
                            pointRadius: chartType === 'line' ? 2 : 0,
                            tension: chartType === 'line' ? 0.1 : 0,
                            spanGaps: chartType === 'line' // Connect lines over null data points for line charts
                        });
                        colorIndex++;
                    }
                });
            }
        });

        if (datasets.length === 0) {
            showPlaceholderWithMessage("No chartable data found after processing fundamentals response.");
            return;
        }

        // Use the generic charting function from analytics_timeseries.js
        if (typeof window.AnalyticsTimeseriesModule !== 'undefined' && 
            typeof window.AnalyticsTimeseriesModule.renderGenericTimeseriesChart === 'function') {
            
            const chartTitle = `Fundamentals History`;
            const yAxisLabel = "Value";

            window.AnalyticsTimeseriesModule.renderGenericTimeseriesChart(
                datasets, 
                chartTitle, 
                yAxisLabel,
                {
                    chartType: chartType, // 'line' or 'bar'
                    isTimeseries: true, // Indicate that x-axis is time
                    // labelsForTimeAxis: chartLabels // This might be redundant if data is {x,y}
                                                      // but leaving it for now doesn't hurt, Chart.js time scale
                                                      // primarily uses the x-values from the data points.
                }
            );
        } else {
            console.error(LOG_PREFIX, "AnalyticsTimeseriesModule.renderGenericTimeseriesChart not found. Cannot display chart.");
            showPlaceholderWithMessage("Charting function for fundamentals is not available. See console.");
        }
    }

    /**
     * Helper to show a message in the chart placeholder area.
     * @param {string} message - The message to display.
     */
    function showPlaceholderWithMessage(message) {
        const placeholder = document.getElementById('ts-chart-placeholder');
        const chartCanvas = document.getElementById('ts-chart-canvas');
        if (placeholder) {
            placeholder.textContent = message;
            placeholder.style.display = 'block';
        }
        if (chartCanvas) {
            chartCanvas.style.display = 'none';
             if (timeseriesChartInstance) {
                timeseriesChartInstance.destroy();
                timeseriesChartInstance = null;
            }
        }
        const resetZoomBtn = document.getElementById('ts-reset-zoom-btn');
        if(resetZoomBtn) resetZoomBtn.style.display = 'none';
    }

    // NEW: Listener for when main analytics data is ready
    function handleAnalyticsTransformationComplete_Fund() {
        console.log(LOG_PREFIX, "'AnalyticsTransformComplete' event received in Fundamentals module.");
        isDataReadyForFund = true; // Set flag

        if (fhTickerSelect) {
            console.log(LOG_PREFIX, "Controls were already initialized. Populating tickers now.");
            populateFundamentalsHistoryTickerSelect();
        } else {
            console.warn(LOG_PREFIX, "Controls not yet initialized by the time 'AnalyticsTransformComplete' was caught. Ticker population will occur when controls are initialized.");
        }
    }

    // --- Expose module functions ---
    window.TimeseriesFundamentalsModule = {
        initializeFundamentalsHistoryStudyControls,
        handleRunFundamentalsHistory
        // renderFundamentalsHistoryChart // Not typically exposed directly, called by handleRun
    };

    console.log(LOG_PREFIX, "Module loaded and exposed.");

    // Add event listener for data readiness
    console.log(LOG_PREFIX, "Adding event listener for 'AnalyticsTransformComplete' to WINDOW.");
    window.addEventListener('AnalyticsTransformComplete', handleAnalyticsTransformationComplete_Fund);

})(); 