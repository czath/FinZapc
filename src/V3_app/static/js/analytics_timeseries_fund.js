(function() {
    "use strict";

    const LOG_PREFIX = "[TimeseriesFundamentalsModule]";
    let timeseriesChartInstance = null; // To hold the Chart.js instance for this module
    let fhTickerSelect = null; // Store globally within this IIFE
    let fhFieldSelect = null; // Store globally
    let fhStartDateInput = null;
    let fhEndDateInput = null;
    let isFundDataReady = false; // Consolidated flag for data readiness

    // NEW: DOM Elements for Price-Fundamental Comparison (PFC)
    let pfcTickerSelect = null;
    let pfcFieldSelect = null;
    let pfcStartDateInput = null;
    let pfcEndDateInput = null;
    let pfcRunButton = null;
    let pfcStatusLabel = null;

    // NEW: DOM Elements for Price-Fundamental Ratios (PFR)
    let pfrTickerSelect = null;
    let pfrFieldSelect = null;
    let pfrPeriodSelector = null;
    let pfrStartDateInput = null;
    let pfrEndDateInput = null;
    let pfrRunButton = null;
    let pfrStatusLabel = null;
    let pfrDisplayModeSelect = null; // NEW: For Display Mode dropdown

    function formatDateToYYYYMMDD(dateObj) {
        const year = dateObj.getFullYear();
        const month = (dateObj.getMonth() + 1).toString().padStart(2, '0');
        const day = dateObj.getDate().toString().padStart(2, '0');
        return `${year}-${month}-${day}`;
    }

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
        if (isFundDataReady && fhTickerSelect) {
            console.log(LOG_PREFIX, "Data was ready for FH. Populating tickers now during control initialization.");
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
        isFundDataReady = true; // Set flag

        if (fhTickerSelect) {
            console.log(LOG_PREFIX, "FH Controls were initialized or became available. Populating FH tickers.");
            populateFundamentalsHistoryTickerSelect();
            // Also ensure FH fields are populated/rebuilt if they were waiting for data
            if (fhFieldSelect && (!$(fhFieldSelect).data('multiselect') || $(fhFieldSelect).find('option').length <=1 ) ){
                 console.log(LOG_PREFIX, "Populating/rebuilding FH fields as data is now ready.");
                 populateFieldSelect(); // This is the FH field populator
            }
        } else {
            console.warn(LOG_PREFIX, "FH Ticker Select not available when 'AnalyticsTransformComplete' was caught.");
        }

        // NEW: Populate PFC fields if its select element is ready
        if (pfcFieldSelect) {
            console.log(LOG_PREFIX, "PFC Field Select is available. Populating PFC fields as data is now ready.");
            populatePfcFieldSelect();
        } else {
            console.warn(LOG_PREFIX, "PFC Field Select not available when 'AnalyticsTransformComplete' was caught. Fields will populate upon PFC pane initialization.");
        }
    }

    // --- NEW: Functions for Price-Fundamental Comparison Study ---
    function initializePriceFundamentalComparisonControls() {
        console.log(LOG_PREFIX, "Initializing Price-Fundamental Comparison (PFC) study controls...");

        pfcTickerSelect = document.getElementById('ts-pfc-ticker-select');
        pfcFieldSelect = document.getElementById('ts-pfc-field-select');
        pfcStartDateInput = document.getElementById('ts-pfc-start-date');
        pfcEndDateInput = document.getElementById('ts-pfc-end-date');
        pfcRunButton = document.getElementById('ts-pfc-run-study-btn');
        pfcStatusLabel = document.getElementById('ts-pfc-status');

        if (!pfcTickerSelect) console.error(LOG_PREFIX, "PFC Ticker select (ts-pfc-ticker-select) not found!");
        if (!pfcFieldSelect) console.error(LOG_PREFIX, "PFC Field select (ts-pfc-field-select) not found!");
        if (!pfcRunButton) console.error(LOG_PREFIX, "PFC Run button (ts-pfc-run-study-btn) not found!");
        if (!pfcStatusLabel) console.warn(LOG_PREFIX, "PFC Status label (ts-pfc-status) not found.");

        // Populate fundamental fields (tickers are populated by analytics_timeseries.js)
        populatePfcFieldSelect();

        if (pfcRunButton) {
            pfcRunButton.addEventListener('click', handleRunPriceFundamentalComparison);
        } else {
            console.error(LOG_PREFIX, "PFC Run button not found, cannot attach event listener.");
        }
        console.log(LOG_PREFIX, "PFC controls initialized and field select populated.");
    }

    function populatePfcFieldSelect() {
        if (!pfcFieldSelect) {
            console.error(LOG_PREFIX, "PFC Field select element not found.");
            return;
        }

        let allFieldsMetadata = {};
        let finalAvailableFields = [];

        if (window.AnalyticsMainModule) {
            if (typeof window.AnalyticsMainModule.getFinalFieldMetadata === 'function') {
                allFieldsMetadata = window.AnalyticsMainModule.getFinalFieldMetadata() || {};
            }
            if (typeof window.AnalyticsMainModule.getFinalAvailableFields === 'function') {
                finalAvailableFields = window.AnalyticsMainModule.getFinalAvailableFields() || [];
            }
        }

        pfcFieldSelect.innerHTML = ''; // Clear existing options
        let populatedCount = 0;

        finalAvailableFields.forEach(fullFieldIdentifier => {
            if (fullFieldIdentifier && typeof fullFieldIdentifier === 'string' && fullFieldIdentifier.startsWith('yf_item_')) {
                const option = document.createElement('option');
                option.value = fullFieldIdentifier;
                option.textContent = fullFieldIdentifier.substring('yf_item_'.length);
                pfcFieldSelect.appendChild(option);
                populatedCount++;
            }
        });

        if (populatedCount === 0) {
            const noFieldOption = document.createElement('option');
            noFieldOption.value = "";
            noFieldOption.textContent = "No fundamental fields available";
            noFieldOption.disabled = true;
            pfcFieldSelect.appendChild(noFieldOption);
        } else {
             // Initialize Bootstrap Multiselect for PFC fields if available
            if (typeof $(pfcFieldSelect).multiselect === 'function') {
                if (!$(pfcFieldSelect).data('multiselect')) {
                    $(pfcFieldSelect).multiselect({
                        buttonWidth: '100%',
                        enableFiltering: true,
                        enableCaseInsensitiveFiltering: true,
                        filterPlaceholder: 'Search fields...',
                        maxHeight: 200,
                        includeSelectAllOption: true,
                        nonSelectedText: 'Select Fundamental Field(s)',
                        numberDisplayed: 1,
                        nSelectedText: ' fields selected',
                        allSelectedText: 'All fields selected',
                    });
                } else {
                    $(pfcFieldSelect).multiselect('rebuild');
                }
            }
        }
        console.log(LOG_PREFIX, `Populated fields for PFC: ${populatedCount}`);
    }

    async function handleRunPriceFundamentalComparison() {
        console.log(LOG_PREFIX, "Run Price-Fundamental Comparison clicked.");

        if (!pfcTickerSelect || !pfcFieldSelect || !pfcStartDateInput || !pfcEndDateInput || !pfcRunButton) {
            alert("Error: Essential configuration elements for PFC study are missing.");
            console.error(LOG_PREFIX, "Missing PFC UI elements.");
            return;
        }

        const selectedTicker = pfcTickerSelect.value;
        const selectedFundamentalFields = $(pfcFieldSelect).val() || [];
        let startDate = pfcStartDateInput.value;
        let endDate = pfcEndDateInput.value;

        if (!selectedTicker) {
            alert("Please select a ticker.");
            return;
        }
        if (selectedFundamentalFields.length === 0) {
            alert("Please select at least one fundamental field.");
            return;
        }

        // Default date range to 1 year if none provided
        if (!startDate && !endDate) {
            const today = new Date();
            endDate = today.toISOString().split('T')[0];
            const oneYearAgo = new Date(today.setFullYear(today.getFullYear() - 1));
            startDate = oneYearAgo.toISOString().split('T')[0];
            console.log(LOG_PREFIX, `PFC: No dates provided, defaulting to 1 year: ${startDate} to ${endDate}`);
        } else if (!startDate || !endDate) {
            alert("Please provide both Start and End dates, or leave both blank for default (1 year).");
            return;
        }

        if (new Date(startDate) >= new Date(endDate)) {
            alert("Start Date must be strictly before End Date.");
            return;
        }

        if (pfcStatusLabel) pfcStatusLabel.textContent = 'Fetching data...';
        if (pfcRunButton) {
            pfcRunButton.disabled = true;
            pfcRunButton.querySelector('.spinner-border').style.display = 'inline-block';
            pfcRunButton.querySelector('.button-text').textContent = 'Loading...';
        }
        
        const loadingIndicator = document.getElementById('timeseries-loading-indicator'); 
        if (loadingIndicator) loadingIndicator.style.display = 'flex';

        try {
            // 1. Fetch Price Data
            const priceApiEndDate = new Date(endDate);
            priceApiEndDate.setDate(priceApiEndDate.getDate() + 1); // Yahoo API is exclusive for end_date
            const priceApiParams = `ticker=${encodeURIComponent(selectedTicker)}&interval=1d&start_date=${encodeURIComponent(startDate)}&end_date=${encodeURIComponent(priceApiEndDate.toISOString().split('T')[0])}`;
            const priceApiUrl = `/api/v3/timeseries/price_history?${priceApiParams}`;
            console.log(LOG_PREFIX, "PFC Fetching Price History from:", priceApiUrl);
            const priceResponse = await fetch(priceApiUrl);
            if (!priceResponse.ok) {
                const err = await priceResponse.json().catch(() => ({detail: `Price data fetch failed (${priceResponse.status})`}));
                throw new Error(err.detail);
            }
            const priceDataRaw = await priceResponse.json();
            console.log(LOG_PREFIX, "PFC Price Data Received:", priceDataRaw.length);

            // 2. Fetch Fundamental Data
            const fundamentalsRequestPayload = {
                tickers: [selectedTicker],
                field_identifiers: selectedFundamentalFields,
                start_date: startDate,
                end_date: endDate,
            };
            console.log(LOG_PREFIX, "PFC Fetching Fundamentals History with payload:", fundamentalsRequestPayload);
            const fundamentalsResponse = await fetch('/api/v3/timeseries/fundamentals_history', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRF-Token': document.getElementById('csrf_token') ? document.getElementById('csrf_token').value : ''
                },
                body: JSON.stringify(fundamentalsRequestPayload)
            });
            if (!fundamentalsResponse.ok) {
                const err = await fundamentalsResponse.json().catch(() => ({detail: `Fundamentals data fetch failed (${fundamentalsResponse.status})`}));
                throw new Error(err.detail);
            }
            const fundamentalDataRaw = await fundamentalsResponse.json();
            console.log(LOG_PREFIX, "PFC Fundamental Data Received:", fundamentalDataRaw);

            // 3. Process and Prepare Datasets
            const datasets = [];
            const lineColors = [
                'rgb(255, 99, 132)', 'rgb(54, 162, 235)', 'rgb(255, 206, 86)', 
                'rgb(75, 192, 192)', 'rgb(153, 102, 255)', 'rgb(255, 159, 64)'
            ]; // Price will be the first, fundamentals will follow

            // Price Dataset (Y-Axis 1: Left)
            if (priceDataRaw && priceDataRaw.length > 0) {
                const priceChartData = priceDataRaw.map(d => ({
                    x: new Date(d.Datetime || d.Date).valueOf(),
                    y: d.Close
                }));
                datasets.push({
                    label: `${selectedTicker} Price (Close)`,
                    data: priceChartData,
                    borderColor: 'rgb(0, 123, 255)', // Primary blue for price
                    backgroundColor: 'rgba(0, 123, 255, 0.1)',
                    yAxisID: 'y-axis-price',
                    type: 'line',
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.1
                });
            } else {
                 console.warn(LOG_PREFIX, "PFC: No price data returned.");
            }

            // Fundamental Datasets (Y-Axis 2: Right)
            let fundamentalColorIndex = 0;
            if (fundamentalDataRaw && fundamentalDataRaw[selectedTicker]) {
                console.log(LOG_PREFIX, "PFC: Available fundamental keys for", selectedTicker, ":", Object.keys(fundamentalDataRaw[selectedTicker]));
                
                // Iterate over the keys *returned by the API* for the given ticker
                for (const returnedKey in fundamentalDataRaw[selectedTicker]) {
                    // Ensure this key corresponds to a field the user actually selected if strict adherence is needed.
                    // For now, let's assume the API only returns data for fields included in `selectedFundamentalFields` request.
                    // If not, we might need to match `returnedKey` back to the `selectedFundamentalFields` list.
                    // Example: `selectedFundamentalFields.some(selField => selField.includes(returnedKey.replace(/_/g, ' ')))`

                    const seriesData = fundamentalDataRaw[selectedTicker][returnedKey];
                    const displayFieldName = returnedKey.replace(/_/g, ' '); // Use the returned key for display name

                    if (seriesData && seriesData.length > 0) {
                        const fundamentalChartData = seriesData.map(d => ({
                            x: new Date(d.date).valueOf(), 
                            y: typeof d.value === 'string' ? parseFloat(d.value) : d.value
                        }));                        
                        datasets.push({
                            label: `${displayFieldName} (${selectedTicker})`,
                            data: fundamentalChartData,
                            borderColor: lineColors[fundamentalColorIndex % lineColors.length],
                            backgroundColor: lineColors[fundamentalColorIndex % lineColors.length].replace('rgb', 'rgba').replace(')', ',0.1)'),
                            yAxisID: 'y-axis-fundamental',
                            type: 'line',
                            borderWidth: 1.5,
                            pointRadius: 2,
                            stepped: 'before', // Show fundamental value held until next report
                            tension: 0
                        });
                        fundamentalColorIndex++;
                    } else {
                        console.warn(LOG_PREFIX, `PFC: No data for fundamental field ${returnedKey}`);
                    }
                }
            }

            if (datasets.length === 0) {
                window.AnalyticsTimeseriesModule.showPlaceholderWithMessage("No data available to plot for Price-Fundamental Comparison.");
                return;
            }

            // 4. Render Chart
            const chartTitle = `${selectedTicker}: Price vs Fundamentals`;
            const yAxesConfig = [
                { id: 'y-axis-price', position: 'left', title: 'Price (USD)' },
                { id: 'y-axis-fundamental', position: 'right', title: 'Fundamental Value', grid: { drawOnChartArea: false } }
            ];

            window.AnalyticsTimeseriesModule.renderGenericTimeseriesChart(
                datasets,
                chartTitle,
                null, // yAxisLabel is not needed when yAxesConfig is used
                {
                    chartType: 'line', // Overall chart will be line, individual datasets define their type if mixed
                    isTimeseries: true,
                    yAxesConfig: yAxesConfig,
                    rangeDetails: {start: startDate, end: endDate} // For subtitle
                }
            );
             if (pfcStatusLabel) pfcStatusLabel.textContent = `Chart loaded. Ticker: ${selectedTicker}, Range: ${startDate} to ${endDate}.`;

        } catch (error) {
            console.error(LOG_PREFIX, "Error during Price-Fundamental Comparison:", error);
            alert(`Error: ${error.message}`);
            window.AnalyticsTimeseriesModule.showPlaceholderWithMessage(`Error: ${error.message}`);
            if (pfcStatusLabel) pfcStatusLabel.textContent = `Error: ${error.message}`;
        } finally {
            if (loadingIndicator) loadingIndicator.style.display = 'none';
            if (pfcRunButton) {
                pfcRunButton.disabled = false;
                pfcRunButton.querySelector('.spinner-border').style.display = 'none';
                pfcRunButton.querySelector('.button-text').textContent = 'Run Comparison';
            }
        }
    }

    // --- NEW: Functions for Price-Fundamental Ratios Study (PFR) ---
    function initializePriceFundamentalRatiosControls() {
        console.log(LOG_PREFIX, "Initializing Price-Fundamental Ratios (PFR) study controls...");

        pfrTickerSelect = document.getElementById('ts-pfr-ticker-select');
        pfrFieldSelect = document.getElementById('ts-pfr-field-select');
        pfrPeriodSelector = document.getElementById('ts-pfr-period-selector'); 
        pfrStartDateInput = document.getElementById('ts-pfr-start-date');
        pfrEndDateInput = document.getElementById('ts-pfr-end-date');
        pfrRunButton = document.getElementById('ts-pfr-run-study-btn');
        pfrStatusLabel = document.getElementById('ts-pfr-status');
        pfrDisplayModeSelect = document.getElementById('ts-pfr-display-mode-select'); // NEW

        if (!pfrTickerSelect) console.error(LOG_PREFIX, "PFR Ticker select not found!");
        if (!pfrFieldSelect) console.error(LOG_PREFIX, "PFR Field select not found!");
        if (!pfrRunButton) console.error(LOG_PREFIX, "PFR Run button not found!");
        if (!pfrDisplayModeSelect) console.warn(LOG_PREFIX, "PFR Display Mode select (ts-pfr-display-mode-select) not found!"); // NEW

        // Populate fundamental fields (tickers populated by analytics_timeseries.js)
        if (isFundDataReady && pfrFieldSelect) {
            populatePfrFieldSelect();
        } else if (pfrFieldSelect) {
            console.log(LOG_PREFIX, "PFR Fields pending data readiness signal ('AnalyticsTransformComplete').");
            if (typeof $(pfrFieldSelect).multiselect === 'function' && !$(pfrFieldSelect).data('multiselect')) {
                $(pfrFieldSelect).multiselect({
                    buttonWidth: '100%',
                    nonSelectedText: 'Fields loading...',
                    numberDisplayed: 1 });
            } else if (typeof $(pfrFieldSelect).multiselect === 'function' && $(pfrFieldSelect).data('multiselect')) {
                $(pfrFieldSelect).multiselect('setOptions', { nonSelectedText: 'Fields loading...' });
                $(pfrFieldSelect).multiselect('rebuild');
            }
        }

        if (pfrRunButton) {
            pfrRunButton.addEventListener('click', handleRunPriceFundamentalRatios);
        }
        console.log(LOG_PREFIX, "PFR controls initialized.");
    }

    function populatePfrFieldSelect() {
        if (!pfrFieldSelect) {
            console.error(LOG_PREFIX, "PFR Field select element for PFR study not found.");
            return;
        }
        // This logic is identical to populatePfcFieldSelect, could be refactored later if needed
        let finalAvailableFields = (window.AnalyticsMainModule && typeof window.AnalyticsMainModule.getFinalAvailableFields === 'function') ? 
                                 window.AnalyticsMainModule.getFinalAvailableFields() || [] : [];
        
        pfrFieldSelect.innerHTML = ''; 
        let populatedCount = 0;
        finalAvailableFields.forEach(fullFieldIdentifier => {
            if (fullFieldIdentifier && typeof fullFieldIdentifier === 'string' && fullFieldIdentifier.startsWith('yf_item_')) {
                const option = document.createElement('option');
                option.value = fullFieldIdentifier;
                option.textContent = fullFieldIdentifier.substring('yf_item_'.length);
                pfrFieldSelect.appendChild(option);
                populatedCount++;
            }
        });

        if (populatedCount === 0) {
            pfrFieldSelect.appendChild(new Option("No fundamental fields available", "", false, false).disabled = true);
        } else {
            if (typeof $(pfrFieldSelect).multiselect === 'function') {
                if (!$(pfrFieldSelect).data('multiselect')) {
                    $(pfrFieldSelect).multiselect({
                        buttonWidth: '100%', enableFiltering: true, enableCaseInsensitiveFiltering: true,
                        filterPlaceholder: 'Search fields...', maxHeight: 200, includeSelectAllOption: true,
                        nonSelectedText: 'Select Fundamental Field(s)', numberDisplayed: 1,
                        nSelectedText: ' fields selected', allSelectedText: 'All fields selected',
                    });
                } else { $(pfrFieldSelect).multiselect('rebuild'); }
            }
        }
        console.log(LOG_PREFIX, `Populated fields for PFR: ${populatedCount}`);
    }

    async function handleRunPriceFundamentalRatios() {
        console.log(LOG_PREFIX, "Run Price-Fundamental Ratios (PFR) clicked.");

        if (!pfrTickerSelect || !pfrFieldSelect || !pfrPeriodSelector || !pfrStartDateInput || !pfrEndDateInput || !pfrRunButton || !pfrDisplayModeSelect) { // Added pfrDisplayModeSelect check
            alert("Error: Essential PFR UI elements are missing."); return;
        }

        const selectedTickers = $(pfrTickerSelect).val() || [];
        const selectedFundamentalFields = $(pfrFieldSelect).val() || [];
        const selectedPeriod = pfrPeriodSelector.value;
        const displayMode = pfrDisplayModeSelect.value; // NEW: Get display mode
        let startDate = pfrStartDateInput.value;
        let endDate = pfrEndDateInput.value;

        console.log(LOG_PREFIX, `PFR: Display mode selected: ${displayMode}`); // Log the display mode

        if (selectedTickers.length === 0) { alert("Please select at least one ticker."); return; }
        if (selectedFundamentalFields.length === 0) { alert("Please select at least one fundamental field."); return; }

        if (selectedPeriod === 'custom') {
            if (!startDate || !endDate) { alert("For custom range, please select Start and End dates."); return; }
            if (new Date(startDate) >= new Date(endDate)) { alert("Start Date must be before End Date."); return; }
        } else {
            const today = new Date();
            endDate = formatDateToYYYYMMDD(today); 
            let sDateObj = new Date(); 

            switch (selectedPeriod) {
                case '1d': sDateObj.setDate(today.getDate() - 1); break;
                case '5d': sDateObj.setDate(today.getDate() - 5); break;
                case '1mo': sDateObj.setMonth(today.getMonth() - 1); break;
                case '3mo': sDateObj.setMonth(today.getMonth() - 3); break;
                case '6mo': sDateObj.setMonth(today.getMonth() - 6); break;
                case '1y': sDateObj.setFullYear(today.getFullYear() - 1); break;
                case '2y': sDateObj.setFullYear(today.getFullYear() - 2); break;
                case '5y': sDateObj.setFullYear(today.getFullYear() - 5); break;
                case '10y': sDateObj.setFullYear(today.getFullYear() - 10); break;
                case 'ytd': sDateObj = new Date(today.getFullYear(), 0, 1); break;
                default: sDateObj.setFullYear(today.getFullYear() - 1); 
            }
            startDate = formatDateToYYYYMMDD(sDateObj); 

             if (selectedPeriod === 'max') { 
                startDate = null; endDate = null;
            }
            console.log(LOG_PREFIX, `PFR: Period '${selectedPeriod}' selected. Calculated dates: ${startDate} to ${endDate}`);
        }

        if (pfrStatusLabel) pfrStatusLabel.textContent = 'Fetching data...';
        if (pfrRunButton) { pfrRunButton.disabled = true; pfrRunButton.querySelector('.spinner-border').style.display = 'inline-block'; pfrRunButton.querySelector('.button-text').textContent = 'Loading...'; }
        const loadingIndicator = document.getElementById('timeseries-loading-indicator'); 
        if (loadingIndicator) loadingIndicator.style.display = 'flex';

        try {
            const allChartDatasets = [];
            const priceDataCache = {}; 
            const fundamentalDataCache = {}; 

            const dataFetchingPromises = [];

            selectedTickers.forEach(ticker => {
                if (startDate && endDate) { 
                    const priceApiEndDate = new Date(endDate);
                    priceApiEndDate.setDate(priceApiEndDate.getDate() + 1);
                    const priceApiParams = `ticker=${encodeURIComponent(ticker)}&interval=1d&start_date=${encodeURIComponent(startDate)}&end_date=${encodeURIComponent(priceApiEndDate.toISOString().split('T')[0])}`;
                    dataFetchingPromises.push(
                        fetch(`/api/v3/timeseries/price_history?${priceApiParams}`)
                            .then(response => {
                                if (!response.ok) return response.json().then(err => Promise.reject({ ticker, type: 'price', detail: err.detail || `Price fetch failed (${response.status})`}));
                                return response.json();
                            })
                            .then(data => ({ ticker, type: 'price', data }))
                            .catch(error => ({ ticker, type: 'price', error: error.detail || error.message || 'Unknown price fetch error' }))
                    );
                } else if (selectedPeriod === 'max') {
                    const priceApiParams = `ticker=${encodeURIComponent(ticker)}&interval=1d&period=max`;
                    dataFetchingPromises.push(
                        fetch(`/api/v3/timeseries/price_history?${priceApiParams}`)
                            .then(response => {
                                if (!response.ok) return response.json().then(err => Promise.reject({ ticker, type: 'price', detail: err.detail || `Price fetch (max) failed (${response.status})`}));
                                return response.json();
                            })
                            .then(data => ({ ticker, type: 'price', data }))
                            .catch(error => ({ ticker, type: 'price', error: error.detail || error.message || 'Unknown price fetch error' }))
                    );
                } 

                // Determine the start date for the fundamental data query
                let fundamentalQueryStartDate = startDate;
                if (startDate) { // If not 'max' period, adjust the start date for fundamentals
                    const periodStartDateObj = new Date(startDate);
                    // Go back 2 years to ensure we capture the fundamental report active at the start of the period
                    periodStartDateObj.setFullYear(periodStartDateObj.getFullYear() - 2);
                    fundamentalQueryStartDate = formatDateToYYYYMMDD(periodStartDateObj);
                    console.log(LOG_PREFIX, `PFR: Original period start: ${startDate}, Adjusted fundamental query start: ${fundamentalQueryStartDate}`);
                }

                const fundamentalsRequestPayload = {
                    tickers: [ticker], 
                    field_identifiers: selectedFundamentalFields,
                    start_date: fundamentalQueryStartDate, // Use the adjusted earlier start date for fundamentals
                    end_date: endDate,   
                };
                dataFetchingPromises.push(
                    fetch('/api/v3/timeseries/fundamentals_history', {
                        method: 'POST', headers: {'Content-Type': 'application/json', 'X-CSRF-Token': document.getElementById('csrf_token')?.value || ''},
                        body: JSON.stringify(fundamentalsRequestPayload)
                    })
                    .then(response => {
                        if (!response.ok) return response.json().then(err => Promise.reject({ ticker, type: 'fundamental', detail: err.detail || `Fundamental fetch failed (${response.status})`}));
                        return response.json();
                    })
                    .then(data => ({ ticker, type: 'fundamental', data })) 
                    .catch(error => ({ ticker, type: 'fundamental', error: error.detail || error.message || 'Unknown fundamental fetch error' }))
                );
            });

            const results = await Promise.allSettled(dataFetchingPromises);
            console.log(LOG_PREFIX, "PFR: All data fetch results:", results);

            let fetchErrors = [];
            results.forEach(result => {
                if (result.status === 'fulfilled') {
                    const res = result.value;
                    if (res.error) {
                        fetchErrors.push(`${res.ticker} (${res.type}): ${res.error}`);
                    } else {
                        if (res.type === 'price') {
                            priceDataCache[res.ticker] = res.data;
                        } else if (res.type === 'fundamental') {
                            if (res.data && res.data[res.ticker]) {
                                fundamentalDataCache[res.ticker] = res.data[res.ticker];
                            } else {
                                console.warn(LOG_PREFIX, `Fundamental data for ${res.ticker} was not in the expected format or empty.`);
                            }
                        }
                    }
                } else { 
                    fetchErrors.push(`${result.reason.ticker || 'Unknown ticker'} (${result.reason.type || 'Unknown type'}): ${result.reason.detail || result.reason.message || 'Request failed'}`);
                }
            });

            if (fetchErrors.length > 0) {
                alert("Some data could not be fetched for PFR study:\n" + fetchErrors.join("\n"));
            }
            
            const lineColors = [
                'rgb(75, 192, 192)', 'rgb(255, 99, 132)', 'rgb(54, 162, 235)',
                'rgb(255, 206, 86)', 'rgb(153, 102, 255)', 'rgb(255, 159, 64)',
                '#28a745', '#ffc107', '#dc3545', '#17a2b8', '#6f42c1', '#fd7e14'
            ];
            let colorIndex = 0;

            selectedTickers.forEach(ticker => {
                const tickerPriceData = priceDataCache[ticker];
                const tickerFundamentalDataGroup = fundamentalDataCache[ticker]; 

                if (!tickerPriceData || tickerPriceData.length === 0) {
                    console.warn(LOG_PREFIX, `PFR: No price data for ${ticker}, skipping ratio calculation.`);
                    return; 
                }
                if (!tickerFundamentalDataGroup || Object.keys(tickerFundamentalDataGroup).length === 0) {
                    console.warn(LOG_PREFIX, `PFR: No fundamental data (or empty object) for ${ticker} in cache, skipping ratio calculation.`);
                    return; 
                }

                // Iterate directly over the keys returned by the API for the fundamental data
                for (const actualFundKey in tickerFundamentalDataGroup) {
                    if (Object.hasOwnProperty.call(tickerFundamentalDataGroup, actualFundKey)) {
                        const fundSeriesData = tickerFundamentalDataGroup[actualFundKey];

                        if (!fundSeriesData || fundSeriesData.length === 0) {
                            console.warn(LOG_PREFIX, `PFR: No data points for ${ticker} - ${actualFundKey}, skipping ratio for this field.`);
                            continue; // Skip to the next fundamental field in the API response
                        }

                        const sortedFundData = [...fundSeriesData].sort((a, b) => new Date(a.date) - new Date(b.date));
                        const ratioDataPoints = [];
                        let currentFundamentalValue = null;
                        let currentFundamentalDateEpoch = -1;

                        const firstPriceDateEpoch = new Date(tickerPriceData[0].Datetime || tickerPriceData[0].Date).valueOf();
                        for (let i = sortedFundData.length - 1; i >= 0; i--) {
                            const fundDateEpoch = new Date(sortedFundData[i].date).valueOf();
                            if (fundDateEpoch <= firstPriceDateEpoch) {
                                currentFundamentalValue = typeof sortedFundData[i].value === 'string' ? parseFloat(sortedFundData[i].value) : sortedFundData[i].value;
                                currentFundamentalDateEpoch = fundDateEpoch;
                                break;
                            }
                        }

                        tickerPriceData.forEach(pricePoint => {
                            const priceDateEpoch = new Date(pricePoint.Datetime || pricePoint.Date).valueOf();
                            const priceValue = pricePoint.Close;
                            const priceDateReadable = new Date(priceDateEpoch).toISOString().split('T')[0]; // For logging

                            for (const fundEntry of sortedFundData) {
                                const fundDateEpoch = new Date(fundEntry.date).valueOf();
                                if (fundDateEpoch <= priceDateEpoch && fundDateEpoch > currentFundamentalDateEpoch) {
                                    currentFundamentalValue = typeof fundEntry.value === 'string' ? parseFloat(fundEntry.value) : fundEntry.value;
                                    currentFundamentalDateEpoch = fundDateEpoch;
                                } else if (fundDateEpoch > priceDateEpoch) {
                                    break; 
                                }
                            }

                            if (currentFundamentalValue !== null && currentFundamentalValue !== 0 && !isNaN(currentFundamentalValue) && priceValue !== null && !isNaN(priceValue)) {
                                const ratio = priceValue / currentFundamentalValue;
                                console.log(LOG_PREFIX, `PFR Calc: ${ticker} - ${actualFundKey} | Date: ${priceDateReadable} | Price: ${priceValue} | Fund Value: ${currentFundamentalValue} (from ${new Date(currentFundamentalDateEpoch).toISOString().split('T')[0]}) | Ratio: ${ratio.toFixed(4)}`);
                                ratioDataPoints.push({ x: priceDateEpoch, y: ratio });
                            } else {
                                console.log(LOG_PREFIX, `PFR Calc SKIP: ${ticker} - ${actualFundKey} | Date: ${priceDateReadable} | Price: ${priceValue} | Fund Value: ${currentFundamentalValue} (from ${currentFundamentalDateEpoch > 0 ? new Date(currentFundamentalDateEpoch).toISOString().split('T')[0] : 'N/A'}) | Reason: Invalid price or fund value for ratio.`);
                                ratioDataPoints.push({ x: priceDateEpoch, y: null }); 
                            }
                        });

                        // NEW: Transform data if displayMode is 'percent_change'
                        if (displayMode === 'percent_change' && ratioDataPoints.length > 0) {
                            let baselineRatio = null;
                            for (const point of ratioDataPoints) {
                                if (point.y !== null && !isNaN(point.y)) {
                                    baselineRatio = point.y;
                                    break;
                                }
                            }

                            if (baselineRatio !== null && baselineRatio !== 0) {
                                for (let i = 0; i < ratioDataPoints.length; i++) {
                                    if (ratioDataPoints[i].y !== null && !isNaN(ratioDataPoints[i].y)) {
                                        ratioDataPoints[i].y = ((ratioDataPoints[i].y / baselineRatio) - 1) * 100;
                                    } else {
                                        ratioDataPoints[i].y = null; 
                                    }
                                }
                            } else {
                                for (let i = 0; i < ratioDataPoints.length; i++) {
                                    ratioDataPoints[i].y = null;
                                }
                            }
                        }

                        if (ratioDataPoints.length > 0) {
                            const displayFundName = actualFundKey.replace(/_/g, ' '); 
                            allChartDatasets.push({
                                label: `${ticker} P/${displayFundName}` + (displayMode === 'percent_change' ? ' (% Chg)' : ''), // Append to label
                                data: ratioDataPoints,
                                borderColor: lineColors[colorIndex % lineColors.length],
                                backgroundColor: lineColors[colorIndex % lineColors.length].replace('rgb', 'rgba').replace(')', ',0.05)'), 
                                yAxisID: 'y-axis-ratio', 
                                type: 'line',
                                borderWidth: 1.5,
                                pointRadius: 0,
                                tension: 0.1,
                                appDataType: displayMode // NEW: Pass data type for tooltip/pill formatting
                            });
                            colorIndex++;
                        }
                    }
                }
            });

            if (allChartDatasets.length === 0) {
                window.AnalyticsTimeseriesModule.showPlaceholderWithMessage("No ratio data could be calculated or plotted.");
                 if (pfrStatusLabel) pfrStatusLabel.textContent = "No ratio data generated.";
                return;
            }

            // Determine Y-axis title and chart title suffix based on display mode
            let yAxisTitle = "Ratio Value";
            let chartTitleSuffix = "";
            if (displayMode === 'percent_change') {
                yAxisTitle = "% Change from Baseline";
                chartTitleSuffix = " (% Change)";
            }

            const finalChartTitle = (selectedTickers.length > 1 ? `Price-Fundamental Ratios` : `${selectedTickers[0]} Price-Fundamental Ratios`) + chartTitleSuffix;
            const yAxesConfig = [{ id: 'y-axis-ratio', position: 'left', title: yAxisTitle }]; 
            
            let overallStartDate = startDate;
            let overallEndDate = endDate;
            if (selectedPeriod === 'max' && priceDataCache[selectedTickers[0]] && priceDataCache[selectedTickers[0]].length > 0) {
                const firstTickerPrices = priceDataCache[selectedTickers[0]];
                overallStartDate = formatDateToYYYYMMDD(new Date(firstTickerPrices[0].Datetime || firstTickerPrices[0].Date));
                overallEndDate = formatDateToYYYYMMDD(new Date(firstTickerPrices[firstTickerPrices.length - 1].Datetime || firstTickerPrices[firstTickerPrices.length - 1].Date));
            }

            window.AnalyticsTimeseriesModule.renderGenericTimeseriesChart(allChartDatasets, finalChartTitle, null, 
                { chartType: 'line', isTimeseries: true, yAxesConfig: yAxesConfig, rangeDetails: {start: overallStartDate, end: overallEndDate} }
            );
            if (pfrStatusLabel) pfrStatusLabel.textContent = `PFR Chart Loaded. Tickers: ${selectedTickers.join(', ')}. Range: ${overallStartDate || 'N/A'} to ${overallEndDate || 'N/A'}.`;

        } catch (error) {
            console.error(LOG_PREFIX, "Error during Price-Fundamental Ratios:", error);
            alert(`Error: ${error.message}`);
            window.AnalyticsTimeseriesModule.showPlaceholderWithMessage(`Error: ${error.message}`);
            if (pfrStatusLabel) pfrStatusLabel.textContent = `Error: ${error.message}`;
        } finally {
            if (loadingIndicator) loadingIndicator.style.display = 'none';
            if (pfrRunButton) { pfrRunButton.disabled = false; pfrRunButton.querySelector('.spinner-border').style.display = 'none'; pfrRunButton.querySelector('.button-text').textContent = 'Calculate Ratios'; }
        }
    }

    // --- Expose module functions ---
    window.TimeseriesFundamentalsModule = {
        initializeFundamentalsHistoryStudyControls,
        handleRunFundamentalsHistory,
        initializePriceFundamentalComparisonControls,
        initializePriceFundamentalRatiosControls // NEW: Expose PFR init
    };

    console.log(LOG_PREFIX, "Module loaded and exposed.");

    // Add event listener for data readiness
    console.log(LOG_PREFIX, "Adding event listener for 'AnalyticsTransformComplete' to WINDOW.");
    window.addEventListener('AnalyticsTransformComplete', handleAnalyticsTransformationComplete_Fund);

})(); 