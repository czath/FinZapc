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

    // --- Expose module functions ---
    window.TimeseriesFundamentalsModule = {
        initializeFundamentalsHistoryStudyControls,
        handleRunFundamentalsHistory,
        initializePriceFundamentalComparisonControls // NEW: Expose PFC init
        // renderFundamentalsHistoryChart // Not typically exposed directly, called by handleRun
    };

    console.log(LOG_PREFIX, "Module loaded and exposed.");

    // Add event listener for data readiness
    console.log(LOG_PREFIX, "Adding event listener for 'AnalyticsTransformComplete' to WINDOW.");
    window.addEventListener('AnalyticsTransformComplete', handleAnalyticsTransformationComplete_Fund);

})(); 