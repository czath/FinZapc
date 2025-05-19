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
    let pfrTtmToggle = null; // NEW: Declare variable for TTM Toggle

    // NEW: DOM Elements for Synthetic Fundamentals (SF)
    let sfTickerSelect = null;
    let sfRatioSelect = null;
    let sfStartDateInput = null;
    let sfEndDateInput = null;
    let sfRunButton = null;
    let sfStatusLabel = null;

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
            // MODIFICATION START: Integrate AnalyticsPriceCache for price data
            let priceDataRaw = null;
            const intervalForPrice = '1d'; // PFC seems to always use 1d for price

            // Determine parameters for cache lookup and API request for price data
            // For PFC, startDate and endDate are always defined (defaulted to 1 year if not user-provided)
            const cacheLookupPeriod = 'custom'; // Always treat as custom due to defined startDate/endDate
            const cacheLookupStartDate = startDate;
            const cacheLookupEndDate = endDate;

            // API query parameters for price
            const priceApiEndDateObj = new Date(endDate);
            priceApiEndDateObj.setDate(priceApiEndDateObj.getDate() + 1); // API end_date is exclusive for /price_history
            const priceApiQueryEndDate = priceApiEndDateObj.toISOString().split('T')[0];
            const priceApiQueryStartDate = startDate;


            if (window.AnalyticsPriceCache) {
                console.log(LOG_PREFIX, `[PFC] Attempting to fetch ${selectedTicker} ${intervalForPrice} (Period: '${cacheLookupPeriod}', Start: '${cacheLookupStartDate}', End: '${cacheLookupEndDate}') from cache.`);
                priceDataRaw = window.AnalyticsPriceCache.getPriceData(selectedTicker, intervalForPrice, cacheLookupPeriod, cacheLookupStartDate, cacheLookupEndDate);
                if (priceDataRaw) {
                    console.log(LOG_PREFIX, `[PFC] Cache HIT for ${selectedTicker} ${intervalForPrice}. Data points: ${priceDataRaw.length}`);
                } else {
                    console.log(LOG_PREFIX, `[PFC] Cache MISS for ${selectedTicker} ${intervalForPrice}. Will fetch from API.`);
                }
            } else {
                console.warn(LOG_PREFIX, "[PFC] AnalyticsPriceCache module not found. Fetching directly from API.");
            }

            if (!priceDataRaw) { // If cache miss or cache module not available
                const priceApiParams = `ticker=${encodeURIComponent(selectedTicker)}&interval=${encodeURIComponent(intervalForPrice)}&start_date=${encodeURIComponent(priceApiQueryStartDate)}&end_date=${encodeURIComponent(priceApiQueryEndDate)}`;
                const priceApiUrl = `/api/v3/timeseries/price_history?${priceApiParams}`;
                console.log(LOG_PREFIX, "[PFC] Fetching Price History from API:", priceApiUrl);
                
                const priceResponse = await fetch(priceApiUrl);
                if (!priceResponse.ok) {
                    const err = await priceResponse.json().catch(() => ({detail: `Price data fetch failed (${priceResponse.status})`}));
                    throw new Error(`Price data for ${selectedTicker}: ${err.detail || 'Fetch error'}`);
                }
                const fetchedPriceData = await priceResponse.json();
                console.log(LOG_PREFIX, `[PFC] API Price Data Received for ${selectedTicker}:`, fetchedPriceData ? fetchedPriceData.length : 0);

                if (fetchedPriceData && fetchedPriceData.length > 0) {
                    priceDataRaw = fetchedPriceData;
                    if (window.AnalyticsPriceCache) {
                        console.log(LOG_PREFIX, `[PFC] Storing ${selectedTicker} ${intervalForPrice} (Period: '${cacheLookupPeriod}', Start: '${cacheLookupStartDate}', End: '${cacheLookupEndDate}') in cache. Points: ${priceDataRaw.length}`);
                        window.AnalyticsPriceCache.storePriceData(selectedTicker, intervalForPrice, priceDataRaw, cacheLookupPeriod, cacheLookupStartDate, cacheLookupEndDate);
                    }
                } else {
                    // No price data from API, throw error or handle as needed
                    throw new Error(`No price data returned from API for ${selectedTicker}.`);
                }
            }
            // MODIFICATION END: Integrate AnalyticsPriceCache for price data

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
        pfrDisplayModeSelect = document.getElementById('ts-pfr-display-mode-select');
        pfrTtmToggle = document.getElementById('ts-pfr-ttm-toggle'); // NEW: Get reference to TTM toggle

        if (!pfrTickerSelect) console.error(LOG_PREFIX, "PFR Ticker select not found!");
        if (!pfrFieldSelect) console.error(LOG_PREFIX, "PFR Field select not found!");
        if (!pfrRunButton) console.error(LOG_PREFIX, "PFR Run button not found!");
        if (!pfrDisplayModeSelect) console.warn(LOG_PREFIX, "PFR Display Mode select (ts-pfr-display-mode-select) not found!");
        if (!pfrTtmToggle) console.warn(LOG_PREFIX, "PFR TTM Toggle select (ts-pfr-ttm-toggle) not found!");

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

    // REFACTORED: handleRunPriceFundamentalRatios to use new TTM architecture
    async function handleRunPriceFundamentalRatios() {
        console.log(LOG_PREFIX, "Run Price-Fundamental Ratios (PFR) clicked.");

        if (!pfrTickerSelect || !pfrFieldSelect || !pfrPeriodSelector || !pfrStartDateInput || !pfrEndDateInput || !pfrRunButton || !pfrDisplayModeSelect || !pfrTtmToggle) {
            alert("Error: Essential PFR UI elements are missing. Please refresh.");
            console.error(LOG_PREFIX, "PFR: Missing one or more UI elements for PFR study.", 
                {
                    pfrTickerSelect, pfrFieldSelect, pfrPeriodSelector, 
                    pfrStartDateInput, pfrEndDateInput, pfrRunButton, 
                    pfrDisplayModeSelect, pfrTtmToggle
                }
            );
            return;
        }

        const selectedTickers = $(pfrTickerSelect).val() || [];
        const selectedOriginalFieldIds = $(pfrFieldSelect).val() || []; // These are the yf_item_... IDs
        const selectedPeriod = pfrPeriodSelector.value;
        const displayMode = pfrDisplayModeSelect.value;
        const useTtmForAnnuals = pfrTtmToggle.checked;

        console.log(LOG_PREFIX, `PFR: Tickers: ${selectedTickers.join(', ')}, Fields: ${selectedOriginalFieldIds.join(', ')}, Period: ${selectedPeriod}, Display: ${displayMode}, TTM Toggle: ${useTtmForAnnuals}`);

        if (selectedTickers.length === 0) { alert("Please select at least one ticker."); return; }
        if (selectedOriginalFieldIds.length === 0) { alert("Please select at least one fundamental field."); return; }

        let baseStartDate = pfrStartDateInput.value;
        let baseEndDate = pfrEndDateInput.value;

        if (selectedPeriod === 'custom') {
            if (!baseStartDate || !baseEndDate) { alert("For custom range, please select Start and End dates."); return; }
            if (new Date(baseStartDate) >= new Date(baseEndDate)) { alert("Start Date must be before End Date for custom range."); return; }
        } else {
            // RESTORED: Original date calculation logic
            const today = new Date();
            baseEndDate = formatDateToYYYYMMDD(today); 
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
                // 'max' period is handled by leaving baseStartDate and baseEndDate as null or their initial values
                // which will be caught by preparePfrDataRequests and no specific start/end dates will be sent to the price API.
                case 'max': 
                    baseStartDate = null; // Explicitly set to null for max
                    baseEndDate = null;   // Explicitly set to null for max
                    break;
                default: 
                    console.warn(LOG_PREFIX, `PFR: Unknown period '${selectedPeriod}', defaulting to 1 year.`);
                    sDateObj.setFullYear(today.getFullYear() - 1); // Default to 1 year
            }
            if (selectedPeriod !== 'max') { // Only set baseStartDate if not max
                 baseStartDate = formatDateToYYYYMMDD(sDateObj);
            }
            // END RESTORED LOGIC
            console.log(LOG_PREFIX, `PFR: Period '${selectedPeriod}' selected. Calculated dates: ${baseStartDate} to ${baseEndDate}`);
        }
        
        // Determine the lookback start date for fundamental data queries
        // Go back further (e.g., 2-3 years before the price series start) to ensure enough historical data for TTM or initial value.
        let fundamentalQueryLookbackStartDate = baseStartDate;
        if (baseStartDate) {
            const tempDate = new Date(baseStartDate);
            tempDate.setFullYear(tempDate.getFullYear() - 3); // Look back 3 years for fundamentals
            fundamentalQueryLookbackStartDate = formatDateToYYYYMMDD(tempDate);
        }
        console.log(LOG_PREFIX, `PFR: Price period ${baseStartDate}-${baseEndDate}. Fundamental query lookback start: ${fundamentalQueryLookbackStartDate}`);


        if (pfrStatusLabel) pfrStatusLabel.innerHTML = 'Fetching data...'; // Use innerHTML for line breaks
        if (pfrRunButton) { pfrRunButton.disabled = true; pfrRunButton.querySelector('.spinner-border').style.display = 'inline-block'; pfrRunButton.querySelector('.button-text').textContent = 'Loading...'; }
        const loadingIndicator = document.getElementById('timeseries-loading-indicator');
        if (loadingIndicator) loadingIndicator.style.display = 'flex';

        try {
            const { fetchPromises, processingDetails } = await preparePfrDataRequests(
                selectedTickers,
                selectedOriginalFieldIds,
                useTtmForAnnuals,
                baseStartDate, 
                baseEndDate,
                fundamentalQueryLookbackStartDate
            );

            const results = await Promise.allSettled(fetchPromises);
            console.log(LOG_PREFIX, "PFR: All data fetch results from preparePfrDataRequests:", results);

            const priceDataCache = {}; // Keyed by ticker
            const fundamentalDataCache = {}; // Keyed by ticker, then by originalFieldId (NOT the fetched one, but the one user selected)
            let fetchErrorMessages = [];

            results.forEach(result => {
                if (result.status === 'fulfilled') {
                    const res = result.value;
                    if (res.error) {
                        const errorMsg = `${res.ticker} (${res.type}${res.originalFieldId ? ' for ' + res.originalFieldId.substring(res.originalFieldId.lastIndexOf('_') + 1) : ''}): ${res.error}`;
                        fetchErrorMessages.push(errorMsg);
                        console.warn(LOG_PREFIX, `PFR: Fetch error for ${res.ticker}, type ${res.type}:`, res.error);
                    } else {
                        if (res.type === 'price') {
                            priceDataCache[res.ticker] = res.data.map(d => ({
                                dateEpoch: new Date(d.Datetime || d.Date).valueOf(),
                                price: d.Close
                            })).sort((a,b) => a.dateEpoch - b.dateEpoch); // Ensure sorted by date
                        } else if (res.type === 'fundamental') {
                            if (!fundamentalDataCache[res.ticker]) {
                                fundamentalDataCache[res.ticker] = {};
                            }
                            // Store fundamental data keyed by the original field ID requested by the user,
                            // but the actual data is for `res.fieldFetched` (which might be quarterly)
                            // The payload `res.data` is { ticker: { field_payload_key: [...] } }
                            // So, fundamentalDataCache[ticker][originalFieldId] will store { field_payload_key_from_api: [...] }
                            if (res.data && res.data[res.ticker]) {
                                fundamentalDataCache[res.ticker][res.originalFieldId] = res.data[res.ticker]; 
                                console.log(LOG_PREFIX, `PFR: Cached fundamental data for ${res.ticker}, original field ${res.originalFieldId} (fetched as ${res.fieldFetched}). Data keys:`, Object.keys(res.data[res.ticker] || {}));
                            } else {
                                console.warn(LOG_PREFIX, `PFR: Fundamental data for ${res.ticker}, field ${res.originalFieldId} (fetched as ${res.fieldFetched}) was not in expected format or was empty.`);
                                fundamentalDataCache[res.ticker][res.originalFieldId] = {}; // Ensure an empty object if no data
                            }
                        }
                    }
                } else { // Promise rejected
                    const reason = result.reason;
                    const errorMsg = `${reason.ticker || 'Unknown ticker'} (${reason.type || 'Unknown type'}${reason.originalFieldId ? ' for ' + reason.originalFieldId.substring(reason.originalFieldId.lastIndexOf('_') + 1) : ''}): ${reason.detail || reason.message || 'Request failed'}`;
                    fetchErrorMessages.push(errorMsg);
                    console.error(LOG_PREFIX, `PFR: Promise rejection for ${reason.ticker}, type ${reason.type}:`, reason);
                }
            });

            if (fetchErrorMessages.length > 0) {
                pfrStatusLabel.innerHTML += '<br><strong class="text-danger">Data Fetch Issues:</strong><br>' + fetchErrorMessages.map(e => `<small>${e}</small>`).join('<br>');
            }

            const allChartSeriesData = [];
            const lineColors = [
                'rgb(75, 192, 192)', 'rgb(255, 99, 132)', 'rgb(54, 162, 235)',
                'rgb(255, 206, 86)', 'rgb(153, 102, 255)', 'rgb(255, 159, 64)',
                '#28a745', '#ffc107', '#dc3545', '#17a2b8', '#6f42c1', '#fd7e14'
            ];
            let colorIndex = 0;

            // Iterate through each selected original field ID first, then by ticker
            selectedOriginalFieldIds.forEach(originalFieldId => {
                selectedTickers.forEach(ticker => {
                    const detailKey = `${originalFieldId}__${ticker}`;
                    const currentProcessingDetail = processingDetails[detailKey];

                    if (!currentProcessingDetail) {
                        console.error(LOG_PREFIX, `PFR: CRITICAL - No processing detail found for key ${detailKey}. This should not happen.`);
                        pfrStatusLabel.innerHTML += `<br><small class='text-danger'>Internal error processing ${ticker} - ${originalFieldId}.</small>`;
                        return; // Skip this ticker/field combination
                    }

                    const priceSeriesForTicker = priceDataCache[ticker];
                    const fundamentalDataPayloadForField = fundamentalDataCache[ticker] ? fundamentalDataCache[ticker][originalFieldId] : null;

                    if (!priceSeriesForTicker || priceSeriesForTicker.length === 0) {
                        console.warn(LOG_PREFIX, `PFR: No price data for ${ticker}. Cannot calculate ratio for ${originalFieldId}.`);
                        pfrStatusLabel.innerHTML += `<br><small>No price data for ${ticker} to calculate P/${originalFieldId.substring(originalFieldId.lastIndexOf('_') + 1)} ratio.</small>`;
                        return; // Skip to next ticker if no price data for this one
                    }

                    if (!fundamentalDataPayloadForField) {
                        console.warn(LOG_PREFIX, `PFR: No fundamental data payload found for ${ticker} - ${originalFieldId} (expected fetch as ${currentProcessingDetail.fieldFetchedForFundamentals}). Skipping ratio.`);
                        pfrStatusLabel.innerHTML += `<br><small>No fundamental data available for ${ticker} - ${originalFieldId.substring(originalFieldId.lastIndexOf('_') + 1)}.</small>`;
                        return; // Skip this field for this ticker
                    }
                    
                    const priceSeriesDates = priceSeriesForTicker.map(p => p.dateEpoch);

                    const dailyFundamentalSeries = generateDailyFundamentalSeries(
                        priceSeriesDates, 
                        fundamentalDataPayloadForField, // This is the { api_field_key: [...] } object
                        currentProcessingDetail 
                    );

                    if (!dailyFundamentalSeries || dailyFundamentalSeries.length === 0) {
                        console.warn(LOG_PREFIX, `PFR: No daily fundamental series generated for ${ticker} - ${originalFieldId}.`);
                        pfrStatusLabel.innerHTML += `<br><small>Could not generate fundamental series for ${ticker} - ${originalFieldId.substring(originalFieldId.lastIndexOf('_') + 1)}.</small>`;
                        return; // Skip this field for this ticker
                    }

                    const ratioSeries = [];
                    let ttmActuallyUsedInSeries = false;
                    let seriesTtmErrorMessages = new Set();

                    for (let i = 0; i < priceSeriesForTicker.length; i++) {
                        const pricePoint = priceSeriesForTicker[i];
                        const fundamentalPoint = dailyFundamentalSeries.find(fs => fs.date === pricePoint.dateEpoch);

                        if (!fundamentalPoint) {
                            // This case should ideally not happen if generateDailyFundamentalSeries covers all priceSeriesDates
                            console.warn(LOG_PREFIX, `PFR: Mismatch - no fundamental point for price date ${new Date(pricePoint.dateEpoch).toISOString()} for ${ticker} - ${originalFieldId}`);
                            ratioSeries.push([pricePoint.dateEpoch, null]);
                            continue;
                        }

                        if (fundamentalPoint.ttmUsed) {
                            ttmActuallyUsedInSeries = true;
                            if (fundamentalPoint.ttmError) {
                                seriesTtmErrorMessages.add(fundamentalPoint.ttmError);
                            }
                        }

                        if (pricePoint.price !== null && fundamentalPoint.value !== null && fundamentalPoint.value !== 0 && !isNaN(fundamentalPoint.value)) {
                            ratioSeries.push([pricePoint.dateEpoch, pricePoint.price / fundamentalPoint.value]);
                        } else {
                            ratioSeries.push([pricePoint.dateEpoch, null]);
                        }
                    }
                    
                    // Handle displayMode ('raw_value' or 'percent_change')
                    let finalRatioSeriesData = ratioSeries;
                    if (displayMode === 'percent_change' && ratioSeries.length > 0) {
                        let baselineRatio = null;
                        for (const point of ratioSeries) {
                            if (point[1] !== null && !isNaN(point[1])) {
                                baselineRatio = point[1];
                                break;
                            }
                        }
                        if (baselineRatio !== null && baselineRatio !== 0) {
                            finalRatioSeriesData = ratioSeries.map(point => 
                                [point[0], (point[1] !== null && !isNaN(point[1])) ? ((point[1] / baselineRatio) - 1) * 100 : null]
                            );
                        } else {
                            finalRatioSeriesData = ratioSeries.map(point => [point[0], null]); // All null if no valid baseline
                        }
                    }

                    if (finalRatioSeriesData.length > 0) {
                        let seriesNameSuffix = "";
                        if (currentProcessingDetail.isAnnualOriginal) {
                            if (currentProcessingDetail.needsTtm) { // TTM was requested for this annual field
                                seriesNameSuffix = ttmActuallyUsedInSeries ? " (TTM)" : " (TTM requested, data unavailable)";
                                if (seriesTtmErrorMessages.size > 0 && !ttmActuallyUsedInSeries) { // Errors prevented TTM
                                     seriesNameSuffix = ` (TTM Err: ${Array.from(seriesTtmErrorMessages)[0]})`; // Show first error
                                } else if (seriesTtmErrorMessages.size > 0 && ttmActuallyUsedInSeries) { // TTM used but with errors
                                    seriesNameSuffix += ` (some errors: ${Array.from(seriesTtmErrorMessages)[0]})`;
                                }
                            } else { // TTM not requested, direct annual
                                seriesNameSuffix = " (Annual)";
                            }
                        } else { // Originally quarterly or other non-annual
                            seriesNameSuffix = originalFieldId.includes('_quarterly_') ? " (Quarterly)" : ""; // Default to (Quarterly) if applicable
                        }
                        
                        // Extract the core field name (e.g., TotalRevenue from ..._annual_TotalRevenue)
                        const coreFieldNameMatch = originalFieldId.match(/_([a-zA-Z0-9]+)$/);
                        const displayableFieldName = coreFieldNameMatch ? coreFieldNameMatch[1] : originalFieldId;

                        const seriesName = `${ticker} P/${displayableFieldName}${seriesNameSuffix}` + (displayMode === 'percent_change' ? ' (% Chg)' : '');

                        // NEW: Debug log before pushing to chart data
                        console.log(LOG_PREFIX, `PFR: Preparing chart series. Name: '${seriesName}', Ticker: ${ticker}, Field: ${originalFieldId}, Suffix: '${seriesNameSuffix}', DisplayMode: ${displayMode}`);
                        console.log(LOG_PREFIX, `PFR: Sample data for '${seriesName}':`, finalRatioSeriesData.slice(0, 5));
                        // END NEW: Debug log

                        // Convert finalRatioSeriesData to {x,y} format
                        const chartJsData = finalRatioSeriesData.map(point => ({ x: point[0], y: point[1] }));

                        allChartSeriesData.push({
                            label: seriesName, // MODIFIED: Changed 'name' to 'label'
                            data: chartJsData, // MODIFIED: Use {x,y} formatted data
                            type: 'line',
                            yAxisID: 'y-axis-ratio',
                            borderColor: lineColors[colorIndex % lineColors.length],
                            backgroundColor: lineColors[colorIndex % lineColors.length].replace('rgb', 'rgba').replace(')', ',0.05)'),
                            borderWidth: 1.5,
                            pointRadius: 0,
                            tension: 0.1,
                            _ttmUsedOverallInSeries: ttmActuallyUsedInSeries,
                            _ttmErrorsInSeries: Array.from(seriesTtmErrorMessages),
                            _processingDetail: currentProcessingDetail,
                            appDataType: displayMode // For tooltip formatting
                        });
                        colorIndex++;

                        if (seriesTtmErrorMessages.size > 0) {
                            pfrStatusLabel.innerHTML += `<br><small class='text-warning'>${seriesName}: TTM issues: ${Array.from(seriesTtmErrorMessages).join(', ')}.</small>`;
                        }

                    } else {
                        console.warn(LOG_PREFIX, `PFR: No ratio series data generated for ${ticker} - ${originalFieldId}`);
                        pfrStatusLabel.innerHTML += `<br><small>Could not calculate ratio for ${ticker} - ${originalFieldId.substring(originalFieldId.lastIndexOf('_') + 1)}.</small>`;
                    }
                }); // End loop selectedTickers
            }); // End loop selectedOriginalFieldIds


            if (allChartSeriesData.length === 0) {
                window.AnalyticsTimeseriesModule.showPlaceholderWithMessage("No ratio data could be calculated or plotted based on current selections and available data.");
                if (pfrStatusLabel) pfrStatusLabel.innerHTML += "<br>No ratio data generated.";
                return;
            }

            let yAxisTitle = "Ratio Value";
            let chartTitleSuffix = "";
            if (displayMode === 'percent_change') {
                yAxisTitle = "% Change from Baseline";
                chartTitleSuffix = " (% Change)";
            }

            const finalChartTitle = (selectedTickers.length > 1 || selectedOriginalFieldIds.length > 1 ? `Price-Fundamental Ratios` : `${selectedTickers[0]} P/${selectedOriginalFieldIds[0].substring(selectedOriginalFieldIds[0].lastIndexOf('_') + 1)}`) + chartTitleSuffix;
            const yAxesConfig = [{ id: 'y-axis-ratio', position: 'left', title: yAxisTitle }];
            
            let overallStartDateForDisplay = baseStartDate;
            let overallEndDateForDisplay = baseEndDate;
            // If 'max' period was used, try to get actual dates from the first price series for display
            if (selectedPeriod === 'max' && priceDataCache[selectedTickers[0]] && priceDataCache[selectedTickers[0]].length > 0) {
                const firstPriceDataPoints = priceDataCache[selectedTickers[0]];
                if (firstPriceDataPoints.length > 0) {
                    overallStartDateForDisplay = formatDateToYYYYMMDD(new Date(firstPriceDataPoints[0].dateEpoch));
                    overallEndDateForDisplay = formatDateToYYYYMMDD(new Date(firstPriceDataPoints[firstPriceDataPoints.length - 1].dateEpoch));
                }
            }

            window.AnalyticsTimeseriesModule.renderGenericTimeseriesChart(allChartSeriesData, finalChartTitle, null,
                { 
                    chartType: 'line', 
                    isTimeseries: true, 
                    yAxesConfig: yAxesConfig, 
                    rangeDetails: {start: overallStartDateForDisplay, end: overallEndDateForDisplay}
                }
            );
            if (pfrStatusLabel) pfrStatusLabel.innerHTML += `<br>PFR Chart Loaded. Range: ${overallStartDateForDisplay || 'N/A'} to ${overallEndDateForDisplay || 'N/A'}.`;

        } catch (error) {
            console.error(LOG_PREFIX, "Error during Price-Fundamental Ratios execution:", error);
            alert(`Error in PFR study: ${error.message}`);
            window.AnalyticsTimeseriesModule.showPlaceholderWithMessage(`Error: ${error.message}`);
            if (pfrStatusLabel) pfrStatusLabel.innerHTML += `<br><strong class='text-danger'>Runtime Error: ${error.message}</strong>`;
        } finally {
            if (loadingIndicator) loadingIndicator.style.display = 'none';
            if (pfrRunButton) {
                pfrRunButton.disabled = false;
                pfrRunButton.querySelector('.spinner-border').style.display = 'none';
                pfrRunButton.querySelector('.button-text').textContent = 'Calculate Ratios';
            }
        }
    }

    // ADD BACK: preparePfrDataRequests function here
    // Function to prepare data requests for Price-Fundamental Ratios (PFR)
    async function preparePfrDataRequests(selectedTickers, selectedOriginalFieldIds, useTtmForAnnuals, baseStartDate, baseEndDate, fundamentalQueryLookbackStartDate) {
        console.log(LOG_PREFIX, "PFR.preparePfrDataRequests: Preparing data requests. TTM for annuals:", useTtmForAnnuals, "Base Dates:", baseStartDate, "-", baseEndDate, "Fund. Lookback:", fundamentalQueryLookbackStartDate);
        const fetchPromises = [];
        const processingDetails = {}; // Keyed by originalFieldId__ticker (double underscore)
        
        const masterFieldListAll = (window.AnalyticsMainModule && typeof window.AnalyticsMainModule.getFinalAvailableFields === 'function') ?
                                 window.AnalyticsMainModule.getFinalAvailableFields() || [] : [];

        // 1. Price Data Requests for each ticker
        selectedTickers.forEach(ticker => {
            // MODIFICATION START: Integrate AnalyticsPriceCache for price data within PFR
            const intervalForPrice = '1d'; // PFR uses 1d interval for price data
            let pricePromise;

            // Determine parameters for cache lookup and API request for price data
            // baseStartDate and baseEndDate are from the main PFR handler (user's selection or calculated from period)
            const cacheLookupPeriod = (baseStartDate && baseEndDate) ? 'custom' : (pfrPeriodSelector ? pfrPeriodSelector.value : 'max'); // If no base dates, use selected period (e.g. 'ytd', 'max')
            const cacheLookupStartDate = (baseStartDate && baseEndDate) ? baseStartDate : null;
            const cacheLookupEndDate = (baseStartDate && baseEndDate) ? baseEndDate : null;

            let cachedPriceData = null;
            if (window.AnalyticsPriceCache) {
                console.log(LOG_PREFIX, `[PFR] Attempting to fetch ${ticker} ${intervalForPrice} (Period: '${cacheLookupPeriod}', Start: '${cacheLookupStartDate}', End: '${cacheLookupEndDate}') from cache.`);
                cachedPriceData = window.AnalyticsPriceCache.getPriceData(ticker, intervalForPrice, cacheLookupPeriod, cacheLookupStartDate, cacheLookupEndDate);
            }

            if (cachedPriceData) {
                console.log(LOG_PREFIX, `[PFR] Cache HIT for ${ticker} ${intervalForPrice}. Data points: ${cachedPriceData.length}`);
                pricePromise = Promise.resolve({ ticker, type: 'price', data: cachedPriceData });
            } else {
                console.log(LOG_PREFIX, `[PFR] Cache MISS for ${ticker} ${intervalForPrice}. Will fetch from API.`);
                let priceApiUrl;
                const params = new URLSearchParams();
                params.append('ticker', encodeURIComponent(ticker));
                params.append('interval', intervalForPrice);

                if (baseStartDate && baseEndDate) { // Custom or calculated range from specific period
                    const priceApiEndDateObj = new Date(baseEndDate);
                    priceApiEndDateObj.setDate(priceApiEndDateObj.getDate() + 1); // API end_date is exclusive
                    params.append('start_date', encodeURIComponent(baseStartDate));
                    params.append('end_date', encodeURIComponent(priceApiEndDateObj.toISOString().split('T')[0]));
                } else { // 'max' period or other predefined periods where baseStart/EndDate might be null
                    params.append('period', cacheLookupPeriod); // Use cacheLookupPeriod (e.g., 'max', 'ytd')
                }
                priceApiUrl = `/api/v3/timeseries/price_history?${params.toString()}`;
                console.log(LOG_PREFIX, `[PFR] Fetching Price for ${ticker} from API: ${priceApiUrl}`);

                pricePromise = fetch(priceApiUrl)
                    .then(response => {
                        if (!response.ok) return response.json().then(err => Promise.reject({ ticker, type: 'price', detail: err.detail || `Price fetch failed (${response.status})`}));
                        return response.json();
                    })
                    .then(apiData => {
                        console.log(LOG_PREFIX, `[PFR] API Price Data Received for ${ticker}:`, apiData ? apiData.length : 0);
                        if (apiData && apiData.length > 0) {
                            if (window.AnalyticsPriceCache) {
                                console.log(LOG_PREFIX, `[PFR] Storing ${ticker} ${intervalForPrice} (Period: '${cacheLookupPeriod}', Start: '${cacheLookupStartDate}', End: '${cacheLookupEndDate}') in cache. Points: ${apiData.length}`);
                                window.AnalyticsPriceCache.storePriceData(ticker, intervalForPrice, apiData, cacheLookupPeriod, cacheLookupStartDate, cacheLookupEndDate);
                            }
                            return { ticker, type: 'price', data: apiData };
                        }
                        return { ticker, type: 'price', error: "No price data returned or empty dataset from API." };
                    })
                    .catch(error => {
                        console.error(LOG_PREFIX, `[PFR] Error in price fetch promise for ${ticker}:`, error);
                        return { ticker, type: 'price', error: error.detail || error.message || 'Unknown price fetch error' };
                    });
            }
            fetchPromises.push(pricePromise);
            // MODIFICATION END: Integrate AnalyticsPriceCache for price data within PFR

            // 2. Fundamental Data Requests for each ticker and field
            selectedOriginalFieldIds.forEach(originalFieldId => {
                const isAnnualFieldOriginal = originalFieldId.includes('_annual_');
                const processThisFieldAsTtm = useTtmForAnnuals && isAnnualFieldOriginal;
                
                let fieldIdToFetchForFundamentals = originalFieldId;
                if (processThisFieldAsTtm) {
                    const quarterlyEquivalent = originalFieldId.replace('_annual_', '_quarterly_');
                    if (masterFieldListAll.includes(quarterlyEquivalent)) {
                        fieldIdToFetchForFundamentals = quarterlyEquivalent;
                        console.log(LOG_PREFIX, `PFR.preparePfrDataRequests: TTM active for ${originalFieldId}. Will fetch QUARTERLY field: ${fieldIdToFetchForFundamentals}`);
                    } else {
                        console.warn(LOG_PREFIX, `PFR.preparePfrDataRequests: TTM active for ${originalFieldId}, but its quarterly equivalent '${quarterlyEquivalent}' was NOT FOUND. Will fall back to original. TTM may not be possible.`);
                    }
                }

                const detailKey = `${originalFieldId}__${ticker}`;
                processingDetails[detailKey] = {
                    ticker: ticker,
                    originalFieldId: originalFieldId,
                    isAnnualOriginal: isAnnualFieldOriginal,
                    needsTtm: processThisFieldAsTtm, 
                    fieldIdFetchedForFundamentals: fieldIdToFetchForFundamentals,
                };

                const fundamentalsRequestPayload = {
                    tickers: [ticker],
                    field_identifiers: [fieldIdToFetchForFundamentals],
                    start_date: fundamentalQueryLookbackStartDate,
                    end_date: baseEndDate,
                };

                fetchPromises.push(
                    fetch('/api/v3/timeseries/fundamentals_history', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'X-CSRF-Token': document.getElementById('csrf_token')?.value || ''
                        },
                        body: JSON.stringify(fundamentalsRequestPayload)
                    })
                    .then(response => {
                        if (!response.ok) return response.json().then(err => Promise.reject({ ticker, originalFieldId, fieldFetched: fieldIdToFetchForFundamentals, type: 'fundamental', detail: err.detail || `Fundamental fetch failed (${response.status})`}));
                        return response.json();
                    })
                    .then(data => ({ ticker, originalFieldId, fieldFetched: fieldIdToFetchForFundamentals, type: 'fundamental', data }))
                    .catch(error => ({ ticker, originalFieldId, fieldFetched: fieldIdToFetchForFundamentals, type: 'fundamental', error: error.detail || error.message || 'Unknown fundamental fetch error' }))
                );
            });
        });
        console.log(LOG_PREFIX, "PFR.preparePfrDataRequests: Processing Details map:", processingDetails);
        return { fetchPromises, processingDetails };
    }
    // END ADD BACK

    // NEW: Phase 3.1 - Standalone TTM Calculation Function
    function calculateRollingTtmSeries(quarterlyPoints, priceSeriesDates) {
        console.log(LOG_PREFIX, `PFR.calculateRollingTtmSeries: Calculating TTM using ${quarterlyPoints.length} quarterly points for ${priceSeriesDates.length} price dates.`);
        const ttmResults = [];

        if (!quarterlyPoints || quarterlyPoints.length === 0) {
            console.warn(LOG_PREFIX, "PFR.calculateRollingTtmSeries: No quarterly points provided. Returning nulls for all price dates.");
            return priceSeriesDates.map(dateEpoch => ({ date: dateEpoch, value: null, ttmUsed: true, ttmError: "No quarterly data available" }));
        }

        const sortedQuarterlyPoints = [...quarterlyPoints].sort((a, b) => a.dateEpoch - b.dateEpoch);

        for (const priceDateEpoch of priceSeriesDates) {
            const relevantQuarters = sortedQuarterlyPoints.filter(q => q.dateEpoch <= priceDateEpoch);
            if (relevantQuarters.length >= 4) {
                const lastFourQuarters = relevantQuarters.slice(-4);
                const ttmValue = lastFourQuarters.reduce((sum, q) => sum + ((q.value !== null && typeof q.value === 'number') ? q.value : 0), 0);
                ttmResults.push({ date: priceDateEpoch, value: ttmValue, ttmUsed: true, ttmError: null });
            } else {
                ttmResults.push({ date: priceDateEpoch, value: null, ttmUsed: true, ttmError: "Insufficient quarterly data for this date" });
            }
        }
        return ttmResults;
    }

    // NEW: Phase 3.2 (from original plan) - Generate Daily Fundamental Series (handles TTM)
    function generateDailyFundamentalSeries(priceSeriesDates, rawFundamentalPayloadForField, fieldDetail) {
        const { ticker, originalFieldId, needsTtm, fieldIdFetchedForFundamentals } = fieldDetail;
        console.log(LOG_PREFIX, `PFR.generateDailyFundamentalSeries for ${ticker} - ${originalFieldId} (fetched as ${fieldIdFetchedForFundamentals}). Needs TTM: ${needsTtm}. Received payload object keys:`, rawFundamentalPayloadForField ? Object.keys(rawFundamentalPayloadForField) : 'No payload object');

        let fundamentalPoints = [];
        let actualDataKey = null;

        if (rawFundamentalPayloadForField && typeof rawFundamentalPayloadForField === 'object' && Object.keys(rawFundamentalPayloadForField).length > 0) {
            // Assuming the API for a single field_identifier returns an object with one key,
            // which is the actual series name (e.g., "annual Inventory" or "quarterly Total Revenue")
            actualDataKey = Object.keys(rawFundamentalPayloadForField)[0];
            if (rawFundamentalPayloadForField[actualDataKey] && Array.isArray(rawFundamentalPayloadForField[actualDataKey])) {
                fundamentalPoints = rawFundamentalPayloadForField[actualDataKey].map(p => ({
                    dateEpoch: new Date(p.date).valueOf(),
                    value: typeof p.value === 'string' ? parseFloat(p.value) : p.value
                })).sort((a, b) => a.dateEpoch - b.dateEpoch);
                console.log(LOG_PREFIX, `PFR.generateDailyFundamentalSeries: Successfully extracted ${fundamentalPoints.length} points using key '${actualDataKey}' for ${ticker} - ${originalFieldId}`);
            } else {
                console.warn(LOG_PREFIX, `PFR.generateDailyFundamentalSeries: Key '${actualDataKey}' found, but its value is not an array or is missing in payload for ${ticker} - ${originalFieldId}.`);
            }
        } else {
            console.warn(LOG_PREFIX, `PFR.generateDailyFundamentalSeries: Payload object for ${ticker} - ${originalFieldId} is empty, not an object, or missing.`);
        }

        if (fundamentalPoints.length === 0) {
            // This log now also covers cases where actualDataKey was found but the array was empty or not an array.
            console.warn(LOG_PREFIX, `PFR.generateDailyFundamentalSeries: No processable fundamental points extracted for ${ticker} - ${originalFieldId} (expected from API as ${fieldIdFetchedForFundamentals}).`);
            return priceSeriesDates.map(dateEpoch => ({ date: dateEpoch, value: null, ttmUsed: needsTtm, ttmError: needsTtm ? "No data for TTM field" : "No data for field" }));
        }
        
        if (needsTtm) {
            console.log(LOG_PREFIX, `PFR.generateDailyFundamentalSeries: Using TTM calculation for ${ticker} - ${originalFieldId}`);
            return calculateRollingTtmSeries(fundamentalPoints, priceSeriesDates); // fundamentalPoints are already the quarterly ones
        } else {
            // Standard "last known value" propagation for non-TTM (e.g. direct annual or direct quarterly)
            console.log(LOG_PREFIX, `PFR.generateDailyFundamentalSeries: Propagating last known value for ${ticker} - ${originalFieldId}`);
            const dailyFundamentalValues = [];
            let lastKnownValue = null;
            let lastKnownFundDateEpoch = -1; // Epoch of the last fundamental data point used

            // Find the initial fundamental value for the start of the price series
            const firstPriceDateEpoch = priceSeriesDates[0];
            for (let i = fundamentalPoints.length - 1; i >= 0; i--) {
                if (fundamentalPoints[i].dateEpoch <= firstPriceDateEpoch) {
                    lastKnownValue = fundamentalPoints[i].value;
                    lastKnownFundDateEpoch = fundamentalPoints[i].dateEpoch;
                    break;
                }
            }
            // If no fundamental data point is found on or before the first price date, lastKnownValue will remain null.

            priceSeriesDates.forEach(priceDateEpoch => {
                // Iterate through fundamental points to find the most recent one for the current priceDateEpoch
                for (const fundPoint of fundamentalPoints) {
                    if (fundPoint.dateEpoch <= priceDateEpoch && fundPoint.dateEpoch > lastKnownFundDateEpoch) {
                        lastKnownValue = fundPoint.value;
                        lastKnownFundDateEpoch = fundPoint.dateEpoch;
                    } else if (fundPoint.dateEpoch > priceDateEpoch) {
                        // Since fundamentalPoints is sorted, we can break early
                        break;
                    }
                }
                dailyFundamentalValues.push({ date: priceDateEpoch, value: lastKnownValue, ttmUsed: false, ttmError: null });
            });
            return dailyFundamentalValues;
        }
    }
    // END NEW: Phase 3.2

    // --- NEW: Functions for Synthetic Fundamentals Study (SF) ---
    function initializeSyntheticFundamentalsControls() {
        console.log(LOG_PREFIX, "Initializing Synthetic Fundamentals (SF) study controls...");

        sfTickerSelect = document.getElementById('ts-sf-ticker-select');
        sfRatioSelect = document.getElementById('ts-sf-ratio-select');
        sfStartDateInput = document.getElementById('ts-sf-start-date');
        sfEndDateInput = document.getElementById('ts-sf-end-date');
        sfRunButton = document.getElementById('ts-sf-run-study-btn');
        sfStatusLabel = document.getElementById('ts-sf-status');

        if (!sfTickerSelect) console.error(LOG_PREFIX, "SF Ticker select (ts-sf-ticker-select) not found!");
        if (!sfRatioSelect) console.error(LOG_PREFIX, "SF Ratio select (ts-sf-ratio-select) not found!");
        if (!sfRunButton) console.error(LOG_PREFIX, "SF Run button (ts-sf-run-study-btn) not found!");
        // sfStatusLabel is optional to find

        // Populate tickers (will be handled by 'AnalyticsTransformComplete' event)
        if (isFundDataReady && sfTickerSelect) {
            populateSfTickerSelect(); 
        } else if (sfTickerSelect) {
            console.log(LOG_PREFIX, "SF Tickers pending data readiness signal ('AnalyticsTransformComplete'). Initializing multiselect placeholder.");
            if (typeof $(sfTickerSelect).multiselect === 'function' && !$(sfTickerSelect).data('multiselect')) {
                $(sfTickerSelect).multiselect({
                    buttonWidth: '100%',
                    nonSelectedText: 'Tickers loading...',
                    numberDisplayed: 1,
                    includeSelectAllOption: true,
                    enableFiltering: true,
                    enableCaseInsensitiveFiltering: true,
                    maxHeight: 200
                });
            } else if (typeof $(sfTickerSelect).multiselect === 'function' && $(sfTickerSelect).data('multiselect')) {
                // If already initialized, ensure placeholder is updated if options are not yet there
                if ($(sfTickerSelect).find('option').length <= 1 || ($(sfTickerSelect).find('option').length === 1 && $(sfTickerSelect).find('option').first().val() === "")) {
                    $(sfTickerSelect).multiselect('setOptions', { nonSelectedText: 'Tickers loading...' });
                    $(sfTickerSelect).multiselect('rebuild');
                }
            }
        }
        
        // Populate ratio select (statically for now)
        if (sfRatioSelect) {
            sfRatioSelect.innerHTML = ''; // Clear existing options first

            const componentIcon = "🧱"; // Brick
            const ratioIcon = "📊";     // Bar Chart

            const epsOption = document.createElement('option');
            epsOption.value = "EPS_TTM";
            epsOption.textContent = `${componentIcon} Diluted EPS (TTM)`;
            // epsOption.style.color = "green"; // REMOVED
            sfRatioSelect.appendChild(epsOption);

            const peOption = document.createElement('option');
            peOption.value = "PE_TTM";
            peOption.textContent = `${ratioIcon} P/E (TTM)`;
            // peOption.style.color = "blue"; // REMOVED
            sfRatioSelect.appendChild(peOption);
            
            const cashPerShareOption = document.createElement('option');
            cashPerShareOption.value = "CASH_PER_SHARE";
            cashPerShareOption.textContent = `${componentIcon} Cash/Share`;
            // cashPerShareOption.style.color = "green"; // REMOVED
            sfRatioSelect.appendChild(cashPerShareOption);

            const cashPlusStInvPerShareOption = document.createElement('option');
            cashPlusStInvPerShareOption.value = "CASH_PLUS_ST_INV_PER_SHARE";
            cashPlusStInvPerShareOption.textContent = `${componentIcon} Cash+ST Inv/Share`;
            // cashPlusStInvPerShareOption.style.color = "green"; // REMOVED
            sfRatioSelect.appendChild(cashPlusStInvPerShareOption);
            
            const priceToCashPlusStInvOption = document.createElement('option');
            priceToCashPlusStInvOption.value = "PRICE_TO_CASH_PLUS_ST_INV";
            priceToCashPlusStInvOption.textContent = `${ratioIcon} Price/Cash+ST Inv`;
            // priceToCashPlusStInvOption.style.color = "blue"; // REMOVED
            sfRatioSelect.appendChild(priceToCashPlusStInvOption);

            // NEW: Book Value per Share options
            const bookValuePerShareOption = document.createElement('option');
            bookValuePerShareOption.value = "BOOK_VALUE_PER_SHARE";
            bookValuePerShareOption.textContent = `${componentIcon} Book Value/Share`;
            sfRatioSelect.appendChild(bookValuePerShareOption);

            const priceToBookValueOption = document.createElement('option');
            priceToBookValueOption.value = "PRICE_TO_BOOK_VALUE";
            priceToBookValueOption.textContent = `${ratioIcon} Price/Book Value`;
            sfRatioSelect.appendChild(priceToBookValueOption);
            // END NEW

            // Select the first one by default if needed, or let HTML decide initial selected
            if (!sfRatioSelect.value) {
                 epsOption.selected = true;
            }
        } 

        if (sfRunButton) {
            sfRunButton.addEventListener('click', handleRunSyntheticFundamentals);
        }
        console.log(LOG_PREFIX, "SF controls initialization listeners set up.");
    }

    function populateSfTickerSelect() {
        console.log(LOG_PREFIX, "Populating Synthetic Fundamentals ticker select (populateSfTickerSelect)...");
        if (!sfTickerSelect) {
            console.error(LOG_PREFIX, "SF Ticker select (ts-sf-ticker-select) not found when trying to populate.");
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
                }
            } catch (e) {
                console.error(LOG_PREFIX, "Error calling getFinalDataForAnalysis for SF tickers:", e);
            }
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
        
        const oldVal = $(sfTickerSelect).val(); // Preserve selection if multiselect
        sfTickerSelect.innerHTML = ''; // Clear existing

        if (sortedTickers.length > 0) {
            sortedTickers.forEach(ticker => {
                const option = document.createElement('option');
                option.value = ticker;
                option.textContent = ticker;
                sfTickerSelect.appendChild(option);
            });
        } else {
            const noTickerOption = document.createElement('option');
            noTickerOption.value = "";
            noTickerOption.textContent = "No tickers in loaded data";
            noTickerOption.disabled = true;
            sfTickerSelect.appendChild(noTickerOption);
        }
        console.log(LOG_PREFIX, `Populated tickers for SF: ${sortedTickers.length}`);

        if (typeof $(sfTickerSelect).multiselect === 'function') {
            if (!$(sfTickerSelect).data('multiselect')) {
                 $(sfTickerSelect).multiselect({
                    buttonWidth: '100%',
                    enableFiltering: true,
                    enableCaseInsensitiveFiltering: true,
                    maxHeight: 200,
                    includeSelectAllOption: true,
                    nonSelectedText: sortedTickers.length > 0 ? 'Select Ticker(s)' : 'No tickers loaded',
                    numberDisplayed: 1,
                    nSelectedText: ' tickers selected',
                    allSelectedText: 'All tickers selected',
                });
            } else {
                $(sfTickerSelect).multiselect('setOptions', { nonSelectedText: sortedTickers.length > 0 ? 'Select Ticker(s)' : 'No tickers loaded' });
                $(sfTickerSelect).multiselect('rebuild');
                // Try to reapply old selection if it was a multiselect
                if (Array.isArray(oldVal) && oldVal.length > 0) {
                    $(sfTickerSelect).multiselect('select', oldVal);
                }
            }
        }
    }

    async function handleRunSyntheticFundamentals() {
        console.log(LOG_PREFIX, "Run Synthetic Fundamentals clicked.");

        if (!sfTickerSelect || !sfRatioSelect || !sfRunButton) {
            alert("Error: Essential UI elements for Synthetic Fundamentals are missing.");
            console.error(LOG_PREFIX, "Missing SF UI elements.");
            return;
        }

        const selectedTickers = $(sfTickerSelect).val() || [];
        const selectedRatio = sfRatioSelect.value;
        let startDate = sfStartDateInput ? sfStartDateInput.value : null;
        let endDate = sfEndDateInput ? sfEndDateInput.value : null;

        if (selectedTickers.length === 0) {
            alert("Please select at least one ticker."); return;
        }
        if (!selectedRatio) {
            alert("Please select a metric/ratio."); return;
        }

        if (!startDate && !endDate) {
            const today = new Date();
            endDate = formatDateToYYYYMMDD(today); 
            const ytdStart = new Date(today.getFullYear(), 0, 1);
            startDate = formatDateToYYYYMMDD(ytdStart); 
            console.log(LOG_PREFIX, `SF: No dates provided, defaulting to YTD: ${startDate} to ${endDate}`);
        } else if (startDate && endDate && new Date(startDate) >= new Date(endDate)) {
            alert("Start Date must be strictly before End Date."); return;
        } else if ((startDate && !endDate) || (!startDate && endDate)) {
            alert("Please provide both Start and End dates, or leave both blank for YTD."); return;
        }

        if (sfStatusLabel) sfStatusLabel.textContent = 'Fetching & calculating data...';
        let sfRunButtonText, sfRunButtonSpinner;
        if (sfRunButton) {
            sfRunButton.disabled = true;
            sfRunButtonText = sfRunButton.querySelector('.button-text');
            sfRunButtonSpinner = sfRunButton.querySelector('.spinner-border');
            if (sfRunButtonText) sfRunButtonText.textContent = 'Loading...';
            if (sfRunButtonSpinner) sfRunButtonSpinner.style.display = 'inline-block';
        }
        const generalLoadingIndicator = document.getElementById('timeseries-loading-indicator'); 
        if (generalLoadingIndicator) generalLoadingIndicator.style.display = 'flex';

        const requestPayload = {
            tickers: selectedTickers,
            start_date: startDate, 
            end_date: endDate,     
        };
        console.log(LOG_PREFIX, "SF Request Payload for", selectedRatio, JSON.stringify(requestPayload, null, 2));

        try {
            const response = await fetch(`/api/v3/timeseries/synthetic_fundamental/${selectedRatio}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRF-Token': document.getElementById('csrf_token')?.value || ''
                },
                body: JSON.stringify(requestPayload)
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ detail: "Unknown error during synthetic fundamental fetch." }));
                console.error(LOG_PREFIX, "SF API Error:", response.status, errorData);
                throw new Error(errorData.detail || `HTTP error ${response.status}`);
            }

            const apiData = await response.json(); 
            console.log(LOG_PREFIX, "SF API Response Data:", apiData);

            if (Object.keys(apiData).length === 0) {
                 showPlaceholderWithMessage(`No data returned for Synthetic Fundamental: ${selectedRatio}.`);
                 if (sfStatusLabel) sfStatusLabel.textContent = `No data for ${selectedRatio}.`;
                 return;
            }
            
            const datasets = [];
            const lineColors = [
                'rgb(75, 192, 192)', 'rgb(255, 99, 132)', 'rgb(54, 162, 235)',
                'rgb(255, 206, 86)', 'rgb(153, 102, 255)', 'rgb(255, 159, 64)',
                '#28a745', '#ffc107', '#dc3545', '#17a2b8', '#6f42c1', '#fd7e14'
            ];
            let colorIndex = 0;

            selectedTickers.forEach(ticker => {
                if (apiData[ticker] && apiData[ticker].length > 0) {
                    const timeseriesForTicker = apiData[ticker];
                    const dataPoints = timeseriesForTicker.map(point => ({
                        x: new Date(point.date).valueOf(), 
                        y: point.value !== null ? (typeof point.value === 'string' ? parseFloat(point.value) : point.value) : null
                    }));

                    datasets.push({
                        label: `${ticker} - ${selectedRatio.replace(/_/g, ' ')}`,
                        data: dataPoints,
                        borderColor: lineColors[colorIndex % lineColors.length],
                        backgroundColor: lineColors[colorIndex % lineColors.length].replace('rgb', 'rgba').replace(')', ',0.1)'),
                        borderWidth: 1.5,
                        fill: false,
                        pointRadius: 0, // Set to 0 to hide points
                        tension: 0.1,
                        spanGaps: true 
                    });
                    colorIndex++;
                } else {
                    console.warn(LOG_PREFIX, `SF: No data or empty series for ticker ${ticker} in API response.`);
                }
            });

            if (datasets.length === 0) {
                showPlaceholderWithMessage(`No chartable data found after processing Synthetic Fundamental: ${selectedRatio}.`);
                if (sfStatusLabel) sfStatusLabel.textContent = `No chartable data for ${selectedRatio}.`;
                return;
            }

            if (typeof window.AnalyticsTimeseriesModule !== 'undefined' && 
                typeof window.AnalyticsTimeseriesModule.renderGenericTimeseriesChart === 'function') {
                
                let chartTitle = `${selectedRatio.replace(/_/g, ' ')} (${selectedTickers.join(', ')})`;
                let yAxisLabel = "Calculated Value"; 

                if (selectedRatio === "EPS_TTM") {
                    yAxisLabel = "EPS Value (TTM)";
                } else if (selectedRatio === "PE_TTM") {
                    yAxisLabel = "P/E Ratio (TTM)";
                }
                // --- NEW: Add yAxisLabel for Cash/Share (TTM) ---
                else if (selectedRatio === "CASH_PER_SHARE") {
                    yAxisLabel = "Cash/Share";
                }
                // --- END: Add yAxisLabel for Cash/Share (TTM) ---

                // --- NEW: Add yAxisLabel for Cash+ST Inv/Share ---
                else if (selectedRatio === "CASH_PLUS_ST_INV_PER_SHARE") {
                    yAxisLabel = "Cash+ST Inv/Share";
                }
                // --- END: Add yAxisLabel for Cash+ST Inv/Share ---
                // --- NEW: Add yAxisLabel for Price/Cash+ST Inv ---
                else if (selectedRatio === "PRICE_TO_CASH_PLUS_ST_INV") {
                    yAxisLabel = "Price/Cash+ST Inv Ratio";
                }
                // --- END: Add yAxisLabel for Price/Cash+ST Inv ---
                // NEW: Add yAxisLabels for Book Value per Share and Price/Book Value
                else if (selectedRatio === "BOOK_VALUE_PER_SHARE") {
                    yAxisLabel = "Book Value/Share";
                }
                else if (selectedRatio === "PRICE_TO_BOOK_VALUE") {
                    yAxisLabel = "Price/Book Value Ratio";
                }
                // END NEW

                window.AnalyticsTimeseriesModule.renderGenericTimeseriesChart(
                    datasets, 
                    chartTitle, 
                    yAxisLabel,
                    {
                        chartType: 'line',
                        isTimeseries: true,
                        rangeDetails: {start: startDate, end: endDate} 
                    }
                );
                if (sfStatusLabel) sfStatusLabel.textContent = `Chart loaded for ${selectedRatio.replace(/_/g, ' ')}. Range: ${startDate || 'N/A'} to ${endDate || 'N/A'}.`;
            } else {
                console.error(LOG_PREFIX, "AnalyticsTimeseriesModule.renderGenericTimeseriesChart not found. Cannot display SF chart.");
                showPlaceholderWithMessage("Charting function is not available. See console.");
                if (sfStatusLabel) sfStatusLabel.textContent = "Error: Charting function unavailable.";
            }
            
        } catch (error) {
            console.error(LOG_PREFIX, "Error fetching or processing synthetic fundamentals:", error);
            const displayRatioName = selectedRatio.replace(/_/g, ' ');
            showPlaceholderWithMessage(`Error for ${displayRatioName}: ${error.message}`);
            if (sfStatusLabel) sfStatusLabel.textContent = `Error: ${error.message}`;
        } finally {
            if (generalLoadingIndicator) generalLoadingIndicator.style.display = 'none';
            if (sfRunButton) {
                sfRunButton.disabled = false;
                sfRunButton.querySelector('.spinner-border').style.display = 'none';
                sfRunButton.querySelector('.button-text').textContent = 'Run Calculation';
            }
        }
    }
    // --- END: Functions for Synthetic Fundamentals Study (SF) ---

    // --- Expose module functions ---
    window.TimeseriesFundamentalsModule = {
        initializeFundamentalsHistoryStudyControls,
        handleRunFundamentalsHistory,
        initializePriceFundamentalComparisonControls,
        initializePriceFundamentalRatiosControls, 
        initializeSyntheticFundamentalsControls // NEW: Expose SF init
    };

    console.log(LOG_PREFIX, "Module loaded and exposed.");

    // Store original event listener if it exists, or a placeholder if not.
    const originalAnalyticsTransformCompleteListener = window.handleAnalyticsTransformationComplete_Fund || function() { 
        console.log(LOG_PREFIX, "Original handleAnalyticsTransformationComplete_Fund was not found, using placeholder.");
    };

    // New extended handler
    function handleAnalyticsTransformationComplete_Fund_Extended() {
        console.log(LOG_PREFIX, "'AnalyticsTransformComplete' event received in Fundamentals module (Extended).");
        isFundDataReady = true; // Set flag

        // Call original logic for FH and PFC (assuming original function handles its own checks)
        // if (typeof originalAnalyticsTransformCompleteListener === 'function') {
        //    originalAnalyticsTransformCompleteListener(); // This would double-log and re-set isFundDataReady if not careful
        // } 
        // Instead, explicitly call the necessary population functions from the original logic that are safe to re-run
        if (fhTickerSelect) {
            console.log(LOG_PREFIX, "(Extended Handler) Populating FH tickers.");
            populateFundamentalsHistoryTickerSelect();
            if (fhFieldSelect && (!$(fhFieldSelect).data('multiselect') || $(fhFieldSelect).find('option').length <=1 ) ){
                 populateFieldSelect(); 
            }
        }
        if (pfcFieldSelect) {
            console.log(LOG_PREFIX, "(Extended Handler) Populating PFC fields.");
            populatePfcFieldSelect();
        }
        // PFR field population is handled within its own initialize function or when data is ready inside that init. 
        // We don't need to explicitly call populatePfrFieldSelect() here as it's part of initializePriceFundamentalRatiosControls.

        // NEW: Populate SF tickers if its select element is ready
        if (sfTickerSelect) {
            console.log(LOG_PREFIX, "(Extended Handler) Populating SF tickers.");
            populateSfTickerSelect(); 
        } else {
            console.warn(LOG_PREFIX, "(Extended Handler) SF Ticker Select not available when 'AnalyticsTransformComplete' was caught.");
        }
    }

    // Ensure the old listener is removed before adding the new one to prevent duplicates.
    // The original `handleAnalyticsTransformationComplete_Fund` was at the end of the file.
    // We need to make sure we correctly reference it or replace its functionality cleanly.
    // For safety, let's assume for now the assignment at the end of the file is the one we need to replace.
    // This is tricky as the original code sets `window.addEventListener('AnalyticsTransformComplete', handleAnalyticsTransformationComplete_Fund);`
    // Best to remove any existing listener for this event by this module before adding the new one.
    
    // Attempt to remove by function reference if it was globally exposed or use a named function strategy.
    // Since handleAnalyticsTransformationComplete_Fund is not globally exposed, we replace its definition earlier if we can.
    // The current structure of the file defines handleAnalyticsTransformationComplete_Fund then later adds it as a listener.
    // Let's redefine what 'handleAnalyticsTransformationComplete_Fund' does or ensure the new listener is the only one.

    // Remove the old listener if it was added by name 'handleAnalyticsTransformationComplete_Fund' internally.
    // This is a bit of a guess as the original function is not passed to removeEventListener.
    // A more robust way would be to ensure `handleAnalyticsTransformationComplete_Fund` itself calls the new logic, or is replaced.
    // For now, we assume the last `addEventListener` in the file using the name `handleAnalyticsTransformationComplete_Fund` is the target.
    // Since it's an IIFE, direct removal by named function might be tricky if it wasn't stored.

    // The provided file has this at the end: 
    // window.addEventListener('AnalyticsTransformComplete', handleAnalyticsTransformationComplete_Fund);
    // We need to ensure our new extended handler is used instead.
    // Simplest: rename the original `handleAnalyticsTransformationComplete_Fund` and call it from the new one, then attach the new one.
    // Or, replace the addEventListener call at the end of the file.
    // Given the tools, it's easier to modify the addEventListener line itself, or add a remove and then add.
    
    // The previous edit tried to remove and re-add. Let's stick to that logic but ensure the original function's definition
    // is either replaced or the new extended function is correctly added.
    // The variable `handleAnalyticsTransformationComplete_Fund` is defined locally within the IIFE.

    // Replace the line: window.addEventListener('AnalyticsTransformComplete', handleAnalyticsTransformationComplete_Fund);
    // with: window.addEventListener('AnalyticsTransformComplete', handleAnalyticsTransformationComplete_Fund_Extended);
    // This will be done in a separate edit to the end of the file.

    console.log(LOG_PREFIX, "Adding new event listener for 'AnalyticsTransformComplete' to WINDOW (Extended).");
    // Remove any old listener first, if it was the original one from this IIFE.
    // This is a bit tricky because the original listener function is defined within this IIFE and not globally accessible for removal by reference easily from outside.
    // However, since we are modifying this IIFE, we can ensure only one listener is added.
    // The key is that `handleAnalyticsTransformationComplete_Fund` which was previously added is now effectively superseded by `handleAnalyticsTransformationComplete_Fund_Extended`.
    // If the old `window.addEventListener('AnalyticsTransformComplete', handleAnalyticsTransformationComplete_Fund);` line is removed or commented out, and replaced with the new one, it will work.
    // The previous diff showed `handleAnalyticsTransformationComplete_Fund` was defined and then later `window.addEventListener('AnalyticsTransformComplete', handleAnalyticsTransformationComplete_Fund);` was the line.
    // We will replace that specific line.

    // If an old listener under the exact name `handleAnalyticsTransformationComplete_Fund` was attached by this IIFE,
    // it should be replaced by the new one. The logic above tries to make the new function the one that gets called.
    // The most direct way is to ensure the `addEventListener` call at the very end of this file uses the new function.
    window.removeEventListener('AnalyticsTransformComplete', handleAnalyticsTransformationComplete_Fund); // Attempt to remove old one by its presumed name
    window.addEventListener('AnalyticsTransformComplete', handleAnalyticsTransformationComplete_Fund_Extended);

})(); 